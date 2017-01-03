//
// Created by jie.huang on 16/11/15.
//
#include <iostream>
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "util/coding.h"

struct DataPoint {
    bool hasValue = false;
    int64_t value = 0;
};

struct TagFilter {
    uint32_t count = 0;
    uint32_t tagName = 0;
    uint32_t *tagValues = nullptr;

    ~TagFilter() {
        if (nullptr != tagValues) {
            delete[] tagValues;
        }
    }
};

namespace rocksdb {
    class MetricsScannerImpl : public MetricsScanner {
    private:
        DB *db_;
        Env *env_;
        ReadOptions read_options_;

        std::string seekKey_ = "";
        uint32_t readMetric_ = 0;
        uint8_t readHour_ = 0;
        int8_t saveHour_ = -1;
        int8_t currentHour_ = -1;

        uint32_t *curGroupByKey_ = nullptr;
        uint32_t *saveGroupByKey_ = nullptr;

        Iterator *iter_ = nullptr;
        bool hasValue_ = false;
        bool close_ = false;
        bool finish_ = false;

        std::string resultSet_ = "";
        std::string groupByResult_ = "";
        std::string statResult_ = "";
        TagFilter *tagFilters_ = nullptr;
        uint32_t filterCount_ = 0;
        bool hasFilter_ = false;
        uint32_t *groupBy_ = nullptr;
        uint32_t groupByCount_ = 0;
        uint32_t groupFoundCount_ = 0;
        uint32_t groupPos_ = 0;
        bool hasGroup_ = false;
        bool groupDiff_ = false;

        uint32_t aggCount_ = 0;

        DataPoint *countPoints_ = nullptr;
        DataPoint *sumPoints_ = nullptr;
        DataPoint *minPoints_ = nullptr;
        DataPoint *maxPoints_ = nullptr;

        uint32_t readCount_ = 0;
        uint32_t skipCount_ = 0;
        uint32_t readKeySize_ = 0;
        uint32_t readValueSize_ = 0;
    public:
        MetricsScannerImpl(DB *db, ReadOptions &read_options) {
            db_ = db;
            env_ = db_->GetEnv();
            read_options_ = read_options;
        }

        ~MetricsScannerImpl() {
            if (nullptr != iter_) {
                delete iter_;
            }
            if (nullptr != countPoints_) {
                delete[] countPoints_;
            }
            if (nullptr != sumPoints_) {
                delete[] sumPoints_;
            }
            if (nullptr != maxPoints_) {
                delete[] maxPoints_;
            }
            if (nullptr != minPoints_) {
                delete[] minPoints_;
            }
            if (nullptr != tagFilters_) {
                delete[] tagFilters_;
            }
            if (nullptr != groupBy_) {
                delete[] groupBy_;
            }
            if (nullptr != saveGroupByKey_) {
                delete[] saveGroupByKey_;
            }
            if (nullptr != curGroupByKey_) {
                delete[] curGroupByKey_;
            }
        }

        virtual void setTagFilter(Slice &target) override {
            //tag filters only can be set once
            if (nullptr != tagFilters_) {
                return;
            }
            GetVarint32(&target, &filterCount_);
            if (filterCount_ == 0) {
                return;
            }
            hasFilter_ = true;
            tagFilters_ = new TagFilter[filterCount_];
            for (uint32_t i = 0; i < filterCount_; i++) {
                //read tag name
                uint32_t tagName = 0;
                GetVarint32(&target, &tagName);
                tagFilters_[i].tagName = tagName;
                //read tag value count for this tag name
                uint32_t tagValueCount = 0;
                GetVarint32(&target, &tagValueCount);
                if (tagValueCount == 0) {
                    continue;
                }
                tagFilters_[i].count = tagValueCount;
                //read tag values for this tag name
                tagFilters_[i].tagValues = new uint32_t[tagValueCount];
                for (uint32_t j = 0; j < tagValueCount; j++) {
                    uint32_t tagValue = 0;
                    GetVarint32(&target, &tagValue);
                    tagFilters_[i].tagValues[j] = tagValue;
                }
            }
        }

        virtual void setGroupBy(Slice &target) override {
            //tag group by only can be set once
            if (nullptr != groupBy_) {
                return;
            }
            GetVarint32(&target, &groupByCount_);
            if (groupByCount_ == 0) {
                return;
            }
            hasGroup_ = true;
            groupBy_ = new uint32_t[groupByCount_];

            if (nullptr == saveGroupByKey_) {
                saveGroupByKey_ = new uint32_t[groupByCount_];
            }
            if (nullptr == curGroupByKey_) {
                curGroupByKey_ = new uint32_t[groupByCount_];
            }
            for (uint32_t i = 0; i < groupByCount_; i++) {
                uint32_t groupByTag = 0;
                GetVarint32(&target, &groupByTag);
                groupBy_[i] = groupByTag;
                saveGroupByKey_[i] = 0;
                curGroupByKey_[i] = 0;
            }
        }

        virtual bool hasNext() override {
            return hasValue_;
        }

        virtual void next() override {
            //reset scan context for new loop
            resultSet_.clear();
            currentHour_ = -1;
            finish_ = false;
            hasValue_ = false;

            if (close_) {
                return;
            }
            if (pointCount <= 0) {
                return;
            }
            if (metric <= 0) {
                return;
            }
            if (readHour_ > endHour) {
                return;
            }
            if (nullptr == iter_) {
                iter_ = db_->NewIterator(read_options_);
                seekKey_.clear();
                PutVarint32Varint32(&seekKey_, metric, startHour);
                skip();
            } else {
                iter_->Next();
            }
            DBOptions options;
            if (enableLog) {
                options = db_->GetDBOptions();
            }
            while (!finish_ && iter_->Valid()) {
                Slice key = iter_->key();
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "key : %s %d", key.ToString(true).data(),
                        currentHour_);
                    readKeySize_ += key.size();
                    readCount_++;
                }
                GetVarint32(&key, &readMetric_);
                if (readMetric_ != metric) {
                    //read metric != query metric, finish scan
                    close_ = true;
                    break;
                }

                readHour_ = (uint8_t) key[0];
                key.remove_prefix(1);
                if (readHour_ > endHour) {
                    //finish iterator, because hour > end hour
                    close_ = true;
                    break;
                }

                if (saveHour_ == -1) {
                    currentHour_ = readHour_;
                    saveHour_ = readHour_;
                } else if (saveHour_ != readHour_) {
                    //if hour is different, then finish current hour scan
                    if (enableLog) {
                        Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "hour diff dump");
                    }
                    dumpAllResult();
                    finish_ = true;
                }

                if (hasGroup_) {
                    //reset group context for new key, if need do group by
                    groupFoundCount_ = 0;
                    groupPos_ = 0;
                    groupDiff_ = false;
                }
                //do tag filter if input tag filter
                if (hasFilter_ && filterTag(&key)) {
                    if (enableLog) {
                        Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "skip key : %s",
                            Slice(seekKey_).ToString(true).data());
                        skipCount_++;
                    }
                    bool flag = skip();
                    if (!flag && !finish_) {
                        //if no skip, move to next row
                        iter_->Next();
                    } else if (finish_ && readHour_ < endHour) {
                        //if finish and read hour < end hour, skip to next hour
                        seekKey_.clear();
                        PutVarint32Varint32(&seekKey_, metric, readHour_ + 1);
                        skip();
                    }
                    continue;
                }

                if (hasGroup_) {
                    //if group by diff, then finish current group by scan
                    if (enableLog) {
                        Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "has group by reminding key is %s %d %d",
                            key.ToString(true).data(), groupFoundCount_, groupByCount_);
                    }
                    if (groupFoundCount_ < groupByCount_) {
                        uint32_t tagName = 0;
                        uint32_t tagValue = 0;
                        while (key.size() > 0) {
                            tagName = GetFixed32(key.data());
                            key.remove_prefix(4);
                            tagValue = GetFixed32(key.data());
                            key.remove_prefix(4);
                            doGroupBy(tagName, tagValue);
                            if (groupFoundCount_ == groupByCount_) {
                                break;
                            }
                        }
                    }
                    if (groupFoundCount_ != groupByCount_) {
                        //not find group by, move to next row
                        iter_->Next();
                        continue;
                    }

                    if (groupDiff_) {
                        //if group by diff, then finish current group by scan
                        if (enableLog) {
                            Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "group diff dump");
                        }
                        dumpAllResult();
                        finish_ = true;
                    }
                }

                hasValue_ = true;
                saveHour_ = readHour_;
                aggCount_++;
                Slice value = iter_->value();
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "need add agg key : %s %d",
                        iter_->key().ToString(true).data(), readHour_);
                    readValueSize_ += value.size();
                }
                while (value.size() > 0) {
                    //get point type
                    char point_type = value[0];
                    value.remove_prefix(1);
                    //get point value len
                    uint32_t len = 0;
                    GetVarint32(&value, &len);

                    //do agg
                    if (point_type == point_type_sum) {
                        if (nullptr == sumPoints_) {
                            sumPoints_ = new DataPoint[pointCount];
                        }
                        agg(point_type, len, &value, sumPoints_);
                    } else if (point_type == point_type_count) {
                        if (nullptr == countPoints_) {
                            countPoints_ = new DataPoint[pointCount];
                        }
                        agg(point_type, len, &value, countPoints_);
                    } else if (point_type == point_type_min) {
                        if (nullptr == minPoints_) {
                            minPoints_ = new DataPoint[pointCount];
                        }
                        agg(point_type, len, &value, minPoints_);
                    } else if (point_type == point_type_max) {
                        if (nullptr == maxPoints_) {
                            maxPoints_ = new DataPoint[pointCount];
                        }
                        agg(point_type, len, &value, maxPoints_);
                    }
                }

                //if current hour scan not finish, move to next row
                if (!finish_) {
                    if (enableLog) {
                        Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "do next row ");
                    }
                    iter_->Next();
                } else {
                    if (hasGroup_) {
                        //if save group diff current group and read new value for current group, then reset save group
                        for (uint32_t i = 0; i < groupByCount_; i++) {
                            saveGroupByKey_[i] = curGroupByKey_[i];
                        }
                    }
                    return;
                }
            }
            if (!hasGroup_ && endHour <= readHour_) {
                close_ = true;
            }

            if (aggCount_ > 0) {
                currentHour_ = readHour_;
                dumpAllResult();
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "finish dump");
                }
            }
        }

        virtual int32_t getCurrentHour() override {
            return currentHour_;
        }

        virtual Slice getResultSet() override {
            return resultSet_;
        }

        virtual Slice getGroupBy() override {
            return groupByResult_;
        }

        virtual Slice getStat() override {
            PutVarint32Varint32(&statResult_, readCount_, skipCount_);
            PutVarint32Varint32(&statResult_, readKeySize_, readValueSize_);
            return statResult_;
        }

    private:

        void doGroupBy(uint32_t tagName, uint32_t tagValue) {
            if (groupFoundCount_ >= groupByCount_) {
                return;
            }
            if (enableLog) {
                DBOptions options = db_->GetDBOptions();
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "do group by save group key %d %d %d %d", groupPos_,
                    saveGroupByKey_[groupPos_], tagName, tagValue);
            }
            if (tagName == groupBy_[groupPos_]) {
                curGroupByKey_[groupPos_] = tagValue;
                if (saveGroupByKey_[groupPos_] != 0 && saveGroupByKey_[groupPos_] != tagValue) {
                    groupDiff_ = true;
                } else if (saveGroupByKey_[groupPos_] == 0) {
                    saveGroupByKey_[groupPos_] = tagValue;
                }
                groupFoundCount_++;
                groupPos_++;
            }
        }

        bool skip() {
            if (nullptr == iter_ || seekKey_.size() <= 0) {
                return false;
            }
            iter_->Seek(seekKey_);
            seekKey_.clear();
            return true;
        }

        bool filterTag(Slice *key) {
            size_t pos = key->size();
            if (pos <= 0) {
                return false;
            }
            Slice rawKey = Slice(key->data(), key->size());
            bool backTrack = false;
            uint32_t backTrackName = 0;
            uint32_t backTrackValue = 0;
            int32_t diff = 0;
            size_t backTrackPos = pos;
            uint32_t tagNameFilter = 0;

            for (uint32_t i = 0; i < filterCount_; i++) {
                TagFilter &tagFilter = tagFilters_[i];
                tagNameFilter = tagFilter.tagName;
                uint32_t tagValue = 0;
                uint32_t tagName = 0;
                pos = key->size();
                while (pos > 0) {
                    tagName = GetFixed32(key->data());
                    key->remove_prefix(4);
                    tagValue = GetFixed32(key->data());
                    key->remove_prefix(4);
                    //do group by if need
                    if (hasGroup_) {
                        doGroupBy(tagName, tagValue);
                    }
                    diff = tagNameFilter - tagName;
//                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "tag name diff is %d %d %d",tagNameFilter,tagName,diff);
                    if (diff <= 0) {
                        //query tag name small or equals read tag name
                        break;
                    } else {
                        //if query tag name large read tag name, track it, maybe can skip to read tag value + 1
                        backTrackName = tagName;
                        backTrackValue = tagValue + 1;
                        backTrackPos = pos;
                    }
                    pos = key->size();
                }
                if (diff == 0) {
                    uint32_t upperIndex = binarySearch(tagFilter.count, tagFilter.tagValues, tagValue);
                    if (upperIndex == tagFilter.count) {
                        //tag value larger than all filter tag values
                        backTrack = true;
                        break;
                    } else {
                        uint32_t upper = tagFilter.tagValues[upperIndex];
                        if (upper == tagValue) {
                            //match tag value, track for next filter tag value, maybe can skip to it
                            if (upperIndex < tagFilter.count - 1) {
                                backTrackName = tagName;
                                backTrackValue = tagFilter.tagValues[upperIndex + 1];
                            }
                            //compare next tag filter
                            continue;
                        } else {
                            //skip to upper tag value for this tag name
                            createHint(&rawKey, pos, tagName, upper);
                            return true;
                        }
                    }
                } else {
                    backTrack = true;
                    break;
                }
            }
            if (backTrack) {
                //if current query tag name is not first tag name, seek to next tag value for before tag name
                if (backTrackName > 0) {
                    if (diff > 0) {
                        //if query tag name larger than read tag name, skip to query tag name
                        createHint(&rawKey, backTrackPos, tagNameFilter);
                    } else {
                        //skip to next tag value for before tag name
                        createHint(&rawKey, backTrackPos, backTrackName, backTrackValue);
                    }
                    return true;
                } else {
                    //filter tag value smaller than first tag value, no data match, finish current next,
                    //maybe have next hour
                    finish_ = true;
                    return true;
                }
            }
            return false;
        }

        void createHint(Slice *key, uint32_t pos, uint32_t tagName, uint32_t tagValue) {
            copyHintPrefix(key, pos);
            PutFixed32Value(&seekKey_, tagName);
            PutFixed32Value(&seekKey_, tagValue);
        }

        void createHint(Slice *key, uint32_t pos, uint32_t tagName) {
            copyHintPrefix(key, pos);
            PutFixed32Value(&seekKey_, tagName);
        }

        void copyHintPrefix(Slice *key, uint32_t pos) {
            seekKey_.clear();
            PutVarint32Varint32(&seekKey_, metric, readHour_);
            uint32_t len = key->size() - pos;
            for (uint32_t i = 0; i < len; i++) {
                seekKey_.append(1, key->data()[i]);
            }
        }

        void dumpAllResult() {
            currentHour_ = saveHour_;
            if (hasGroup_) {
                groupByResult_.clear();
                for (uint32_t i = 0; i < groupByCount_; i++) {
                    PutVarint32(&groupByResult_, saveGroupByKey_[i]);
                    saveGroupByKey_[i] = 0;
                }
            }
            dumpResult(point_type_sum, sumPoints_);
            dumpResult(point_type_count, countPoints_);
            dumpResult(point_type_min, minPoints_);
            dumpResult(point_type_max, maxPoints_);
            aggCount_ = 0;
        }

        void dumpResult(char point_type, DataPoint *aggMap) {
            if (nullptr == aggMap) {
                return;
            }
            std::string aggResult = "";
            for (uint32_t i = 0; i < pointCount; i++) {
                DataPoint *point = &aggMap[i];
                if (point->hasValue) {
                    PutVarint32Varint64(&aggResult, i, point->value);
                    point->hasValue = false;
                }
            }
            if (aggResult.size() > 0) {
                resultSet_.append(1, point_type);
                PutVarint32(&resultSet_, (uint32_t) aggResult.size());
                resultSet_.append(aggResult);
            }
        }

        void agg(char point_type, size_t len, Slice *value, DataPoint *aggMap) {
            size_t size = value->size();
            while (size - value->size() < len) {
                int32_t slot;
                GetVarint32(value, (uint32_t *) &slot);
                int64_t val;
                GetVarint64(value, (uint64_t *) &val);

                DataPoint *point = &aggMap[slot];
                if (!point->hasValue) {
                    point->value = val;
                    point->hasValue = true;
                } else {
                    if (point_type == point_type_sum || point_type == point_type_count) {
                        point->value += val;
                    } else if (point_type == point_type_min) {
                        point->value = point->value > val ? val : point->value;
                    } else if (point_type == point_type_max) {
                        point->value = point->value < val ? val : point->value;
                    }
                }
            }
        }

        /**
        *  find smallest value index in tag values, which >= tag value
        *  if all tag values are < value, return tag values length
        */
        uint32_t binarySearch(uint32_t tagValueLen, uint32_t *tagValues, uint32_t queryValue) {
            if (queryValue <= tagValues[0]) {
                return 0;
            }
            uint32_t low = 0;
            uint32_t high = tagValueLen - 1;
            if (queryValue > tagValues[high]) {
                return tagValueLen;
            }
            while (true) {
                uint32_t mid = (low + high) / 2;
                uint32_t midVal = tagValues[mid];
                if (midVal < queryValue) {
                    low = mid + 1;
                    if (tagValues[low] >= queryValue) {
                        return low;
                    }
                } else if (midVal > queryValue) {
                    high = mid - 1;
                    if (tagValues[high] < queryValue) {
                        return mid;
                    }
                } else {
                    return mid;//key found
                }
            }
        }

        uint32_t GetFixed32(const char *ptr) {
            return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[3] & 0xff)))
                    | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2] & 0xff)) << 8)
                    | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1] & 0xff)) << 16)
                    | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0] & 0xff))) << 24);
        }

        void PutFixed32Value(std::string *buf, uint32_t value) {
            buf->append(1, (char) (value >> 24 & 0xff));
            buf->append(1, (char) (value >> 16 & 0xff));
            buf->append(1, (char) (value >> 8 & 0xff));
            buf->append(1, (char) (value & 0xff));
        }
    };

    MetricsScanner *NewMetricsScannerImpl(DB *db, ReadOptions &read_options) {
        return new MetricsScannerImpl(db, read_options);
    }
}//namespace rocksdb
