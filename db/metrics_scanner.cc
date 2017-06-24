//
// Created by jie.huang on 16/11/15.
//
#include <iostream>
#include <utilities/tsdb/CounterMerger.h>
#include <utilities/tsdb/GaugeMerger.h>
#include <utilities/tsdb/RatioMerger.h>
#include <utilities/tsdb/TSDB.h>
#include <utilities/tsdb/ApdexMerger.h>
#include <utilities/tsdb/TimerMerger.h>
#include <utilities/tsdb/PayloadMerger.h>
#include <utilities/tsdb/HistogramMerger.h>
#include <utilities/tsdb/Aggregator.cc>
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "util/coding.h"

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
        Aggregator *aggregator_ = nullptr;

        std::string seekKey_ = "";
        uint32_t readMetric_ = 0;
        uint8_t currentBaseTime_ = 0;

        uint32_t *curGroupByKey_ = nullptr;
        uint32_t *saveGroupByKey_ = nullptr;


        Iterator *iter_ = nullptr;
        bool close_ = false;
        bool finish_ = false;

        std::string resultSet_ = "";
        std::string tempResult_ = "";

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
            if (nullptr != aggregator_) {
                delete aggregator_;
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

        virtual bool hasNextBaseTime(char nextBaseTime, ColumnFamilyHandle *columnFamilyHandle) override {
            DBOptions dbOptions = db_->GetDBOptions();
            if (enableLog) {
                Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "has next base time :%d %d %d %s %s", start, end,
                    nextBaseTime,
                    close_ ? "true" : "false", finish_ ? "true" : "false");
            }
            if (close_) {
                return false;
            }
            finish_ = false;
            if (nullptr != iter_) {
                delete iter_;
            }
            if (nullptr == aggregator_) {
                if (metric_type == TSDB::METRIC_TYPE_HISTOGRAM) {
                    aggregator_ = new HistogramAggregator();
                }
            }
            iter_ = db_->NewIterator(read_options_, columnFamilyHandle);
            seekKey_.clear();
            seekKey_.append(1, nextBaseTime);
            seekKey_.append(1, metric_type);
            PutVarint32(&seekKey_, metric);
            seekKey_.append(1, minTagValueLen);
            if (enableLog) {
                Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "has next base time skip key : %s",
                    Slice(seekKey_).ToString(true).data());
            }
            skip();

            if (iter_->Valid()) {
                Slice key = iter_->key();
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "has next base time read key : %s",
                        key.ToString(true).data());
                }
                currentBaseTime_ = (uint8_t) key[0];
                key.remove_prefix(1);//remove base time
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "has next base time is : %d", currentBaseTime_);
                }
                if (currentBaseTime_ > end) {
                    //finish scan, because current base time > query end base time
                    close_ = true;
                    return false;
                }
                return true;
            } else {
                return false;
            }
        }

        virtual bool hasNext() override {
            return !finish_ && !close_;
        }

        virtual void next() override {
            //reset scan context for new loop
            resultSet_.clear();
            tempResult_.clear();
            if (close_ || pointCount <= 0 || metric <= 0) {
                return;
            }
            if (!iter_->Valid()) {
                close_ = true;
                return;
            }
            finish_ = false;
            DBOptions dbOptions = db_->GetDBOptions();
            while (!finish_ && iter_->Valid()) {
                Slice key = iter_->key();
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "key : %s %d", key.ToString(true).data(),
                        currentBaseTime_);
                }
                if (enableProfiler) {
                    readKeySize_ += key.size();
                    readCount_++;
                }
                uint8_t readBaseTime = (uint8_t) key[0];
                key.remove_prefix(1);

                if (readBaseTime > end) {
                    close_ = true;
                    break;
                }

                if (readBaseTime != currentBaseTime_) {
                    finish_ = true;
                    break;
                }

                char metricType = key[0];
                key.remove_prefix(1);//remove metric type
                if (metric_type != metricType) {
                    //read metric type != query metric type, finish scan
                    finish_ = true;
                    break;
                }
                GetVarint32(&key, &readMetric_);
                if (readMetric_ != metric) {
                    //read metric != query metric, finish scan
                    finish_ = true;
                    break;
                }

                if (hasGroup_) {
                    //reset group context for new key, if need do group by
                    groupFoundCount_ = 0;
                    groupPos_ = 0;
                    groupDiff_ = false;
                }
                //do tag filter if input tag filter
                char maxTagValueLen = key[0];
                key.remove_prefix(1);
                if (minTagValueLen == 0 && maxTagValueLen > 0) {
                    finish_ = true;//key has tag, but query no tag metric
                    break;
                }
                if (minTagValueLen > 0 && maxTagValueLen == 0) {
                    //maybe can skip to has tag value metric data
                    seekKey_.clear();
                    seekKey_.append(1, readBaseTime);
                    seekKey_.append(1, metric_type);
                    PutVarint32(&seekKey_, metric);
                    seekKey_.append(1, minTagValueLen);
                    skip();
                    continue;
                }
                if (hasFilter_ && filterTag(&key, maxTagValueLen)) {
                    if (enableLog) {
                        Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "skip key : %s",
                            Slice(seekKey_).ToString(true).data());
                    }
                    if (enableProfiler) {
                        skipCount_++;
                    }
                    bool flag = skip();
                    if (!flag && !finish_) {
                        //if no skip, move to next row
                        iter_->Next();
                    }
                    continue;
                }

                if (hasGroup_) {
                    //if group by diff, then finish current group by scan
                    if (enableLog) {
                        Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "has group by reminding key is %s %d %d",
                            key.ToString(true).data(), groupFoundCount_, groupByCount_);
                    }
                    if (groupFoundCount_ < groupByCount_) {
                        uint32_t tagName = 0;
                        uint32_t tagValue = 0;
                        while (key.size() > 0) {
                            tagName = GetFixed32(key.data(), 4);
                            key.remove_prefix(4);
                            tagValue = GetFixed32(key.data(), 4);
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
                            Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "group diff dump result");
                        }
                        dumpAllResult();
                        return;
                    }
                }

                if (finish_) {
                    break;
                }

                aggCount_++;
                Slice value = iter_->value();
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "need add agg key : %s",
                        iter_->key().ToString(true).data());
                }
                if (enableProfiler) {
                    readValueSize_ += value.size();
                }
                if (metric_type == TSDB::METRIC_TYPE_COUNTER) {
                    CounterMerger::merge(resultSet_.data(), (uint32_t) resultSet_.length(), value.data(),
                                         (uint32_t) value.size(), &tempResult_);
                } else if (metric_type == TSDB::METRIC_TYPE_GAUGE) {
                    GaugeMerger::merge(resultSet_.data(), (uint32_t) resultSet_.length(), value.data(),
                                       (uint32_t) value.size(), &tempResult_);
                } else if (metric_type == TSDB::METRIC_TYPE_RATIO) {
                    RatioMerger::merge(resultSet_.data(), (uint32_t) resultSet_.length(), value.data(),
                                       (uint32_t) value.size(), &tempResult_);
                } else if (metric_type == TSDB::METRIC_TYPE_TIMER) {
                    TimerMerger::merge(resultSet_.data(), (uint32_t) resultSet_.length(), value.data(),
                                       (uint32_t) value.size(), &tempResult_);
                } else if (metric_type == TSDB::METRIC_TYPE_APDEX) {
                    ApdexMerger::merge(resultSet_.data(), (uint32_t) resultSet_.length(), value.data(),
                                       (uint32_t) value.size(), &tempResult_);
                } else if (metric_type == TSDB::METRIC_TYPE_PAYLOAD) {
                    PayloadMerger::merge(resultSet_.data(), (uint32_t) resultSet_.length(), value.data(),
                                         (uint32_t) value.size(), &tempResult_);
                } else if (metric_type == TSDB::METRIC_TYPE_HISTOGRAM) {
//                    HistogramMerger::merge(resultSet_.data(), (uint32_t) resultSet_.length(), value.data(),
//                                           (uint32_t) value.size(), &tempResult_);
                    aggregator_->merge(value.data(), value.size());
                }
                resultSet_ = tempResult_;
                tempResult_.clear();
                //if current hour scan not finish, move to next row
                if (!finish_) {
                    if (enableLog) {
                        Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "do next row ");
                    }
                    iter_->Next();
                }
            }

            if (!iter_->Valid()) {
                finish_ = true;
            }

            if (aggCount_ > 0) {
                dumpAllResult();
                if (enableLog) {
                    Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "finish dump");
                }
            }
        }

        virtual int32_t getCurrentBaseTime() override {
            return currentBaseTime_;
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
                DBOptions dbOptions = db_->GetDBOptions();
                Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "do group by save group key %d %d %d %d",
                    groupPos_,
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

        bool filterTag(Slice *key, char maxTagValueLen) {
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
                    tagName = GetFixed32(key->data(), 4);
                    key->remove_prefix(4);
                    tagValue = GetFixed32(key->data(), 4);
                    key->remove_prefix(4);
                    //do group by if need
                    if (hasGroup_) {
                        doGroupBy(tagName, tagValue);
                    }
                    diff = tagNameFilter - tagName;
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
                            createHint(&rawKey, pos, tagName, upper, 4, maxTagValueLen);
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
                        createHint(&rawKey, backTrackPos, tagNameFilter, 4, maxTagValueLen);
                    } else {
                        //skip to next tag value for before tag name
                        createHint(&rawKey, backTrackPos, backTrackName, backTrackValue, 4,
                                   maxTagValueLen);
                    }
                    return true;
                } else {
                    //filter tag value smaller than first tag value, no data match, finish current next,
                    //maybe have next base time
                    finish_ = true;
                    return true;
                }
            }
            return false;
        }

        void createHint(Slice *key, uint32_t pos, uint32_t tagName, uint32_t tagValue, char maxTagNameLen,
                        char maxTagValueLen) {
            char tagValueLen = variableLengthSize(tagValue);
            if (tagValueLen < maxTagValueLen) {
                tagValueLen = maxTagValueLen;
            }
            copyHintPrefix(key, pos, tagValueLen);
            PutFixed32Value(&seekKey_, tagName, 4);
            PutFixed32Value(&seekKey_, tagValue, 4);
        }

        void createHint(Slice *key, uint32_t pos, uint32_t tagName, char maxTagNameLen, char maxTagValueLen) {
            copyHintPrefix(key, pos, maxTagValueLen);
            PutFixed32Value(&seekKey_, tagName, 4);
        }

        void copyHintPrefix(Slice *key, uint32_t pos, char maxTagValue) {
            seekKey_.clear();
            seekKey_.append(1, (char) currentBaseTime_);
            seekKey_.append(1, metric_type);
            PutVarint32(&seekKey_, metric);
            seekKey_.append(1, maxTagValue);
            uint32_t len = key->size() - pos;
            for (uint32_t i = 0; i < len; i++) {
                seekKey_.append(1, key->data()[i]);
            }
        }

        void dumpAllResult() {
            if (hasGroup_) {
                groupByResult_.clear();
                for (uint32_t i = 0; i < groupByCount_; i++) {
                    PutFixed32Value(&groupByResult_, saveGroupByKey_[i], 4);
                    saveGroupByKey_[i] = 0;
                }
            }
            aggCount_ = 0;
            resultSet_ = aggregator_->dumpResult();
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

        uint32_t GetFixed32(const char *ptr, char len) {
            if (len == 1) {
                return (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0] & 0xff)));
            } else if (len == 2) {
                return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[1] & 0xff)))
                        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0] & 0xff))) << 8);
            } else if (len == 3) {
                return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[2] & 0xff)))
                        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1] & 0xff)) << 8)
                        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0] & 0xff))) << 16);
            } else {
                return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[3] & 0xff)))
                        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2] & 0xff)) << 8)
                        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1] & 0xff)) << 16)
                        | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[0] & 0xff))) << 24);
            }
        }

        void PutFixed32Value(std::string *buf, uint32_t value, char len) {
            if (len == 1) {
                buf->append(1, (char) (value & 0xff));
            } else if (len == 2) {
                buf->append(1, (char) (value >> 8 & 0xff));
                buf->append(1, (char) (value & 0xff));
            } else if (len == 3) {
                buf->append(1, (char) (value >> 16 & 0xff));
                buf->append(1, (char) (value >> 8 & 0xff));
                buf->append(1, (char) (value & 0xff));
            } else {
                buf->append(1, (char) (value >> 24 & 0xff));
                buf->append(1, (char) (value >> 16 & 0xff));
                buf->append(1, (char) (value >> 8 & 0xff));
                buf->append(1, (char) (value & 0xff));
            }
        }

        char variableLengthSize(uint32_t value) {
            if (value < (1 << 8) && value >= 0) {
                return 1;
            } else if (value < (1 << 16) && value > 0) {
                return 2;
            } else if (value < (1 << 24) && value > 0) {
                return 3;
            } else {
                return 4;
            }
        }
    };

    MetricsScanner *NewMetricsScannerImpl(DB *db, ReadOptions &read_options) {
        return new MetricsScannerImpl(db, read_options);
    }
}//namespace rocksdb
