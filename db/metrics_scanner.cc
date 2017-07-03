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
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "utilities/tsdb/TimeSeriesStreamReader.h"
#include "utilities/tsdb/TimeSeriesStreamWriter.h"

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
    class LinDBHistogram {
    public:
        int32_t slot = -1;
        int64_t type = -1;
        int64_t baseNumber = 0;
        int64_t maxSlot = 0;
        int64_t min = 1;
        int64_t max = 1;
        int64_t count = 0;
        int64_t sum = 0;
        int64_t *values = nullptr;
        LinDBHistogram *prev_ = nullptr;
        LinDBHistogram *next_ = nullptr;

        LinDBHistogram() {
            min = min << 62;
            max = (max << 63) >> 63;
        }

        ~LinDBHistogram() {
            if (nullptr != values) {
                delete[] values;
            }
            if (nullptr != next_) {
                delete next_;
            }
        }
    };

    class HistogramAggregator {
    private:
        uint8_t firstStats_ = 0;
        std::string firstResult_ = "";
        std::string resultSet_ = "";
        LinDBHistogram *histograms_ = nullptr;
    public:
        HistogramAggregator() {}

        virtual ~HistogramAggregator() {
            if (nullptr != histograms_) {
                delete histograms_;
            }
        }

        std::string dumpResult() {
            resultSet_.clear();
            if (1 == firstStats_) {
                resultSet_.assign(firstResult_.data(), firstResult_.size());
                firstStats_ = 0;
                firstResult_.clear();
                return resultSet_;
            } else if (2 == firstStats_) {
                firstStats_ = 0;
            } else if (nullptr == histograms_) {
                return resultSet_;
            }

            TimeSeriesStreamWriter writer(&resultSet_);
            LinDBHistogram *histogram = histograms_;
            while (nullptr != histogram) {
                writer.appendTimestamp(histogram->slot);//slot
                writer.appendValue(histogram->type);//type
                writer.appendValue(histogram->baseNumber);//baseNumber
                writer.appendValue(histogram->maxSlot);//max value slot
                writer.appendValue(histogram->min);//min
                writer.appendValue(histogram->max);//max
                writer.appendValue(histogram->count);//count
                writer.appendValue(histogram->sum);//sum
                for (int64_t i = 0; i < histogram->maxSlot; ++i) {// values
                    writer.appendValue(histogram->values[i]);
                }
                histogram = histogram->next_;
            }
            writer.flush();
            delete histograms_;
            histograms_ = nullptr;
            return resultSet_;
        }

        LinDBHistogram *findOrCreateLinDBHistogram(LinDBHistogram *linDBHistogram, const int32_t slot, bool &existing) {
            if (nullptr == linDBHistogram) {
                if (nullptr == histograms_) {
                    histograms_ = new LinDBHistogram();
                    histograms_->slot = slot;
                    existing = false;
                    return histograms_;
                }
                linDBHistogram = histograms_;
            }
            LinDBHistogram *index = linDBHistogram;
            while (slot > index->slot && nullptr != index->next_) {
                index = index->next_;
            }
            if (slot == index->slot) {
                existing = true;
                return index;
            } else if (slot < index->slot) {
                linDBHistogram = new LinDBHistogram();
                linDBHistogram->next_ = index;
                if (nullptr != index->prev_) {
                    index->prev_->next_ = linDBHistogram;
                    linDBHistogram->prev_ = index->prev_;
                }
                index->prev_ = linDBHistogram;
                existing = false;
                return linDBHistogram;
            } else {
                linDBHistogram = new LinDBHistogram();
                linDBHistogram->prev_ = index;
                index->next_ = linDBHistogram;
                existing = false;
                return linDBHistogram;
            }

        }


        void add(const char *value, const uint32_t value_size) {
            TimeSeriesStreamReader newStream(value, value_size);
            int32_t slot = newStream.getNextTimestamp();
            LinDBHistogram *histogram = nullptr;
            while (slot != -1) {
                bool existing = true;
//                std::cout << "C++  frist crate : " << existing << "  slot : " << slot << std::endl;
                histogram = findOrCreateLinDBHistogram(histogram, slot, existing);
//                std::cout << "C++  is crate : " << existing << std::endl;
                int64_t type = newStream.getNextValue();//type
                int64_t baseNumber = newStream.getNextValue();//baseNumber
                int64_t max_slot = newStream.getNextValue();//maxSlot
                int64_t min = newStream.getNextValue();//min
                int64_t max = newStream.getNextValue();//max
                int64_t count = newStream.getNextValue();//count
                int64_t sum = newStream.getNextValue();//sum
                if (!existing) {
                    histogram->slot = slot;
                    histogram->type = type;
                    histogram->baseNumber = baseNumber;
                    histogram->maxSlot = max_slot;
                    histogram->min = min;
                    histogram->max = max;
                    histogram->count = count;
                    histogram->sum = sum;
                    histogram->values = new int64_t[max_slot];
                    for (int i = 0; i < max_slot; ++i) {
                        histogram->values[i] = {newStream.getNextValue()};// value
                    }
                } else if (type != histogram->type || baseNumber != histogram->baseNumber ||
                           max_slot != histogram->maxSlot) {
                    for (int64_t i = 0; i < max_slot; ++i) {
                        newStream.getNextValue();// value
                    }
                } else {
                    if (histogram->min > min) {
                        histogram->min = min;
                    }
                    if (histogram->max < max) {
                        histogram->max = max;
                    }
                    histogram->sum += sum;
                    for (int64_t i = 0; i < max_slot; ++i) {
                        histogram->values[i] += newStream.getNextValue();//value
                    }
                }
                slot = newStream.getNextTimestamp();
            }
        }

        void merge(const char *value, const uint32_t value_size) {
            if (0 == firstStats_) {
                firstResult_.assign(value, value_size);
                firstStats_ = 1;
                return;
            } else if (1 == firstStats_) {
                add(firstResult_.data(), firstResult_.size());
                firstResult_.clear();
                firstStats_ = 2;
            }
            add(value, value_size);
        }
    };

    class MetricsScannerImpl : public MetricsScanner {
    private:
        DB *db_;
        Env *env_;
        ReadOptions read_options_;
        HistogramAggregator *aggregator_ = nullptr;

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
                    Log(InfoLogLevel::ERROR_LEVEL, dbOptions.info_log, "has next base time is : %d",
                        currentBaseTime_);
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
//            aggregator_ = new HistogramAggregator();
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
//                    std::cout << "C++  merge in counter " << std::endl;
                    aggregator_->merge(value.data(), (uint32_t) value.size());
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
            resultSet_ = aggregator_->dumpResult();//here is keep the string, it is must
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
