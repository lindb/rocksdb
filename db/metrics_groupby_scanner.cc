//
// Created by jie.huang on 16/11/22.
//

#include <iostream>
#include <map>
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "util/coding.h"

namespace rocksdb {
    struct DataPoint {
        bool hasValue = false;
        int64_t value = 0;
    };

    class MetricsGroupByScannerImpl : public MetricsGroupByScanner {
    private:
        DB *db_;
        Env *env_;
        ReadOptions read_options_;

        Iterator *iter_ = nullptr;
        std::string seekKey = "";

        std::vector<uint32_t> groupBy;

        bool hasValue = false;

        int metricLen = 0;

        int currentHour = 0;
        uint32_t groupSize = 0;
        uint32_t metric_ = 0;

        std::string resultSet_ = "";
        std::string groupResult_ = "";

        uint32_t *curGroupByKey = nullptr;
        uint32_t *saveGroupByKey = nullptr;

        DataPoint *countPoints_ = nullptr;
        DataPoint *sumPoints_ = nullptr;
        DataPoint *minPoints_ = nullptr;
        DataPoint *maxPoints_ = nullptr;
    public:
        MetricsGroupByScannerImpl(DB *db, ReadOptions &read_options) {
            db_ = db;
            env_ = db_->GetEnv();
            read_options_ = read_options;
        }

        ~MetricsGroupByScannerImpl() {
            if (iter_ != nullptr) {
                delete iter_;
            }
            if (countPoints_ != nullptr) {
                delete[] countPoints_;
            }
            if (sumPoints_ != nullptr) {
                delete[] sumPoints_;
            }
            if (maxPoints_ != nullptr) {
                delete[] maxPoints_;
            }
            if (minPoints_ != nullptr) {
                delete[] minPoints_;
            }

            if (curGroupByKey != nullptr) {
                delete[] curGroupByKey;
            }
            if (saveGroupByKey != nullptr) {
                delete[] saveGroupByKey;
            }
        }

        virtual void addGroupBy(uint32_t groupByCount, Slice &target) override {
            if (curGroupByKey == nullptr) {
                curGroupByKey = new uint32_t[groupByCount];
            }
            if (saveGroupByKey == nullptr) {
                saveGroupByKey = new uint32_t[groupByCount];
            }
            groupSize = groupByCount;
            for (uint32_t i = 0; i < groupSize; i++) {
                uint32_t groupByTagName = 0;
                GetVarint32(&target, &groupByTagName);
                groupBy.push_back(groupByTagName);
                curGroupByKey[i] = 0;
                saveGroupByKey[i] = 0;
            }
        }

        virtual bool hasNext() override {
            return hasValue;
        }

        virtual void next() override {
            resultSet_.clear();
            currentHour = -1;
            hasValue = false;
            DBOptions options = db_->GetDBOptions();
            if (iter_ == nullptr) {
                init();
                if (seekKey.size() <= 0) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics seek group by scanner do next: %s %d",
                        Slice(seekKey).ToString(true).data(), seekKey.size());
                    return;
                }
                if (metricLen <= 0) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics len group by scanner do next: %d ",
                        metricLen);
                    return;
                }
                iter_ = db_->NewIterator(read_options_);
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics  do seek =|%s|",
                    Slice(seekKey).ToString(true).data());
                iter_->Seek(seekKey);
            } else {
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics  do next");
                iter_->Next();
            }

            Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics group by scanner do next: %d %s",
                metric, iter_->Valid() ? "true" : "false");

            bool needDump = false;
            bool diff = false;//flag if current group key is different with save group key
            for (; iter_->Valid(); iter_->Next()) {
                Slice key = iter_->key();
                GetVarint32(&key, &metric_);
                if (metric != metric_) {
                    break;
                }
                uint32_t hour = 0;
                GetVarint32(&key, &hour);

                //finish iterator, because hour > end hour
                if (hour > endHour) {
                    break;
                }

                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics key is======: %s %d %d %d %d",
                    iter_->key().ToString(true).data(), saveGroupByKey[0], startHour, hour, endHour);
                size_t foundCount = 0;
                for (uint32_t i = 0; i < groupSize; i++) {
                    uint32_t tagName = 0;
                    uint32_t tagValue = 0;
                    while (key.size() > 0) {
                        GetVarint32(&key, &tagName);
                        GetVarint32(&key, &tagValue);
                        if (tagName == groupBy[i]) {
                            curGroupByKey[i] = tagValue;
                            if (saveGroupByKey[i] != 0 && saveGroupByKey[i] != tagValue) {
                                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics key is diff: %s=%s %d %d %d",
                                    iter_->key().ToString(true).data(), Slice(groupResult_).ToString(true).data(), startHour, hour, endHour);
                                diff = true;
                            }
                            foundCount++;
                            break;
                        }
                    }
                    if (foundCount == groupSize) {
                        break;
                    }
                }
                if (foundCount != groupSize) {
                    //not match reset diff flag
                    diff = false;
                    continue;
                }
                //set first group by key
                if (saveGroupByKey[0] == 0) {
                    for (uint32_t i = 0; i < groupSize; i++) {
                        //exchange current group by to save group by for new
                        saveGroupByKey[i] = curGroupByKey[i];
                    }
                }
                //set first hour
                if (currentHour == -1) {
                    currentHour = hour;
                }

                if (diff || currentHour != hour) {
                    needDump = true;
                }
                Slice value = iter_->value();
                if (!hasValue) {
                    hasValue = value.size() > 0;
                }

                if (hasValue && needDump) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics key is: %s=%s %d %d %d",
                        iter_->key().ToString(true).data(), Slice(groupResult_).ToString(true).data(), startHour, hour, endHour);
                    dumpAllResult();
                } else {
                    needDump = false;
                }
                while (value.size() > 0) {
                    //get point type
                    char point_type = value[0];
                    value.remove_prefix(1);
                    //get point value len
                    uint32_t len = 0;
                    GetVarint32(&value, &len);

                    if (point_type == point_type_sum) {
                        //do agg
                        agg(point_type, len, &value, sumPoints_);
                    } else if (point_type == point_type_count) {
                        agg(point_type, len, &value, countPoints_);
                    } else if (point_type == point_type_min) {
                        agg(point_type, len, &value, minPoints_);
                    } else if (point_type == point_type_max) {
                        agg(point_type, len, &value, maxPoints_);
                    }
                }

                if (needDump) {
                    return;
                }

            }
                //dump aggregator result
                dumpAllResult();
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "dump result only data");
        }

        virtual int32_t getCurrentHour() override {
            return currentHour;
        }

        virtual Slice getGroupBy() override {
            return groupResult_;
        }

        virtual Slice getResultSet() override {
            return resultSet_;
        }

    private:
        void init() {
            if (pointCount <= 0) {
                return;
            }
            if (metric > 0 && startHour > 0 && endHour > 0) {
                PutVarint32Varint32(&seekKey, metric, startHour);
            } else {
                return;
            }
            metricLen = VarintLength(metric);
            if (countPoints_ == nullptr) {
                countPoints_ = new DataPoint[pointCount];
            }
            if (sumPoints_ == nullptr) {
                sumPoints_ = new DataPoint[pointCount];
            }
            if (maxPoints_ == nullptr) {
                maxPoints_ = new DataPoint[pointCount];
            }
            if (minPoints_ == nullptr) {
                minPoints_ = new DataPoint[pointCount];
            }
        }

        void dumpAllResult() {
            if (saveGroupByKey != nullptr) {
                groupResult_.clear();
                for (uint32_t i = 0; i < groupSize; i++) {
                    PutVarint32(&groupResult_, saveGroupByKey[i]);
                    //exchange current group by to save group by for new
                    saveGroupByKey[i] = curGroupByKey[i];
                }
            }

            dumpResult(point_type_sum, sumPoints_);
            dumpResult(point_type_count, countPoints_);
            dumpResult(point_type_min, minPoints_);
            dumpResult(point_type_max, maxPoints_);
        }

        void dumpResult(char point_type, DataPoint *aggMap) {
            if (aggMap == nullptr) {
                return;
            }
            std::string aggResult = "";
            for (uint32_t i = 0; i < pointCount; i++) {
                DataPoint *point = &aggMap[i];
                if (point->hasValue) {
                    PutVarint32Varint64(&aggResult, i, point->value);
                    point->hasValue = false;
                    hasValue = true;
                }
            }
            if (aggResult.size() > 0) {
                resultSet_.append(1, point_type);
                PutVarint32(&resultSet_, (uint32_t) aggResult.size());
                resultSet_.append(aggResult);
            }
        }

        void clearGroupByKey(uint32_t *groupByKey) {
            if (groupByKey == nullptr) {
                return;
            }
            for (uint32_t i = 0; i < groupSize; i++) {
                groupByKey[i] = 0;
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
         *  find smallest value index in tag name ids, which >= tag name id
         *  if all values are < value, return tag name ids length
         */
        int32_t binarySearch(uint32_t tagNameId) {
//            int32_t low = 0;
//            int32_t high = tagNamesIdLen - 1;
//            while (low <= high) {
//                int32_t mid = (low + high) / 2;
//                int midVal = tagNamesId[mid];
//                if (midVal < tagNameId) {
//                    low = mid + 1;
//                } else if (midVal > tagNameId) {
//                    high = mid - 1;
//                } else {
//                    return mid;//key found
//                }
//            }
            return -1;//key found found
        }
    };

    MetricsGroupByScanner *NewMetricsGroupByScannerImpl(DB *db, ReadOptions &read_options) {
        return new MetricsGroupByScannerImpl(db, read_options);
    }
}//namespace rocksdb
