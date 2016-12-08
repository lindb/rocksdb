//
// Created by jie.huang on 16/11/22.
//

#include <iostream>
#include <map>
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "util/coding.h"

typedef struct {
    char value[4];
} tagValue;


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
        std::string groupByKey = "";
        std::string saveGroup = "";

        uint32_t curTagNameId = 0;

        size_t tagNamesIdLen;


        std::vector<uint32_t> tagNamesId;
        std::vector<std::string> groupBys;

        std::vector<uint32_t> curGroupByKey;

        bool hasValue = false;
        bool newGroup = false;

        int metricLen = 0;

        int currentHour = 0;

        std::string resultSet_ = "";

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
        }

        virtual void addGroupBy(uint32_t tagNameId, Slice &target) override {
            tagNamesId.push_back(tagNameId);
            std::string groupBy = "";
            groupBy.append(target.data(), target.size());
            groupBys.push_back(groupBy);
        }

        virtual bool hasNext() override {
            return hasValue;
        }

        virtual void next() override {
            resultSet_.clear();
            groupByKey.clear();
            currentHour = -1;
            hasValue = false;
            newGroup = false;
            DBOptions options = db_->GetDBOptions();
            if (iter_ == nullptr) {
                init();
                if (seekKey.size() <= 0) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics seek group by scanner do next: %s %d",
                        Slice(seekKey).ToString(true).data(),seekKey.size() );
                    return;
                }
                if (metricLen <= 0) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics len group by scanner do next: %d ",
                        metricLen);
                    return;
                }
                iter_ = db_->NewIterator(read_options_);
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics  do seek =|%s|",Slice(seekKey).ToString(true).data());
                iter_->Seek(seekKey);
                seekKey.clear();
            } else {
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics  do next");
                iter_->Next();
            }

            Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics group by scanner do next: %d %d %s",
                metric, tagNamesIdLen,iter_->Valid()?"true":"false");
            for (; iter_->Valid(); iter_->Next()) {
                Slice key = iter_->key();

                key.remove_prefix(metricLen);
                uint32_t hour = 0;
                GetVarint32(&key, &hour);

                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics key is: %s=%s %d %d %d",
                    key.ToString(true).data(), Slice(seekKey).ToString(true).data(),startHour,hour,endHour);
                //finish iterator, because hour > end hour
                if (hour > endHour) {
                    break;
                }


                uint32_t tagNameId = 0;
                GetVarint32(&key, &tagNameId);
                if (curTagNameId != tagNameId) {
                    int32_t idx = binarySearch(tagNameId);
                    //not match group tag name, skip next tag name
                    if (idx < 0) {
                        PutVarint32Varint32(&seekKey, metric, startHour);
                        PutVarint32(&seekKey, tagNameId + 1);
                        iter_->Seek(seekKey);
                        seekKey.clear();
                        Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "skip metrics key is: %s=%s %d %d",
                             Slice(seekKey).ToString(true).data(),idx);
                        continue;
                    }
                    Slice groupBySlice = groupBys[idx];
                    curGroupByKey.clear();
                    while (groupBySlice.size() > 0) {
                        uint32_t tagValueOffset =0;
                        GetVarint32(&groupBySlice,&tagValueOffset);
                        curGroupByKey.push_back(tagValueOffset);
                    }
                    curTagNameId = tagNameId;
                }

                hasValue = true;
                if (currentHour == -1) {
                    currentHour = hour;
                } else if (currentHour != hour) {
                    dumpAllResult();
                    newGroup = true;
                }

                tagValue *tag_values = (tagValue *) key.data();
                std::string group = "";

                for (size_t i = 0; i < curGroupByKey.size(); i++) {
                    int32_t idx = curGroupByKey[i];
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log,
                        "metrics set===== group by scanner do next:  %d ",
                         idx);
                    group.append(1, tag_values[idx].value[0]);
                    group.append(1, tag_values[idx].value[1]);
                    group.append(1, tag_values[idx].value[2]);
                    group.append(1, tag_values[idx].value[3]);
                }
                if (saveGroup.length() == 0) {
                    saveGroup = group;
                }
                Slice g1 = saveGroup;
                Slice g2 = group;
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics group by scanner do next: %s=%s %d",
                    g1.ToString(true).data(), g2.ToString(true).data(),currentHour);
                if (memcmp(saveGroup.data(), group.data(), group.length()) != 0) {
                    Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics diff group by scanner do next");
                    dumpAllResult();

                    newGroup = true;
                }

                Slice value = iter_->value();
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

                if (newGroup) {
                    //set new save group
                    saveGroup = group;
                    break;
                }
            }
            if (hasValue && !newGroup) {
                //dump aggregator result
                dumpAllResult();
                Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "dump result only data");
            }
        }

        virtual int32_t getCurrentHour() override {
            return currentHour;
        }

        virtual Slice getGroupBy() override {
            return groupByKey;
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
            if (tagNamesId.size() != groupBys.size()) {
                return;
            }
            tagNamesIdLen = tagNamesId.size();
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
            groupByKey.append(saveGroup);

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
         *  find smallest value index in tag name ids, which >= tag name id
         *  if all values are < value, return tag name ids length
         */
        int32_t binarySearch(uint32_t tagNameId) {
            int32_t low = 0;
            int32_t high = tagNamesIdLen - 1;
            while (low <= high) {
                int32_t mid = (low + high) / 2;
                int midVal = tagNamesId[mid];
                if (midVal < tagNameId) {
                    low = mid + 1;
                } else if (midVal > tagNameId) {
                    high = mid - 1;
                } else {
                    return mid;//key found
                }
            }
            return -1;//key found found
        }
    };

    MetricsGroupByScanner *NewMetricsGroupByScannerImpl(DB *db, ReadOptions &read_options) {
        return new MetricsGroupByScannerImpl(db, read_options);
    }
}//namespace rocksdb
