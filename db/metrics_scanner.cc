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

namespace rocksdb {
    class MetricsScannerImpl : public MetricsScanner {
    private:
        DB *db_;
        Env *env_;
        ReadOptions read_options_;

        std::string seekKey = "";
        int metricLen = 0;

        int currentHour = 0;

        Iterator *iter_ = nullptr;
        bool hasValue = false;

        std::string resultSet_ = "";

        DataPoint *countPoints_ = nullptr;
        DataPoint *sumPoints_ = nullptr;
        DataPoint *minPoints_ = nullptr;
        DataPoint *maxPoints_ = nullptr;
    public:
        MetricsScannerImpl(DB *db, ReadOptions &read_options) {
            db_ = db;
            env_ = db_->GetEnv();
            read_options_ = read_options;
        }

        ~MetricsScannerImpl() {
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

        virtual bool hasNext() override {
            return hasValue;
        }

        virtual void next() override {
            hasValue = false;
            currentHour = -1;
            resultSet_.clear();
            if (pointCount <= 0) {
                return;
            }

            if (iter_ == nullptr) {
                init();
                if (seekKey.size() <= 0) {
                    return;
                }
                if (metricLen <= 0) {
                    return;
                }
                iter_ = db_->NewIterator(read_options_);
                iter_->Seek(seekKey);
            } else {
                iter_->Next();
            }

            bool finish = false;
            for (; iter_->Valid(); iter_->Next()) {
                Slice key = iter_->key();
                key.remove_prefix(metricLen);
                uint32_t hour = 0;
                GetVarint32(&key, &hour);
                if (hour > endHour) {
                    //finish iterator, because hour > end hour
                    break;
                }
                hasValue = true;

                if (currentHour == -1) {
                    currentHour = hour;
                } else if (currentHour != hour) {
                    dumpAllResult();
                    finish = true;
                }
                Slice value = iter_->value();

                while (value.size() > 0) {
                    //get point type
                    char point_type = value[0];
                    value.remove_prefix(1);
                    //get point value len
                    uint32_t len = 0;
                    GetVarint32(&value, &len);

                    //do agg
                    if (point_type == point_type_sum) {
                        agg(point_type, len, &value, sumPoints_);
                    } else if (point_type == point_type_count) {
                        agg(point_type, len, &value, countPoints_);
                    } else if (point_type == point_type_min) {
                        agg(point_type, len, &value, minPoints_);
                    } else if (point_type == point_type_max) {
                        agg(point_type, len, &value, maxPoints_);
                    }
                }
                //finish current hour
                if (finish) {
                    break;
                }
            }
            if (hasValue && !finish) {
                dumpAllResult();
            }
        }

        virtual int32_t getCurrentHour() override {
            return currentHour;
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

        void dumpAllResult(){
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
    };

    MetricsScanner *NewMetricsScannerImpl(DB *db, ReadOptions &read_options) {
        return new MetricsScannerImpl(db, read_options);
    }
}//namespace rocksdb
