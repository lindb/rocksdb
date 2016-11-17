//
// Created by jie.huang on 16/11/15.
//

#include <iostream>
#include <map>
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "util/coding.h"

namespace rocksdb {
    class MetricsScannerImpl : public MetricsScanner {
    private:
        DB *db_;
        Env *env_;
        ReadOptions read_options_;

        std::string count_ = "";
        std::map<int32_t, int64_t> countAgg_;
        std::string sum_ = "";
        std::map<int32_t, int64_t> sumAgg_;
        std::string min_ = "";
        std::map<int32_t, int64_t> minAgg_;
        std::string max_ = "";
        std::map<int32_t, int64_t> maxAgg_;
    public:
        MetricsScannerImpl(DB *db,  ReadOptions &read_options) {
            db_ = db;
            env_ = db_->GetEnv();
            read_options_ = read_options;
        }

        virtual void doScan() override {
            DBOptions options = db_->GetDBOptions();
            if (metricsId == -1 || time == -1) {
                //invalid metrics and time
                return;
            }
            if (!countEnable && !sumEnable && !maxEnable && !minEnable) {
                //no aggregator function
                return;
            }
            if (startSlot == -1 || endSlot == -1) {
                //invalid slot range
                return;
            }
            Iterator *iter = db_->NewIterator(read_options_);
            std::string seekKey = "";
            PutVarint32(&seekKey, metricsId);
            PutVarint32(&seekKey, time);
            Slice seek = Slice(seekKey);
            for (iter->Seek(seek); iter->Valid(); iter->Next()) {
                Slice key = iter->key();
                //compare seek key and current key
                if (memcmp(key.data_, seek.data_, seek.size_) != 0) {
                    //finish iterator, because seek!=current key
                    break;
                }
                Slice value = iter->value();

                while (value.size() > 0) {
                    //get point type
                    char point_type = value[0];
                    value.remove_prefix(1);
                    //get point value len
                    uint32_t len = 0;
                    GetVarint32(&value, &len);

                    if ((!sumEnable && point_type == point_type_sum) &&
                        (!countEnable && point_type == point_type_count) &&
                        (!minEnable && point_type == point_type_min) &&
                        (!maxEnable && point_type == point_type_max)) {
//                        Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics scanner loop remove value: %d %d",
//                            point_type, len);
                        //skip len value for not accept point type
                        value.remove_prefix(len);
                    } else if (point_type == point_type_sum) {
//                        Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics scanner loop: %d %d", point_type,
//                            len);
                        //do agg
                        agg(point_type, len, &value, &sumAgg_);
                    } else if (point_type == point_type_count) {
                        agg(point_type, len, &value, &countAgg_);
                    } else if (point_type == point_type_min) {
                        agg(point_type, len, &value, &minAgg_);
                    } else if (point_type == point_type_max) {
                        agg(point_type, len, &value, &maxAgg_);
                    }
                }
            }
            Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics finish scanner");
            delete iter;
            Log(InfoLogLevel::ERROR_LEVEL, options.info_log, "metrics finish clear scanner");
        }

        virtual Slice getCountResult() override {
            getResult(&count_, &countAgg_);
            return count_;
        }

        virtual Slice getSumResult() override {
            getResult(&sum_, &sumAgg_);
            return sum_;
        }

        virtual Slice getMinResult() override {
            getResult(&min_, &minAgg_);
            return min_;
        }

        virtual Slice getMaxResult() override {
            getResult(&max_, &maxAgg_);
            return max_;
        }

    private:
        void getResult(std::string *result, std::map<int32_t, int64_t> *agg) {
            for (std::map<int32_t, int64_t>::iterator it = agg->begin(); it != agg->end(); ++it) {
                PutVarint32Varint64(result, it->first, it->second);
            }
        }

        void agg(char point_type, size_t len, Slice *value, std::map<int32_t, int64_t> *agg) {
            size_t size = value->size();
            while (size - value->size() < len) {
                int32_t slot;
                GetVarint32(value, (uint32_t *) &slot);
                int64_t val;
                GetVarint64(value, (uint64_t *) &val);

                if (startSlot > slot || slot > endSlot) {
                    continue;
                }
                auto it = agg->find(slot);
                if (it == agg->end()) {
                    agg->insert(std::pair<int32_t, int64_t>(slot, val));
                } else {
                    if (point_type == point_type_sum || point_type == point_type_count) {
                        it->second += val;
                    } else if (point_type == point_type_min) {
                        it->second = it->second > val ? val : it->second;
                    } else if (point_type == point_type_max) {
                        it->second = it->second < val ? val : it->second;
                    }
                }
            }
        }
    };

    MetricsScanner *NewMetricsScannerImpl(DB *db, ReadOptions &read_options) {
        return new MetricsScannerImpl(db, read_options);
    }
}//namespace rocksdb
