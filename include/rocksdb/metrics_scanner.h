//
// Created by jie.huang on 16/11/15.
//

#ifndef ROCKSDB_METRICS_SCANNER_H
#define ROCKSDB_METRICS_SCANNER_H

#include "string"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"

namespace rocksdb {
    class MetricsScanner {
    public:
        MetricsScanner() {}

        MetricsScanner(DB *db, ReadOptions &read_options) {}

        virtual ~MetricsScanner() {}

        char point_type_sum = 1;
        char point_type_count = 2;
        char point_type_min = 3;
        char point_type_max = 4;


        bool enableLog = false;
        bool enableProfiler = false;
        char metric_type = 0;
        uint32_t metric = 0;
        int32_t start = 0;
        int32_t end = 0;
        uint8_t minTagValueLen = 0;

        uint32_t pointCount = 60;

        virtual void setTagFilter(Slice &target) = 0;

        virtual void setGroupBy(Slice &target)  = 0;

        virtual void next()  = 0;

        virtual bool hasNextBaseTime(char nextBaseTime) = 0;

        virtual bool hasNext() = 0;

        virtual int32_t getCurrentBaseTime()  = 0;

        //for agg result
        virtual Slice getResultSet()  = 0;

        virtual Slice getGroupBy() = 0;

        virtual Slice getStat() = 0;
    };

    // Return an empty iterator (yields nothing).
    extern MetricsScanner *NewMetricsScannerImpl(DB *db, ReadOptions &read_options);
}  // namespace rocksdb

#endif //ROCKSDB_METRICS_SCANNER_H
