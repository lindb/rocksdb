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

        uint32_t metric = 0;
        uint32_t startHour = 0;
        uint32_t endHour = 0;

        uint32_t pointCount = 60;

        virtual void next()  = 0;

        virtual bool hasNext() = 0;

        virtual int32_t getCurrentHour()  = 0;

        //for agg result
        virtual Slice getResultSet()  = 0;
    };

    // Return an empty iterator (yields nothing).
    extern MetricsScanner *NewMetricsScannerImpl(DB *db, ReadOptions &read_options);
}  // namespace rocksdb

#endif //ROCKSDB_METRICS_SCANNER_H
