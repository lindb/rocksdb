//
// Created by jie.huang on 16/11/9.
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

        MetricsScanner(DB *db, const ReadOptions &read_options) {}

        virtual ~MetricsScanner() {}

        const Slice validation_error = "1";
        char point_type_sum = 1;
        char point_type_count = 2;
        char point_type_min = 3;
        char point_type_max = 4;
        int32_t metricsId = -1;
        int32_t time = -1;
        bool countEnable = false;
        bool sumEnable = false;
        bool minEnable = false;
        bool maxEnable = false;
        int32_t startSlot = -1;
        int32_t endSlot = -1;

        virtual void doScan()  = 0;

        //for agg result
        virtual Slice getCountResult()  = 0;

        virtual Slice getSumResult()  = 0;

        virtual Slice getMinResult()  = 0;

        virtual Slice getMaxResult()  = 0;
    };

    // Return an empty iterator (yields nothing).
    extern MetricsScanner *NewMetricsScannerImpl(DB *db, const ReadOptions &read_options);
}  // namespace rocksdb

#endif //ROCKSDB_METRICS_SCANNER_H
