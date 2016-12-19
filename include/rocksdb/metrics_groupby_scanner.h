//
// Created by 黄杰 on 16/11/17.
//

#ifndef ROCKSDB_METRICS_GROUPBY_SCANNER_H
#define ROCKSDB_METRICS_GROUPBY_SCANNER_H

#include <vector>
#include "string"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"

namespace rocksdb {
    class MetricsGroupByScanner {
    public:
        MetricsGroupByScanner() {}

        MetricsGroupByScanner(DB *db, const ReadOptions &read_options) {}

        virtual ~MetricsGroupByScanner() {}

        char point_type_sum = 1;
        char point_type_count = 2;
        char point_type_min = 3;
        char point_type_max = 4;

        uint32_t metric = 0;
        uint32_t startHour = 0;
        uint32_t endHour = 0;

        uint32_t pointCount = 60;

        virtual void addGroupBy(uint32_t, Slice &target) = 0;

        virtual bool hasNext() = 0;

        virtual void next() = 0;

        virtual int32_t getCurrentHour() = 0;

        virtual Slice getGroupBy() = 0;

        //for agg result
        virtual Slice getResultSet()  = 0;
    };

    // Return an empty iterator (yields nothing).
    extern MetricsGroupByScanner *NewMetricsGroupByScannerImpl(DB *db, ReadOptions &read_options);
}  // namespace rocksdb
#endif //ROCKSDB_METRICS_GROUPBY_SCANNER_H
