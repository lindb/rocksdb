//
// Created by 黄杰 on 16/11/17.
//

#ifndef ROCKSDB_METRICS_GROUPBY_SCANNER_H
#define ROCKSDB_METRICS_GROUPBY_SCANNER_H
/*
namespace rocksdb {
    class MetricsGroupByScanner {
    public:
        MetricsGroupByScanner() {}

        MetricsGroupByScanner(DB *db, const ReadOptions &read_options) {}

        virtual ~MetricsGroupByScanner() {}

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

        virtual bool hasNext() = 0;

        virtual void next() = 0;

        //for agg result
        virtual Slice getCountResult()  = 0;

        virtual Slice getSumResult()  = 0;

        virtual Slice getMinResult()  = 0;

        virtual Slice getMaxResult()  = 0;
    };

    // Return an empty iterator (yields nothing).
    extern MetricsGroupByScanner *NewMetricsGroupByScannerImpl(DB *db, const ReadOptions &read_options);
}  // namespace rocksd
*/
#endif //ROCKSDB_METRICS_GROUPBY_SCANNER_H
