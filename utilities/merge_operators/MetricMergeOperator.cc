//
// Created by jie.huang on 17/02/08.
//

#include <memory>
#include <utilities/tsdb/CounterMerger.h>
#include "utilities/tsdb/GaugeMerger.h"
#include "utilities/tsdb/RatioMerger.h"
#include <utilities/tsdb/TSDB.h>
#include <utilities/tsdb/ApdexMerger.h>
#include <utilities/tsdb/TimerMerger.h>
#include <utilities/tsdb/PayloadMerger.h>
#include <utilities/tsdb/HistogramMerger.h>

#include "MetricMergeOperator.h"

#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

    // Implementation for the merge operation (concatenates two strings)
    bool MetricMergeOperator::Merge(const Slice &key, const Slice *existing_value,
                                    const Slice &value, std::string *new_value, Logger *logger) const {

        // Clear the *new_value for writing.
        assert(new_value);
        new_value->clear();
        if (!existing_value) {
            // No existing_value. Set *new_value = value
            new_value->assign(value.data(), value.size());
        } else {
            char metricType = key[0];
            if (metricType == TSDB::METRIC_TYPE_COUNTER) {
                CounterMerger::merge(existing_value->data(),
                                     (uint32_t) existing_value->size(),
                                     value.data(),
                                     (uint32_t) value.size(),
                                     new_value);
            } else if (metricType == TSDB::METRIC_TYPE_GAUGE) {
                GaugeMerger::merge(existing_value->data(),
                                   (uint32_t) existing_value->size(),
                                   value.data(),
                                   (uint32_t) value.size(),
                                   new_value);
            } else if (metricType == TSDB::METRIC_TYPE_TIMER) {
                TimerMerger::merge(existing_value->data(),
                                     (uint32_t) existing_value->size(),
                                     value.data(),
                                     (uint32_t) value.size(),
                                     new_value);
            } else if (metricType == TSDB::METRIC_TYPE_RATIO) {
                RatioMerger::merge(existing_value->data(),
                                     (uint32_t) existing_value->size(),
                                     value.data(),
                                     (uint32_t) value.size(),
                                     new_value);
            } else if (metricType == TSDB::METRIC_TYPE_APDEX) {
                ApdexMerger::merge(existing_value->data(),
                                     (uint32_t) existing_value->size(),
                                     value.data(),
                                     (uint32_t) value.size(),
                                     new_value);
            }else if (metricType == TSDB::METRIC_TYPE_PAYLOAD) {
                PayloadMerger::merge(existing_value->data(),
                                   (uint32_t) existing_value->size(),
                                   value.data(),
                                   (uint32_t) value.size(),
                                   new_value);
            }else if (metricType == TSDB::METRIC_TYPE_PAYLOAD) {
                HistogramMerger::merge(existing_value->data(),
                                     (uint32_t) existing_value->size(),
                                     value.data(),
                                     (uint32_t) value.size(),
                                     new_value);
            }
        }
        return true;
    }

    const char *MetricMergeOperator::Name() const {
        return "MetricMergeOperator";
    }

    std::shared_ptr<MergeOperator> MergeOperators::CreateMetricMergeOperator() {
        return std::make_shared<MetricMergeOperator>();
    }

}