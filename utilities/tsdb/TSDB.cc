
#include "TSDB.h"

namespace rocksdb {
    const char TSDB::METRIC_TYPE_COUNTER = 1;
    const char TSDB::METRIC_TYPE_GAUGE = 2;
    const char TSDB::METRIC_TYPE_TIMER = 3;
    const char TSDB::METRIC_TYPE_RATIO = 4;
    const char TSDB::METRIC_TYPE_APDEX = 5;
    const char TSDB::METRIC_TYPE_PAYLOAD = 6;
    const char TSDB::METRIC_TYPE_HISTOGRAM = 7;
}