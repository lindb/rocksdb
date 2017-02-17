
#include "TSDB.h"

namespace rocksdb {
    const char TSDB::METRIC_TYPE_COUNTER = 1;
    const char TSDB::METRIC_TYPE_GAUGE = 2;
    const char TSDB::METRIC_TYPE_TIMER = 3;
    const char TSDB::METRIC_TYPE_PERCENT = 4;
}