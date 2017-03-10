//
// Created by yufu.deng on 17/3/3.
//

#pragma once

#include "string"

namespace rocksdb {
    class HistogramMerger {
    public:
        static void merge(
                const char *existing_value,
                const uint32_t existing_size,
                const char *value,
                const uint32_t value_size,
                std::string *new_value);
    };
}
