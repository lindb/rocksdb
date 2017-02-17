//
// Created by jie.huang on 17/2/14.
//

#pragma once

#include "string"

namespace rocksdb {
    class CounterMerger {
    public:
        static void merge(
                const char *existing_value,
                const uint32_t existing_size,
                const char *value,
                const uint32_t value_size,
                std::string *new_value);
    };
}