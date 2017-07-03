//
// Created by yufu.deng on 17/6/24.
//
#pragma once

#include "string"

namespace rocksdb {
    class Aggregator {
    public:
        Aggregator() {}

        virtual ~Aggregator() {}

        virtual std::string dumpResult()  = "";

        virtual void merge(const char *value, const uint32_t value_size);
    };

}  // namespace rocksdb
