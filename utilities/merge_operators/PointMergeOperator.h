//
// Created by 黄杰 on 16/11/17.
//

#pragma once

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
namespace rocksdb {


    class PointMergeOperator : public AssociativeMergeOperator {
    public:
        virtual bool Merge(const Slice& key,
                           const Slice* existing_value,
                           const Slice& value,
                           std::string* new_value,
                           Logger* logger) const override;

        virtual const char* Name() const override;
    };

} // namespace rocksdb
