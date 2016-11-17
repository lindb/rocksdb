#pragma once

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {
    class PointMergeOperatorV2 : public AssociativeMergeOperator {
    public:
        virtual bool Merge(const Slice &key,
                           const Slice *existing_value,
                           const Slice &value,
                           std::string *new_value,
                           Logger *logger) const override;

        virtual const char *Name() const override;
    };
} // namespace rocksdb