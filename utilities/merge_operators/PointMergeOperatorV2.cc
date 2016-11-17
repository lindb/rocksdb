//
// Created by jie.huang on 16/11/15.
//

#include <memory>

#include "PointMergeOperatorV2.h"

#include "rocksdb/env.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// Implementation for the merge operation (concatenates two strings)
    bool PointMergeOperatorV2::Merge(const Slice &key, const Slice *existing_value,
                                     const Slice &value, std::string *new_value, Logger *logger) const {

        // Clear the *new_value for writing.
        assert(new_value);
        new_value->clear();
        if (!existing_value) {
            // No existing_value. Set *new_value = value
            new_value->assign(value.data(), value.size());
        } else {
            //copy slice for existing value and current value
            Slice old_slice = Slice(existing_value->data(), existing_value->size());
            Slice cur_slice = Slice(value.data(), value.size());

            char old_op = -1;
            char new_op = -1;
            uint32_t old_point_len = 0, new_point_len = 0;

            while (old_slice.size() > 0 || cur_slice.size() > 0 ||
                   old_op != -1 || new_op != -1) {
                if (old_op == -1 && old_slice.size() > 0) {
                    old_op = old_slice[0];
                    old_slice.remove_prefix(1);
                    GetVarint32(&old_slice, &old_point_len);
                }
                if (new_op == -1 && cur_slice.size() > 0) {
                    new_op = cur_slice[0];
                    cur_slice.remove_prefix(1);
                    GetVarint32(&cur_slice, &new_point_len);
                }
                if (old_op == -1 && new_op == -1) {
                    break;
                }
                if (old_point_len == 0 && new_point_len == 0) {
                    continue;
                }

                if (old_op == new_op) {
                    std::string points = "";
                    size_t old_point_pos = old_slice.size(), new_point_pos = cur_slice.size();
                    int32_t old_slot = -1, new_slot = -1;
                    int64_t old_val = 0, new_val = 0;

                    while (old_point_pos - old_slice.size() < old_point_len ||
                           new_point_pos - cur_slice.size() < new_point_len ||
                           old_slot == -1 || new_slot == -1) {
                        if (old_slot == -1 && old_point_pos - old_slice.size() < old_point_len) {
                            GetVarint32(&old_slice, (uint32_t *) &old_slot);
                            GetVarint64(&old_slice, (uint64_t *) &old_val);
                        }
                        if (new_slot == -1 && new_point_pos - cur_slice.size() < new_point_len) {
                            GetVarint32(&cur_slice, (uint32_t *) &new_slot);
                            GetVarint64(&cur_slice, (uint64_t *) &new_val);
                        }
                        if (old_slot == -1 && new_slot == -1) {
                            break;
                        }
                        if (old_slot == new_slot) {
                            if (old_op == 1 || old_op == 2) {
                                //put count/sum value into merge value
                                PutVarint32Varint64(&points, old_slot, old_val + new_val);
                            } else if (old_op == 3) {
                                //put min value into merge value
                                PutVarint32Varint64(&points, old_slot, old_val > new_val ? new_val : old_val);
                            } else if (old_op == 4) {
                                //put max value into merge value
                                PutVarint32Varint64(&points, old_slot, old_val > new_val ? old_val : new_val);
                            }
                            //reset old/new slot for next loop
                            old_slot = -1, new_slot = -1;
                        } else if (old_slot != -1 && (new_slot == -1 || old_slot < new_slot)) {
                            PutVarint32Varint64(&points, old_slot, old_val);
                            //reset old slot for next loop
                            old_slot = -1;
                        } else if (new_slot != -1 && (old_slot == -1 || new_slot < old_slot)) {
                            PutVarint32Varint64(&points, new_slot, new_val);
                            //reset new slot for next loop
                            new_slot = -1;
                        }
                    }
                    if (points.size() > 0) {
                        //put op type into new value
                        new_value->append(1, old_op);
                        //put merged value size into new_value
                        PutVarint64(new_value, points.size());
                        //put merged value into new_value
                        new_value->append(points);
                    }

                    old_op = -1, new_op = -1;
                } else if (old_op != -1 && (new_op == -1 || old_op < new_op)) {
                    //put old op type into new value
                    new_value->append(1, old_op);
                    PutVarint32(new_value, old_point_len);
                    new_value->append(old_slice.data(), old_point_len);
                    old_slice.remove_prefix(old_point_len);

                    old_op = -1;
                } else if (new_op != -1 && (old_op == -1 || new_op < old_op)) {
                    //put new op type into new value
                    new_value->append(1, new_op);
                    PutVarint32(new_value, new_point_len);
                    new_value->append(cur_slice.data(), new_point_len);
                    cur_slice.remove_prefix(new_point_len);

                    new_op = -1;
                }
            }
        }
        return true;
    }

    const char *PointMergeOperatorV2::Name() const {
        return "PointMergeOperatorV2";
    }

    std::shared_ptr<MergeOperator> MergeOperators::CreatePointMergeOperatorV2() {
        return std::make_shared<PointMergeOperatorV2>();
    }
}