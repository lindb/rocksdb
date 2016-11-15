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
            Log(InfoLogLevel::ERROR_LEVEL, logger, "||||||=======no existing value %d", value.size());
        } else {
            //copy slice for existing value and current value
            Slice old_slice = Slice(existing_value->data(), existing_value->size());
            Slice cur_slice = Slice(value.data(), value.size());

            Log(InfoLogLevel::ERROR_LEVEL, logger, "merge size %d %d", existing_value->size(), value.size());
            char old_op = -1;
            char new_op = -1;

            while (old_slice.size() > 0 || cur_slice.size() > 0 || (old_op == -1 && new_op == -1)) {
                old_op = -1;
                new_op = -1;
                if (old_slice.size() > 0) {
                    old_op = old_slice[0];
                }
                if (cur_slice.size() > 0) {
                    new_op = cur_slice[0];
                }

                Log(InfoLogLevel::ERROR_LEVEL, logger,
                    "loop1 %d %d %d %d", old_slice.size(), cur_slice.size(), old_op, new_op);
                if (old_op != -1 && new_op != -1 && old_op == new_op) {
                    //put op type into new value
                    new_value->append(1, old_op);

                    //remove op from old/current slice
                    old_slice.remove_prefix(1);
                    cur_slice.remove_prefix(1);

                    //read point length for points
                    uint32_t old_point_len = 0, new_point_len = 0;
                    GetVarint32(&old_slice, &old_point_len);
                    GetVarint32(&cur_slice, &new_point_len);

                    std::string points = "";

                    size_t old_point_pos = old_slice.size(), new_point_pos = cur_slice.size();
                    int32_t old_slot = -1, new_slot = -1;
                    int64_t old_val, new_val;

                    if (old_point_len > 0) {
                        GetVarint32(&old_slice, (uint32_t *) &old_slot);
                        GetVarint64(&old_slice, (uint64_t *) &old_val);
                    }
                    if (new_point_len > 0) {
                        GetVarint32(&cur_slice, (uint32_t *) &new_slot);
                        GetVarint64(&cur_slice, (uint64_t *) &new_val);
                    }

                    Log(InfoLogLevel::ERROR_LEVEL, logger,
                        "loop1 op new point len == %d %d %d %d %d %d", old_point_len, new_point_len, old_slot, new_slot,
                        old_val, new_val);

                    while (old_point_pos - old_slice.size() < old_point_len ||
                           new_point_pos - cur_slice.size() < new_point_len || old_slot > -1 || new_slot > -1) {
//                        Log(InfoLogLevel::ERROR_LEVEL, logger,
//                            "loop 2 old op==new op %d %d %d %d %d %d %d", old_op, old_slot, new_slot, old_val, new_val,
//                            old_slice.size(),
//                            cur_slice.size());
                        if (old_slot != -1 && new_slot != -1 && old_slot == new_slot) {
//                            Log(InfoLogLevel::ERROR_LEVEL, logger,
//                                "loop 2  old op==new op slot== %d %d %d %d %d %d", old_slot, new_slot, old_val, new_val,
//                                old_slice.size(),
//                                cur_slice.size());
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
                            if (old_point_pos - old_slice.size() < old_point_len) {
                                GetVarint32(&old_slice, (uint32_t *) &old_slot);
                                GetVarint64(&old_slice, (uint64_t *) &old_val);
                            }
                            if (new_point_pos - cur_slice.size() < new_point_len) {
                                GetVarint32(&cur_slice, (uint32_t *) &new_slot);
                                GetVarint64(&cur_slice, (uint64_t *) &new_val);
                            }
                            Log(InfoLogLevel::ERROR_LEVEL, logger,
                                "loop 2 slot== op==new after== %d %d %d %d %d %d", old_slot, new_slot, old_val, new_val,
                                cur_slice.size(), cur_slice.size());
                        } else if (old_slot != -1 && (new_slot == -1 || old_slot < new_slot)) {
                            PutVarint32Varint64(&points, old_slot, old_val);
                            Log(InfoLogLevel::ERROR_LEVEL, logger,
                                "loop 2 add only old op== slot after== %d %d %d", old_slot, old_val, points.size());

                            //reset old slot for next loop
                            old_slot = -1;
                            if (old_point_pos - old_slice.size() < old_point_len) {
                                GetVarint32(&old_slice, (uint32_t *) &old_slot);
                                GetVarint64(&old_slice, (uint64_t *) &old_val);
                            }
                        } else if (new_slot > -1 && (old_slot < 0 || new_slot < old_slot)) {
                            PutVarint32Varint64(&points, new_slot, new_val);

                            Log(InfoLogLevel::ERROR_LEVEL, logger,
                                "loop 2 add only new slot op=== after== %d %d %d", new_slot, new_val, points.size());
                            //reset new slot for next loop
                            new_slot = -1;
                            if (new_point_pos - cur_slice.size() < new_point_len) {
                                GetVarint32(&cur_slice, (uint32_t *) &new_slot);
                                GetVarint64(&cur_slice, (uint64_t *) &new_val);
                            }
                        }
                    }
//                    Log(InfoLogLevel::ERROR_LEVEL, logger,
//                        "loop 2 after new value size == %d %d", points.size(), new_value->size());
                    //put merged value size into new_value
                    PutVarint64(new_value, points.size());
                    Log(InfoLogLevel::ERROR_LEVEL, logger,
                        "loop 2 after point size op==== new value size == %d %d", points.size(), new_value->size());
                    //put merged value into new_value
                    new_value->append(points);
                } else if (old_op > 0 && (new_op <= 0 || old_op < new_op)) {
                    //put old op type into new value
                    new_value->append(1, old_op);

                    old_slice.remove_prefix(1);

//                    Log(InfoLogLevel::ERROR_LEVEL, logger,
//                        "loop 1 only old op== %d %d", old_slice.size(), new_value->size());

                    uint32_t point_len = 0;
                    GetVarint32(&old_slice, &point_len);
                    PutVarint32(new_value, point_len);
                    new_value->append(old_slice.data(), point_len);
                    old_slice.remove_prefix(point_len);

//                    Log(InfoLogLevel::ERROR_LEVEL, logger,
//                        "loop 1 only old op== %d %d", old_slice.size(), new_value->size());
                } else if (new_op > -1 && (old_op <=0 || new_op < old_op)) {
                    //put new op type into new value
                    new_value->append(1, new_op);
                    cur_slice.remove_prefix(1);
//                    Log(InfoLogLevel::ERROR_LEVEL, logger,
//                        "loop 1 only new op== %d %d", cur_slice.size(), new_value->size());

                    uint32_t point_len = 0;
                    GetVarint32(&cur_slice, &point_len);
                    PutVarint32(new_value, point_len);
                    new_value->append(cur_slice.data(), point_len);
                    cur_slice.remove_prefix(point_len);
//                    Log(InfoLogLevel::ERROR_LEVEL, logger,
//                        "loop 1 only new op== %d %d", cur_slice.size(), new_value->size());
                }
            }
        }
        Log(InfoLogLevel::ERROR_LEVEL, logger,
            "finish merge value = %d", new_value->size());
        return true;
    }

    const char *PointMergeOperatorV2::Name() const {
        return "PointMergeOperatorV2";
    }

    std::shared_ptr<MergeOperator> MergeOperators::CreatePointMergeOperatorV2() {
        return std::make_shared<PointMergeOperatorV2>();
    }
}

