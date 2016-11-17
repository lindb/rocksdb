//
// Created by jie.huang on 16/11/17.
//

#include <memory>
#include <assert.h>

#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"
#include "PointMergeOperator.h"

typedef struct {
    char minute;
    char op;
    char value[8];
} point;

namespace rocksdb {

    static void appendPoint(std::string* new_value,point* point){
        uint64_t val = DecodeFixed64(point->value);
        new_value->append(&point->minute,sizeof(point->minute));
        new_value->append(&point->op,sizeof(point->op));
        PutFixed64(new_value, val);
    }

// Implementation for the merge operation (concatenates two strings)
    bool PointMergeOperator::Merge(const Slice& key, const Slice* existing_value,
                                   const Slice& value, std::string* new_value, Logger* logger) const {

        // Clear the *new_value for writing.
        assert(new_value);
        new_value->clear();
        if (!existing_value) {
            // No existing_value. Set *new_value = value
            new_value->assign(value.data(), value.size());
        } else {
            point *old_points, *new_points;
            old_points = (point*) existing_value->data();
            new_points = (point*) value.data();
            uint16_t old_points_count = existing_value->size()/10;
            uint16_t new_points_count = value.size()/10;

            uint16_t old_point_pos = 0;
            uint16_t new_point_pos = 0;

            point* old_point;
            point* new_point;

            while(old_point_pos<old_points_count || new_point_pos<new_points_count){
                if(old_point_pos<old_points_count){
                    old_point = &old_points[old_point_pos];
                }else{
                    old_point = NULL;
                }

                if(new_point_pos<new_points_count){
                    new_point = &new_points[new_point_pos];
                }else{
                    new_point = NULL;
                }

                if(old_point && new_point){
                    if(old_point->minute==new_point->minute && old_point->op==new_point->op){
                        int64_t old_val = DecodeFixed64(old_point->value);
                        int64_t new_val = DecodeFixed64(new_point->value);
                        new_value->append(&old_point->minute, sizeof(old_point->minute));
                        new_value->append(&old_point->op, sizeof(old_point->op));
                        if (old_point->op== 1 || old_point->op == 2) {
                            PutFixed64(new_value, old_val + new_val);
                        } else if (old_point->op== 3) {
                            PutFixed64(new_value, old_val > new_val ? new_val : old_val);
                        } else if (old_point->op== 4) {
                            PutFixed64(new_value, old_val > new_val ? old_val : new_val);
                        }
                        old_point_pos++;
                        new_point_pos++;
                    }else if(old_point->minute==new_point->minute && old_point->op<new_point->op){
                        appendPoint(new_value,old_point);
                        old_point_pos++;
                    }else if(old_point->minute==new_point->minute && old_point->op>new_point->op){
                        appendPoint(new_value,new_point);
                        new_point_pos++;
                    }else if(old_point->minute<new_point->minute){
                        appendPoint(new_value,old_point);
                        old_point_pos++;
                    }else if(old_point->minute>new_point->minute){
                        appendPoint(new_value,new_point);
                        new_point_pos++;
                    }
                }else if(old_point){
                    appendPoint(new_value,old_point);
                    old_point_pos++;
                }else if(new_point){
                    appendPoint(new_value,new_point);
                    new_point_pos++;
                }
            }
        }

        return true;
    }

    const char* PointMergeOperator::Name() const {
        return "PointMergeOperator";
    }
    std::shared_ptr<MergeOperator> MergeOperators::CreatePointMergeOperator() {
        return std::make_shared<PointMergeOperator>();
    }

}
