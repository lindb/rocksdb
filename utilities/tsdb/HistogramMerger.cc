//
// Created by yufu.deng on 17/3/3.
//

#pragma once

#include "HistogramMerger.h"
#include "TimeSeriesStreamReader.h"
#include "TimeSeriesStreamWriter.h"

namespace rocksdb {
    void HistogramMerger::merge(
            const char *existing_value,
            const uint32_t existing_size,
            const char *value,
            const uint32_t value_size,
            std::string *new_value) {
        TimeSeriesStreamReader existStream(existing_value, existing_size);
        TimeSeriesStreamReader newStream(value, value_size);
        TimeSeriesStreamWriter writer(new_value);

        int32_t old_slot = existStream.getNextTimestamp(), new_slot = newStream.getNextTimestamp();

        while (old_slot != -1 || new_slot != -1) {
            if (old_slot == new_slot && old_slot != -1) {
                //put count/sum value into merge value
                writer.appendTimestamp(old_slot);//slot
                int64_t old_type = existStream.getNextValue(), old_base_number = existStream.getNextValue(), old_max_slot = existStream.getNextValue();
                int64_t new_type = newStream.getNextValue(), new_base_number = newStream.getNextValue(), new_max_slot = newStream.getNextValue();
                writer.appendValue(old_type);//type
                writer.appendValue(old_base_number);//base number
                int64_t max_slot = old_max_slot > new_max_slot ? old_max_slot : new_max_slot;
                if (old_type != new_type || old_base_number != new_base_number) {
                    writer.appendValue(old_max_slot);//max value slot
                    writer.appendValue(existStream.getNextValue());//min
                    writer.appendValue(existStream.getNextValue());//max
                    writer.appendValue(existStream.getNextValue());//sum
                    newStream.getNextValue(); //min
                    newStream.getNextValue(); //max
                    newStream.getNextValue(); //sum
                    for (int i = 0; i < max_slot; i++) {//values
                        if (i < old_max_slot) {
                            writer.appendValue(existStream.getNextValue());
                        }
                        if (i < new_max_slot) {
                            newStream.getNextValue();
                        }
                    }
                } else {
                    writer.appendValue(max_slot);//max value slot
                    int64_t old_min = existStream.getNextValue();
                    int64_t old_max = existStream.getNextValue();
                    int64_t new_min = newStream.getNextValue();
                    int64_t new_max = newStream.getNextValue();
                    writer.appendValue(old_min < new_min ? old_min : new_min);//min
                    writer.appendValue(old_max > new_max ? old_max : new_max);//max
                    writer.appendValue(existStream.getNextValue() + newStream.getNextValue());//sum
                    for (int i = 0; i < max_slot; i++) {//values
                        if (i < old_max_slot && i < new_max_slot) {
                            writer.appendValue(existStream.getNextValue() + newStream.getNextValue());
                        } else if (i < old_max_slot) {
                            writer.appendValue(existStream.getNextValue());
                        } else {
                            writer.appendValue(newStream.getNextValue());
                        }
                    }
                }
                //reset old/new slot for next loop
                old_slot = existStream.getNextTimestamp();
                new_slot = newStream.getNextTimestamp();
            } else if (old_slot != -1 && (new_slot == -1 || old_slot < new_slot)) {
                writer.appendTimestamp(old_slot);
                writer.appendValue(existStream.getNextValue());//type
                writer.appendValue(existStream.getNextValue());//baseNumber
                int64_t max_slot = existStream.getNextValue();
                writer.appendValue(max_slot);//max value slot
                writer.appendValue(existStream.getNextValue());//min
                writer.appendValue(existStream.getNextValue());//max
                writer.appendValue(existStream.getNextValue());//sum
                for (int i = 0; i < max_slot; ++i) {// values
                    writer.appendValue(existStream.getNextValue());
                }
                //reset old slot for next loop
                old_slot = existStream.getNextTimestamp();
            } else if (new_slot != -1 && (old_slot == -1 || new_slot < old_slot)) {
                writer.appendTimestamp(new_slot);
                writer.appendValue(newStream.getNextValue());//type
                writer.appendValue(newStream.getNextValue());//baseNumber
                int64_t max_slot = newStream.getNextValue();
                writer.appendValue(max_slot);//max value slot
                writer.appendValue(newStream.getNextValue());//min
                writer.appendValue(newStream.getNextValue());//max
                writer.appendValue(newStream.getNextValue());//sum
                for (int i = 0; i < max_slot; ++i) {
                    writer.appendValue(newStream.getNextValue());// value
                }
                //reset new slot for next loop
                new_slot = newStream.getNextTimestamp();
            }
        }
        writer.flush();
    }
}