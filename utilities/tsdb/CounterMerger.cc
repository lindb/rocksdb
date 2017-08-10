//
// Created by jie.huang on 17/2/14.
//

#include "CounterMerger.h"
#include "TimeSeriesStreamReader.h"
#include "TimeSeriesStreamWriter.h"

namespace rocksdb {
    void CounterMerger::merge(
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
                writer.append(old_slot, existStream.getNextValue() + newStream.getNextValue());
                //reset old/new slot for next loop
                old_slot = existStream.getNextTimestamp();
                new_slot = newStream.getNextTimestamp();
            } else if (old_slot != -1 && (new_slot == -1 || old_slot < new_slot)) {
                writer.append(old_slot, existStream.getNextValue());
                //reset old slot for next loop
                old_slot = existStream.getNextTimestamp();
            } else if (new_slot != -1 && (old_slot == -1 || new_slot < old_slot)) {
                writer.append(new_slot, newStream.getNextValue());
                //reset new slot for next loop
                new_slot = newStream.getNextTimestamp();
            }
        }
        writer.flush();
    }
}