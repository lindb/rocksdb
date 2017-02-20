//
// Created by jie.huang on 17/2/14.
//

#pragma once

#include "ApdexMerger.h"
#include "TimeSeriesStreamReader.h"
#include "TimeSeriesStreamWriter.h"

namespace rocksdb {
    void ApdexMerger::merge(
            const char *existing_value,
            const uint32_t existing_size,
            const char *value,
            const uint32_t value_size,
            std::string *new_value) {
        TimeSeriesStreamReader *existStream = NewTimeSeriesStreamReader(existing_value, existing_size);
        TimeSeriesStreamReader *newStream = NewTimeSeriesStreamReader(value, value_size);
        TimeSeriesStreamWriter *writer = NewTimeSeriesStreamWriter(new_value);

        int32_t old_slot = existStream->getNextTimestamp(), new_slot = newStream->getNextTimestamp();

        while (old_slot != -1 || new_slot != -1) {
            if (old_slot == new_slot && old_slot != -1) {
                //write for timestamp
                writer->appendTimestamp(old_slot);
                //write for tolerating
                writer->appendValue(existStream->getNextValue() + newStream->getNextValue());
                //write for satisfied
                writer->appendValue(existStream->getNextValue() + newStream->getNextValue());
                //write for total 
                writer->appendValue(existStream->getNextValue() + newStream->getNextValue());
                //reset old/new slot for next loop
                old_slot = existStream->getNextTimestamp();
                new_slot = newStream->getNextTimestamp();
            } else if (old_slot != -1 && (new_slot == -1 || old_slot < new_slot)) {
                writer->append(old_slot, existStream->getNextValue());
                writer->appendValue(existStream->getNextValue());
                writer->appendValue(existStream->getNextValue());
                //reset old slot for next loop
                old_slot = existStream->getNextTimestamp();
            } else if (new_slot != -1 && (old_slot == -1 || new_slot < old_slot)) {
                writer->append(new_slot, newStream->getNextValue());
                writer->appendValue(newStream->getNextValue());
                writer->appendValue(newStream->getNextValue());
                //reset new slot for next loop
                new_slot = newStream->getNextTimestamp();
            }
        }
        writer->flush();
    }
}