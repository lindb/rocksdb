//
// Created by jie.huang on 17/2/13.
//
#pragma once
#include <stdint.h>

namespace rocksdb {
    //This class operates on a stream of TimeSeries Data(timestamp, value)
    class TimeSeriesStreamReader {
    public:
        TimeSeriesStreamReader() {}

        TimeSeriesStreamReader(const char *data, const uint32_t size) {}

        virtual ~TimeSeriesStreamReader() {}

        virtual int16_t getNextTimestamp() = 0;

        virtual int64_t getNextValue() = 0;
    };

    extern TimeSeriesStreamReader *NewTimeSeriesStreamReader(const char *data, const uint32_t size);
}
