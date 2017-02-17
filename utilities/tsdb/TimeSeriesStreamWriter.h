//
// Created by jie.huang on 17/2/13.
//
#pragma once

#include "string"

namespace rocksdb {
    //This class operates on a stream writer of TimeSeries Data(timestamp, value)
    class TimeSeriesStreamWriter {
    public:
        TimeSeriesStreamWriter() {}

        TimeSeriesStreamWriter(std::string *data) {}

        virtual ~TimeSeriesStreamWriter() {}

        virtual void append(uint16_t timestamp, int64_t value) = 0;

        virtual void appendTimestamp(uint16_t timestamp) = 0;

        virtual void appendValue(int64_t value) = 0;

        virtual void flush() = 0;
    };

    extern TimeSeriesStreamWriter *NewTimeSeriesStreamWriter(std::string *data);
}
