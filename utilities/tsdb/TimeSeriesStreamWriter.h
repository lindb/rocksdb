//
// Created by jie.huang on 17/2/13.
//
#pragma once

#include "string"

namespace rocksdb {
    //This class operates on a stream writer of TimeSeries Data(timestamp, value)
    class TimeSeriesStreamWriter {
    private:
        std::string *data_;

        uint16_t prevTimestamp_ = 0;
        int32_t prevTimestampDelta_ = 0;

        int64_t previousValue_ = 0;
        uint32_t previousValueLeadingZeros_ = 0x7fffffff;
        uint32_t previousValueTrailingZeros_ = 0;

        uint32_t count_ = 0;
        uint8_t bitsAvailable_ = 8;
        char cur_ = 0;
    public:
        TimeSeriesStreamWriter() {}

        TimeSeriesStreamWriter(std::string *data) {}

        ~TimeSeriesStreamWriter() {}

        void append(uint16_t timestamp, int64_t value);

        void appendTimestamp(uint16_t timestamp);

        void appendValue(int64_t value);

        void flush();

        void addValueToBitString(int64_t value, uint32_t bitsInValue);

        void flipByte();
    };
}
