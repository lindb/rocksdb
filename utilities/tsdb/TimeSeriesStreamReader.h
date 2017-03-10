//
// Created by jie.huang on 17/2/13.
//
#pragma once

#include <stdint.h>

namespace rocksdb {
    //This class operates on a stream of TimeSeries Data(timestamp, value)
    class TimeSeriesStreamReader {
    private:
        const char *data_;
        uint32_t size_;
        int16_t previousTimestamp_ = -1;
        int64_t previousTimestampDelta_ = 0;
        int64_t previousValue_ = 0;
        uint64_t previousLeadingZeros_;
        uint64_t previousTrailingZeros_;

        uint32_t count_ = 0;
        uint8_t bitsLeft_ = 0;
        uint32_t pos_ = 0;
        char cur_;
    public:
        TimeSeriesStreamReader() {}

        TimeSeriesStreamReader(const char *data, const uint32_t size) {}

        ~TimeSeriesStreamReader() {}

        int16_t getNextTimestamp();

        int64_t getNextValue();

        void flipByte();

        uint64_t readValueFromBitString(uint32_t bitsToRead);

        uint32_t findTheFirstZeroBit(uint32_t limit);
    };
}

