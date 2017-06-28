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
        uint32_t pos_ = 0;
    public:
        TimeSeriesStreamReader(const char *data, const uint32_t size) {
            data_ = data;
            size_ = size;
            if (size_ >= 4) {
                count_ = ((static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 1])))
                          | (static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 2])) << 8)
                          | (static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 3])) << 16)
                          | (static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 4])) << 24));
            }
        }

        ~TimeSeriesStreamReader() {}

        int16_t getNextTimestamp();

        int64_t getNextValue();

        void flipByte();

        uint64_t readValueFromBitString(uint32_t bitsToRead);

        uint32_t findTheFirstZeroBit(uint32_t limit);

        uint64_t readValueFromBit();
    };
}

