//
// Created by jie.huang on 17/2/13.
//
#include "TimeSeriesStreamReader.h"
#include "TSDB.h"

namespace rocksdb {

    int16_t TimeSeriesStreamReader::getNextTimestamp() {
        if (count_ <= 0) {
            return -1;
        }
        count_--;
        if (previousTimestamp_ == -1) {
            uint16_t firstTimestamp = (uint16_t) readValueFromBitString(TSDB::kBitsForFirstTimestamp);
            previousTimestamp_ = firstTimestamp;
            return firstTimestamp;
        } else {
            uint32_t type = findTheFirstZeroBit(4);
            if (type > 0) {
                // Delta of delta is non zero. Calculate the new delta.
                // `index' will be used to find the right length for the value that is read.
                int index = type - 1;
                int64_t decodedValue = readValueFromBitString(timestampEncodings[index].bitsForValue);

                // [0,255] becomes [-128,127]
                decodedValue -=
                        ((int64_t) 1 << (timestampEncodings[index].bitsForValue - 1));
                if (decodedValue >= 0) {
                    // [-128,127] becomes [-128,128] without the zero in the middle
                    decodedValue++;
                }

                previousTimestampDelta_ += decodedValue;
            }
            previousTimestamp_ += previousTimestampDelta_;
            return previousTimestamp_;
        }
    }

    int64_t TimeSeriesStreamReader::getNextValue() {
        if (previousTimestamp_ == -1) {
            return previousValue_;
        }
        //check if current value equals previous value
        uint64_t nonZeroValue = readBitFromBitString();

        if (!nonZeroValue) {
            //for zero, equals previous value
            return previousValue_;
        }

        //read value type(new leading/exist leading)
        uint64_t usePreviousBlockInformation = readBitFromBitString();

        uint64_t xorValue;
        if (usePreviousBlockInformation) {
            xorValue = readValueFromBitString((uint32_t) (64 - previousLeadingZeros_ - previousTrailingZeros_));
            xorValue <<= previousTrailingZeros_;
        } else {
            uint64_t leadingZeros =
                    readValueFromBitString(TSDB::kLeadingZerosLengthBits);
            uint64_t blockSize = readValueFromBitString(TSDB::kBlockSizeLengthBits) + TSDB::kBlockSizeAdjustment;
            previousTrailingZeros_ = 64 - blockSize - leadingZeros;
            xorValue = readValueFromBitString((uint32_t) blockSize);
            xorValue <<= previousTrailingZeros_;
            previousLeadingZeros_ = leadingZeros;
        }
        uint64_t value = xorValue ^previousValue_;
        previousValue_ = value;
        return value;
    }


    uint64_t TimeSeriesStreamReader::readValueFromBitString(uint32_t bitsToRead) {
        uint64_t value = 0;
        while (bitsToRead > 0 && pos_ < pos_size_) {
            value = ((value << 1) | ((data_[pos_ >> 3] >> (7 - (pos_ & 0x7))) & 1));
            pos_++;
            bitsToRead--;
        }
        while (bitsToRead > 0) {
            value <<= 1;
            bitsToRead--;
        }
        return value;
    }

    uint32_t TimeSeriesStreamReader::findTheFirstZeroBit(uint32_t limit) {
        uint32_t bits = 0;
        while (bits < limit) {
            uint32_t bit = readBitFromBitString();
            if (bit == 0) {
                return bits;
            }
            bits++;
        }
        return bits;
    }

    uint32_t TimeSeriesStreamReader::readBitFromBitString() {
        uint32_t bit = 0;
        if (pos_ < pos_size_) {
            bit = ((data_[pos_ >> 3] >> (7 - (pos_ & 0x7))) & 1);
            pos_++;
        }
        return bit;
    }
}
