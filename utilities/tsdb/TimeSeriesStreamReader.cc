//
// Created by jie.huang on 17/2/13.
//
#include "TimeSeriesStreamReader.h"
#include "TSDB.h"

namespace rocksdb {

    class TimeSeriesStreamReaderImpl : public TimeSeriesStreamReader {
    private:
        const char *data_;
        uint32_t size_;
        uint64_t bitPos_;
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
        TimeSeriesStreamReaderImpl(const char *data, const uint32_t size) {
            data_ = data;
            size_ = size;
            if (size_ >= 4) {
                count_ = ((static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 1])))
                          | (static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 2])) << 8)
                          | (static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 3])) << 16)
                          | (static_cast<uint32_t>(static_cast<unsigned char>(data_[size_ - 4])) << 24));
            }
            if (count_ > 0) {
                flipByte();
            }
        }

        ~TimeSeriesStreamReaderImpl() {

        }

        int16_t getNextTimestamp() override {
            if (count_ == 0) {
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

        int64_t getNextValue() override {
            if (previousTimestamp_ == -1) {
                return previousValue_;
            }
            //check if current value equals previous value
            uint64_t nonZeroValue = readValueFromBitString(1);
            if (!nonZeroValue) {
                //for zero, equals previous value
                return previousValue_;
            }

            //read value type(new leading/exist leading)
            uint64_t usePreviousBlockInformation = readValueFromBitString(1);

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

    private:
        uint64_t readValueFromBitString(uint32_t bitsToRead) {
            uint64_t value = 0;
            for (int i = 0; i < bitsToRead; i++) {
                value <<= 1;
                char bit = (char) ((cur_ >> (bitsLeft_ - 1)) & 1);
                value += bit;
                bitsLeft_--;
                flipByte();
            }
            return value;
        }

        uint32_t findTheFirstZeroBit(uint32_t limit) {
            uint32_t bits = 0;
            while (bits < limit) {
                uint32_t bit = (uint32_t) readValueFromBitString(1);
                if (bit == 0) {
                    return bits;
                }
                bits++;
            }
            return bits;
        }

        void flipByte() {
            if (bitsLeft_ == 0) {
                cur_ = data_[pos_];
                bitsLeft_ = 8;
                pos_++;
            }
        }
    };

    TimeSeriesStreamReader *NewTimeSeriesStreamReader(const char *data, const uint32_t size) {
        return new TimeSeriesStreamReaderImpl(data, size);
    }
}