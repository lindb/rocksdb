//
// Created by jie.huang on 17/2/13.
//
#include <cmath>
#include <cstdlib>
#include "TimeSeriesStreamWriter.h"
#include "TSDB.h"

namespace rocksdb {

    void TimeSeriesStreamWriter::append(uint16_t timestamp, int64_t value) {
        appendTimestamp(timestamp);
        appendValue(value);
    }

    /**
     * Store a delta of delta for the rest fo the values in one of the following ways:
     * '0' = delta of delta did not change
     * '10' = followed by a value length of 7
     * '110' = followed by a value length of 9
     * '1110' = followed by a value length of 12
     * '1111' = followed by a value length of 16
     */
    void TimeSeriesStreamWriter::appendTimestamp(uint16_t timestamp) {
        if (count_ == 0) {
            addValueToBitString(timestamp, TSDB::kBitsForFirstTimestamp);
            prevTimestamp_ = timestamp;
            prevTimestampDelta_ = 0;
        } else {
            //calculate the delta of delta
            int64_t delta = timestamp - prevTimestamp_;
            int64_t deltaOfDelta = delta - prevTimestampDelta_;
            //if delta is zero, write single 0 bit
            if (deltaOfDelta == 0) {
                prevTimestamp_ = timestamp;
                addValueToBitString(0, 1);
            } else {
                if (deltaOfDelta > 0) {
                    // There are no zeros. Shift by one to fit in x number of bits
                    deltaOfDelta--;
                }

                int64_t absValue = std::abs(deltaOfDelta);
                for (int i = 0; i < 4; i++) {
                    if (absValue < ((int64_t) 1 << (timestampEncodings[i].bitsForValue - 1))) {
                        addValueToBitString(
                                timestampEncodings[i].controlValue,
                                timestampEncodings[i].controlValueBitLength);

                        // Make this value between [0, 2^timestampEncodings[i].bitsForValue - 1]
                        int64_t encodedValue =
                                deltaOfDelta + ((int64_t) 1 << (timestampEncodings[i].bitsForValue - 1));

                        addValueToBitString(
                                (uint64_t) encodedValue, timestampEncodings[i].bitsForValue);
                        break;
                    }
                }

                prevTimestamp_ = timestamp;
                prevTimestampDelta_ = delta;
            }

        }
        count_++;
    }

    /**
      * Value is encoded by XORing them with previous value.
      * If XORing results is a zero value(value is the same as the previous value),
      * only a single zero bit is stored, otherwise 1 bit is stored.
      *
      * For non-zero XORred results, there are two choices:
      * 1) If the block of meaningful bits falls in between the block of previous meaningful bits,
      *    i.e., there are at least as many leading zeros and as many trailing zeros as with the previous value,
      *    use that information for the block position and just store the XORred value.
      *
      * 2) Length of the number of leading zeros is stored in the next 6 bits,
      *    then length of the XORred value is stored in the next  6 bits and
      *    finally the XORred value is stored.
      */
    void TimeSeriesStreamWriter::appendValue(int64_t value) {
        uint64_t xorWithPrevious = previousValue_ ^value;

        if (xorWithPrevious == 0) {
            addValueToBitString(0, 1);
            return;
        } else {
            addValueToBitString(1, 1);

            uint32_t leadingZeros = __builtin_clzll(xorWithPrevious);
            uint32_t trailingZeros = __builtin_ctzll(xorWithPrevious);

            if (leadingZeros > TSDB::kMaxLeadingZerosLength) {
                leadingZeros = TSDB::kMaxLeadingZerosLength;
            }

            int blockSize = 64 - leadingZeros - trailingZeros;
            uint32_t previousBlockInformationSize = 64 - previousValueTrailingZeros_ - previousValueLeadingZeros_;
            uint32_t expectedSize = blockSize + TSDB::kLeadingZerosLengthBits + TSDB::kBlockSizeLengthBits;

            if (leadingZeros >= previousValueLeadingZeros_ &&
                trailingZeros >= previousValueTrailingZeros_ &&
                previousBlockInformationSize < expectedSize) {
                // Control bit for using previous block information.
                addValueToBitString(1, 1);
                /**
                * If there at least many leading zeros and as many trailing zeros as previous value, control bit = 0(type a)
                * store the meaningful XORed value
                */
                uint64_t blockValue = xorWithPrevious >> previousValueTrailingZeros_;
                addValueToBitString(
                        blockValue, previousBlockInformationSize);
            } else {
                // Control bit for not using previous block information.
                addValueToBitString(0, 1);
                /**
                 * Store the length of the number of leading zeros in the next 6 bits.
                 * Store the length of the meaningful XORed value in the next 6 bits.
                 * Store the meaningful bits of the XORed value(type b).
                 */
                addValueToBitString(leadingZeros, TSDB::kLeadingZerosLengthBits);

                // To fit in 6 bits. There will never be a zero size block
                addValueToBitString((blockSize - TSDB::kBlockSizeAdjustment),
                                    TSDB::kBlockSizeLengthBits);

                uint64_t tmp = xorWithPrevious;

                uint64_t blockValue = tmp >> trailingZeros;
                addValueToBitString(blockValue, blockSize);

                previousValueTrailingZeros_ = trailingZeros;
                previousValueLeadingZeros_ = leadingZeros;
            }

            previousValue_ = value;
        }
    }

    void TimeSeriesStreamWriter::flush() {
        if (bitsAvailable_ != 8) {
            bitsAvailable_ = 0;
            flipByte();
        }
        char buf[sizeof(count_)];
        buf[3] = count_ & 0xff;
        buf[2] = (count_ >> 8) & 0xff;
        buf[1] = (count_ >> 16) & 0xff;
        buf[0] = (count_ >> 24) & 0xff;
        data_->append(buf, sizeof(buf));
    }

    void TimeSeriesStreamWriter::addValueToBitString(int64_t value, uint32_t bitsInValue) {
        if (bitsInValue <= bitsAvailable_) {
            cur_ |= (value << (bitsAvailable_ - bitsInValue));
            bitsAvailable_ -= bitsInValue;
            flipByte();
            return;
        }

        uint32_t bitsLeft = bitsInValue;
        if (bitsAvailable_ > 0) {
            // Fill up the last byte
            if (bitsAvailable_ < 8) {
                uint64_t tmp = value;
                cur_ |= tmp >> (bitsInValue - bitsAvailable_);;
            } else {
                cur_ |= value >> (bitsInValue - bitsAvailable_);
            }
            bitsLeft -= bitsAvailable_;
            bitsAvailable_ = 0;
            flipByte();
        }

        while (bitsLeft >= 8) {
            // Enough bits for a dedicated byte
            char ch = (char) ((value >> (bitsLeft - 8)) & 0xFF);
            data_->append(1, ch);
            bitsLeft -= 8;
        }

        if (bitsLeft != 0) {
            // Start a new byte with the rest of the bits
            cur_ = (char) ((value & ((1 << bitsLeft) - 1)) << (8 - bitsLeft));
            bitsAvailable_ -= bitsLeft;
        }
    }

    void TimeSeriesStreamWriter::flipByte() {
        if (bitsAvailable_ == 0) {
            data_->append(1, cur_);
            cur_ = 0;
            bitsAvailable_ = 8;
        }
    }
}
