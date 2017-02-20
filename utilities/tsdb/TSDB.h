//
// Created by jie.huang on 17/2/14.
//

#include <stdint.h>
#include <cstdint>

namespace rocksdb {
    struct {
        uint32_t bitsForValue;
        uint32_t controlValue;
        uint32_t controlValueBitLength;
    } static const timestampEncodings[4] = {{7,  0x02, 2},
                                            {9,  0x06, 3},
                                            {12, 0x0E, 4},
                                            {16, 0x0F, 4}};

    class TSDB {
    public:
        static const char METRIC_TYPE_GAUGE;
        static const char METRIC_TYPE_TIMER;
        static const char METRIC_TYPE_COUNTER;
        static const char METRIC_TYPE_PERCENT;
        static const char METRIC_TYPE_APDEX;

        static constexpr uint32_t kBitsForFirstTimestamp = 16;
        static constexpr uint32_t kLeadingZerosLengthBits = 6;
        static constexpr uint32_t kBlockSizeLengthBits = 6;
        static constexpr uint32_t kBlockSizeAdjustment = 1;
        static constexpr uint32_t kMaxLeadingZerosLength = (1 << kLeadingZerosLengthBits) - 1;
    };
}
