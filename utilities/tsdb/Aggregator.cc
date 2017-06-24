//
// Created by yufu.deng on 17/6/24.
//

#include <map>
#include "Aggregator.h"
#include "TimeSeriesStreamReader.h"
#include "TimeSeriesStreamWriter.h"
#include "string"

using namespace std;

#pragma once

//namespace rocksdb {
//
//}
//
//namespace rocksdb {
//    class CounterAggregator : public Aggregator {
//    private:
//        bool close_ = false;
//    public:
//
//        CounterAggregator() {}
//
//        virtual ~CounterAggregator() {}
//
//        virtual void merge(const char *value, const uint32_t value_size) {
//
//        }
//    };
//
//}  // namespace rocksdb
//
//#endif //ROCKSDB_METRICS_AGGREGATOR


namespace rocksdb {
    class LinDBHistogram {
    public:
        int64_t type = -1;
        int64_t baseNumber = 0;
        int64_t maxSlot = 0;
        int64_t min = 1;
        int64_t max = 1;
        int64_t sum = 0;
        int64_t count = 0;
        int64_t *values = nullptr;

        LinDBHistogram() {
            min = min << 62;
            max = (max << 63) >> 63;
        }

        ~LinDBHistogram() {
            if (nullptr != values) {
                delete[] values;
            }
        }
    };


    class HistogramAggregator : public Aggregator {
    private:
        std::map<int16_t, LinDBHistogram> histograms_;
    public:

        HistogramAggregator() {}

        virtual ~HistogramAggregator() {
        }

        std::string dumpResult() {
            std::string resultSet = "";
            TimeSeriesStreamWriter writer(&resultSet);
            if (histograms_.empty()) {
                return resultSet;
            }
            auto iter = histograms_.begin();
            while (iter != histograms_.end()) {
                int32_t slot = iter->first;
                LinDBHistogram linDBHistogram = iter->second;
                writer.appendTimestamp(slot);
                writer.appendValue(linDBHistogram.type);//type
                writer.appendValue(linDBHistogram.baseNumber);//baseNumber
                writer.appendValue(linDBHistogram.maxSlot);//max value slot
                writer.appendValue(linDBHistogram.min);//min
                writer.appendValue(linDBHistogram.max);//max
                writer.appendValue(linDBHistogram.sum);//sum
                for (int64_t i = 0; i < linDBHistogram.maxSlot; ++i) {// values
                    writer.appendValue(linDBHistogram.values[i]);
                }
                histograms_.erase(iter++);
            }
            writer.flush();
            histograms_.clear();
        }

        void merge(const char *value, const uint32_t value_size) {
            TimeSeriesStreamReader newStream(value, value_size);
            int16_t slot = newStream.getNextTimestamp();
            while (slot != -1) {
                int64_t type = newStream.getNextValue();//type
                int64_t baseNumber = newStream.getNextValue();//baseNumber
                int64_t max_slot = newStream.getNextValue();
                int64_t min = newStream.getNextValue();//min
                int64_t max = newStream.getNextValue();//max
                int64_t sum = newStream.getNextValue();//sum
                LinDBHistogram linDBHistogram = histograms_[slot];
                if (-1 == linDBHistogram.type) {
                    linDBHistogram.type = type;
                    linDBHistogram.baseNumber = baseNumber;
                    linDBHistogram.maxSlot = max_slot;
                    linDBHistogram.min = min;
                    linDBHistogram.max = max;
                    linDBHistogram.sum = sum;
                    linDBHistogram.values = new int64_t[max_slot];
                    for (int i = 0; i < max_slot; ++i) {
                        linDBHistogram.values[i] = newStream.getNextValue();// value
                    }
                } else if (type != linDBHistogram.type || baseNumber != linDBHistogram.baseNumber ||
                           max_slot != linDBHistogram.maxSlot) {
                    for (int64_t i = 0; i < max_slot; ++i) {
                        newStream.getNextValue();// value
                    }
                } else {
                    linDBHistogram.min = linDBHistogram.min > min ? min : linDBHistogram.min;
                    linDBHistogram.max = linDBHistogram.max < max ? max : linDBHistogram.max;
                    linDBHistogram.sum += sum;
                    for (int64_t i = 0; i < max_slot; ++i) {
                        linDBHistogram.values[i] += newStream.getNextValue();// value
                    }
                }
                //reset new slot for next loop
                slot = newStream.getNextTimestamp();
            }

        }
    };

}  // namespace rocksdb