//
// Created by yufu.deng on 17/8/8.
//

#include <streambuf>
#include "Aggregator.h"
#include "TSDB.h"

namespace LinDB {
    class Counter {
    public:
        int64_t value = 0;
    };

    class Gauge {
    public:
        int64_t timestamp;
        int64_t value;
    };

    class Timer {
    public:
        int64_t min;
        int64_t max;
        int64_t count = 0;
        int64_t sum = 0;
    };

    class Ratio {
    public:
        int64_t numerator;
        int64_t denominator;
    };

    class Apdex {
    public:
        int64_t total;
        int64_t satisfied;
        int64_t tolerating;
    };

    class Payload {
    public:
        int64_t min;
        int64_t max;
        int64_t count = 0;
        int64_t sum = 0;
    };

    class Histogram {
    public:
        int64_t type;
        int64_t baseNumber;
        int64_t maxSlot;
        int64_t min;
        int64_t max;
        int64_t count;
        int64_t sum;
        int64_t *values = nullptr;

        Histogram() {}

        virtual ~Histogram() {
            if (nullptr != values) {
                delete[] values;
            }
        }
    };

    class CounterAggregator : public AggregatorImpl<Counter> {
    public:
        CounterAggregator() {}

        virtual ~CounterAggregator() {}

        Counter *createValue() override {
            return new Counter();
        }

        void writeTo(rocksdb::TimeSeriesStreamWriter &writer, Counter *value) override {
            writer.appendValue(value->value);
        }

        void merge(Counter *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue) override {
            oValue->value += newStream.getNextValue();
        }
    };

    class GaugeAggregator : public AggregatorImpl<Gauge> {
    public:
        GaugeAggregator() {}

        virtual ~GaugeAggregator() {}

        Gauge *createValue() override {
            return new Gauge();
        }

        void writeTo(rocksdb::TimeSeriesStreamWriter &writer, Gauge *value) override {
            writer.appendValue(value->timestamp);
            writer.appendValue(value->value);
        }

        void merge(Gauge *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue) override {
            int64_t new_time = newStream.getNextValue();
            int64_t new_value = newStream.getNextValue();
            if (newValue || new_time >= oValue->timestamp) {
                oValue->timestamp = new_time;
                oValue->value = new_value;
            }
        }
    };

    class TimerAggregator : public AggregatorImpl<Timer> {
    public:
        TimerAggregator() {}

        virtual ~TimerAggregator() {}

        Timer *createValue() override {
            return new Timer();
        }

        void writeTo(rocksdb::TimeSeriesStreamWriter &writer, Timer *value) override {
            writer.appendValue(value->min);
            writer.appendValue(value->max);
            writer.appendValue(value->count);
            writer.appendValue(value->sum);
        }

        void merge(Timer *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue) override {
            int64_t min = newStream.getNextValue();
            int64_t max = newStream.getNextValue();
            int64_t count = newStream.getNextValue();
            int64_t sum = newStream.getNextValue();
            if (newValue) {
                oValue->min = min;
                oValue->max = max;
            } else {
                oValue->min = oValue->min > min ? min : oValue->min;
                oValue->max = oValue->max < max ? max : oValue->max;
            }
            oValue->count += count;
            oValue->sum += sum;
        }
    };

    class RatioAggregator : public AggregatorImpl<Ratio> {
    public:
        RatioAggregator() {}

        virtual ~RatioAggregator() {}

        Ratio *createValue() override {
            return new Ratio();
        }

        void writeTo(rocksdb::TimeSeriesStreamWriter &writer, Ratio *value) override {
            writer.appendValue(value->denominator);
            writer.appendValue(value->numerator);
        }

        void merge(Ratio *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue) override {
            if (newValue) {
                oValue->denominator = newStream.getNextValue();
                oValue->numerator = newStream.getNextValue();
            } else {
                oValue->denominator += newStream.getNextValue();
                oValue->numerator += newStream.getNextValue();
            }
        }
    };

    class ApdexAggregator : public AggregatorImpl<Apdex> {
    public:
        ApdexAggregator() {}

        virtual ~ApdexAggregator() {}

        Apdex *createValue() override {
            return new Apdex();
        }

        void writeTo(rocksdb::TimeSeriesStreamWriter &writer, Apdex *value) override {
            writer.appendValue(value->tolerating);
            writer.appendValue(value->satisfied);
            writer.appendValue(value->total);
        }

        void merge(Apdex *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue) override {
            if (newValue) {
                oValue->tolerating = newStream.getNextValue();
                oValue->satisfied = newStream.getNextValue();
                oValue->total = newStream.getNextValue();
            } else {
                oValue->tolerating += newStream.getNextValue();
                oValue->satisfied += newStream.getNextValue();
                oValue->total += newStream.getNextValue();
            }
        }
    };

    class PayloadAggregator : public AggregatorImpl<Payload> {
    public:
        PayloadAggregator() {}

        virtual ~PayloadAggregator() {

        }

        Payload *createValue() override {
            return new Payload();
        }

        void writeTo(rocksdb::TimeSeriesStreamWriter &writer, Payload *value) override {
            writer.appendValue(value->min);
            writer.appendValue(value->max);
            writer.appendValue(value->count);
            writer.appendValue(value->sum);
        }

        void merge(Payload *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue) override {
            int64_t min = newStream.getNextValue();
            int64_t max = newStream.getNextValue();
            int64_t count = newStream.getNextValue();
            int64_t sum = newStream.getNextValue();
            if (newValue) {
                oValue->min = min;
                oValue->max = max;
            } else {
                oValue->min = oValue->min > min ? min : oValue->min;
                oValue->max = oValue->max < max ? max : oValue->max;
            }
            oValue->count += count;
            oValue->sum += sum;
        }
    };

    class HistogramAggregator : public AggregatorImpl<Histogram> {

    public:
        HistogramAggregator() {}

        virtual ~HistogramAggregator() {}


        void writeTo(rocksdb::TimeSeriesStreamWriter &writer, Histogram *value) override {
            writer.appendValue(value->type);//type
            writer.appendValue(value->baseNumber);//baseNumber
            writer.appendValue(value->maxSlot);//max value slot
            writer.appendValue(value->min);//min
            writer.appendValue(value->max);//max
            writer.appendValue(value->count);//count
            writer.appendValue(value->sum);//sum
            for (int64_t i = 0; i < value->maxSlot; ++i) {// values_
                writer.appendValue(value->values[i]);
            }
        }

        Histogram *createValue() override {
            return new Histogram();
        }

        void merge(Histogram *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue) override {
            int64_t type = newStream.getNextValue();//type
            int64_t baseNumber = newStream.getNextValue();//baseNumber
            int64_t max_slot = newStream.getNextValue();//maxSlot
            int64_t min = newStream.getNextValue();//min
            int64_t max = newStream.getNextValue();//max
            int64_t count = newStream.getNextValue();//count
            int64_t sum = newStream.getNextValue();//sum
            if (newValue) {
                oValue->type = type;
                oValue->baseNumber = baseNumber;
                oValue->maxSlot = max_slot;
                oValue->min = min;
                oValue->max = max;
                oValue->count = count;
                oValue->sum = sum;
                oValue->values = new int64_t[max_slot];
                for (int i = 0; i < max_slot; ++i) {
                    oValue->values[i] = newStream.getNextValue();// value
                }
            } else if (type != oValue->type || baseNumber != oValue->baseNumber ||
                       max_slot != oValue->maxSlot) {
                for (int64_t i = 0; i < max_slot; ++i) {
                    newStream.getNextValue();// value
                }
            } else {
                if (oValue->min > min) {
                    oValue->min = min;
                }
                if (oValue->max < max) {
                    oValue->max = max;
                }
                oValue->count += count;
                oValue->sum += sum;
                for (int64_t i = 0; i < max_slot; ++i) {
                    oValue->values[i] += newStream.getNextValue();//value
                }
            }
        }

    };

    class EmptyAggregator : public Aggregator {
    public:
        EmptyAggregator() {}

        virtual ~EmptyAggregator() {}

        void clear() override {}

        std::string dumpResult() override {
            return "";
        }

        void addOrMerge(const char *value, const uint32_t value_size) override {}
    };

    Aggregator *NewAggregatorImpl(char metric_type) {
        if (metric_type == rocksdb::TSDB::METRIC_TYPE_COUNTER) {
            return new CounterAggregator();
        } else if (metric_type == rocksdb::TSDB::METRIC_TYPE_GAUGE) {
            return new GaugeAggregator();
        } else if (metric_type == rocksdb::TSDB::METRIC_TYPE_RATIO) {
            return new RatioAggregator();
        } else if (metric_type == rocksdb::TSDB::METRIC_TYPE_TIMER) {
            return new TimerAggregator();
        } else if (metric_type == rocksdb::TSDB::METRIC_TYPE_APDEX) {
            return new ApdexAggregator();
        } else if (metric_type == rocksdb::TSDB::METRIC_TYPE_PAYLOAD) {
            return new PayloadAggregator();
        } else if (metric_type == rocksdb::TSDB::METRIC_TYPE_HISTOGRAM) {
            return new HistogramAggregator();
        }
        return new EmptyAggregator();
    }
}