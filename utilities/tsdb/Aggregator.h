//
// Created by yufu.deng on 17/8/8.
//
#ifndef ROCKSDB_AGGREGATOR_H
#define ROCKSDB_AGGREGATOR_H

#include "TimeSeriesStreamWriter.h"
#include "TimeSeriesStreamReader.h"
#include <map>

namespace LinDB {

    class Aggregator {
    public:
        Aggregator() {}

        virtual ~Aggregator() {}

        virtual void clear()=0;

        virtual std::string dumpResult()=0;

        virtual void addOrMerge(const char *value, const uint32_t value_size)=0;
    };

    template<class AggType>
    class AggregatorImpl : public Aggregator {
    public:
        AggregatorImpl() {}


        uint8_t firstStats_ = 0;
        std::string firstResult_ = "";
        std::string resultSet_ = "";
        std::map<int32_t, AggType *> values_;

        virtual ~AggregatorImpl() {
            clear();
        }

        void clear() {
            for (typename std::map<int32_t, AggType *>::iterator iter = values_.begin();
                 iter != values_.end();) {
                delete iter->second;
                values_.erase(iter++);
            }
        }


        std::string dumpResult() {
            resultSet_.clear();
            if (0 == firstStats_) {
                return resultSet_;
            } else if (1 == firstStats_) {
                resultSet_.assign(firstResult_.data(), firstResult_.size());
                firstStats_ = 0;
                firstResult_.clear();
                return resultSet_;
            } else if (2 == firstStats_) {
                firstStats_ = 0;
            }

            rocksdb::TimeSeriesStreamWriter writer(&resultSet_);
            for (typename std::map<int32_t, AggType *>::iterator iter = values_.begin();
                 iter != values_.end();) {
                int slot = iter->first;
                writer.appendTimestamp(slot);//slot
                writeTo(writer, iter->second);
                delete iter->second;
                values_.erase(iter++);
            }
            writer.flush();
            return resultSet_;
        }

        AggType *findOrCreate(int32_t slot, typename std::map<int32_t, AggType *>::iterator &iter, bool &newValue) {
            while (iter != values_.end()) {
                if (iter->first == slot) {
                    newValue = false;
                    return iter->second;
                } else if (iter->first > slot) {
                    AggType *value = createValue();
                    values_.insert(typename std::map<int32_t, AggType *>::value_type(slot, value));
                    return value;
                }
                iter++;
            }
            AggType *value = createValue();
            values_.insert(typename std::map<int32_t, AggType *>::value_type(slot, value));
            return value;
        }

        void addOrMerge(const char *value, const uint32_t value_size) {
            if (0 == firstStats_) {
                firstResult_.assign(value, value_size);
                firstStats_ = 1;
                return;
            } else if (1 == firstStats_) {
                merge(firstResult_.data(), (uint32_t) firstResult_.size());
                firstResult_.clear();
                firstStats_ = 2;
            }
            merge(value, value_size);
        }

        void merge(const char *value, const uint32_t value_size) {
            rocksdb::TimeSeriesStreamReader newStream(value, value_size);
            int32_t slot = newStream.getNextTimestamp();
            typename std::map<int32_t, AggType *>::iterator histogramIterator = values_.begin();
            while (slot != -1) {
                bool newValue = true;
                AggType *oValue = findOrCreate(slot, histogramIterator, newValue);
                merge(oValue, newStream, newValue);
                slot = newStream.getNextTimestamp();
            }
        }

        virtual AggType *createValue()=0;

        virtual void writeTo(rocksdb::TimeSeriesStreamWriter &writer, AggType *value)=0;

        virtual void merge(AggType *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue)=0;
    };

    extern Aggregator *NewAggregatorImpl(char metric_type);
}
#endif //ROCKSDB_AGGREGATOR_H