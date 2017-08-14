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

        virtual bool hasResult()=0;

        virtual void getResult(std::string *resultSet_) =0;

        virtual void addOrMerge(const char *value, const uint32_t value_size)=0;
    };

    template<class AggType>
    class AggregatorImpl : public Aggregator {
    public:
        AggregatorImpl() {}


        uint8_t firstStats_ = 0;
        std::string firstResult_ = "";
        std::map<int32_t, AggType *> values_;

        virtual ~AggregatorImpl() {
            clear();
        }

        void clear() {
            firstStats_ = 0;
            for (typename std::map<int32_t, AggType *>::iterator iter = values_.begin();
                 iter != values_.end();) {
                delete iter->second;
                values_.erase(iter++);
            }
            firstResult_.clear();
        }


        void getResult(std::string *resultSet_) {
            if (0 == firstStats_) {
                return;
            } else if (1 == firstStats_) {
                resultSet_->assign(firstResult_.data(), firstResult_.size());
                firstStats_ = 0;
                firstResult_.clear();
                return;
            } else {
                firstStats_ = 0;
            }

            rocksdb::TimeSeriesStreamWriter writer(resultSet_);
            for (typename std::map<int32_t, AggType *>::iterator iter = values_.begin();
                 iter != values_.end();) {
                int slot = iter->first;
                writer.appendTimestamp(slot);//slot
                writeTo(writer, iter->second);
                delete iter->second;
                values_.erase(iter++);
            }
            writer.flush();
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

        bool hasResult() {
            return firstStats_ > 0;
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
            typename std::map<int32_t, AggType *>::iterator iterator = values_.begin();
            while (slot != -1) {
                bool newValue = true;
                AggType *oValue = findOrCreate(slot, iterator, newValue);
                merge(oValue, newStream, newValue);
                slot = newStream.getNextTimestamp();
            }
        }

        virtual AggType *createValue()=0;

        virtual void writeTo(rocksdb::TimeSeriesStreamWriter &writer, AggType *value)=0;

        virtual void merge(AggType *oValue, rocksdb::TimeSeriesStreamReader &newStream, bool newValue)=0;
    };

    class AggregatorIterator {
    public:
        std::map<std::string, LinDB::Aggregator *>::iterator iterator_;

        AggregatorIterator(
                std::map<std::string, LinDB::Aggregator *>::iterator &iterator) {
            iterator_ = iterator;
        }
    };

    class Aggregators {
    public:
        Aggregator *aggregator_ = nullptr;
        std::map<std::string, Aggregator *> aggregators_;
        AggregatorIterator *aggregatorsIterator_ = nullptr;
        char metric_type_;
        bool hasGroup_;
        uint32_t groupByLimit_;

        Aggregators(char metric_type, bool hasGroup, uint32_t groupByLimit) : metric_type_(metric_type),
                                                                              hasGroup_(hasGroup),
                                                                              groupByLimit_(groupByLimit) {}

        virtual ~Aggregators() {
            if (nullptr != aggregator_) {
                delete aggregator_;
            }
            if (nullptr != aggregatorsIterator_) {
                delete aggregatorsIterator_;
                aggregatorsIterator_ = nullptr;
            }
            for (std::map<std::string, LinDB::Aggregator *>::iterator iter = aggregators_.begin();
                 iter != aggregators_.end();) {
                delete iter->second;
                aggregators_.erase(iter++);
            }
        }

        void flush() {
            if (!hasGroup_) {
                return;
            }
            std::map<std::string, LinDB::Aggregator *>::iterator iterator = aggregators_.begin();
            aggregatorsIterator_ = new AggregatorIterator(iterator);
        }

        bool hasNext() {
            if (!hasGroup_) {
                if (nullptr == aggregator_) {
                    return false;
                }
                return aggregator_->hasResult();
            }
            if (nullptr == aggregatorsIterator_) {
                return false;
            }
            while (aggregatorsIterator_->iterator_ != aggregators_.end()) {
                if (aggregatorsIterator_->iterator_->second->hasResult()) {
                    return true;
                }
                aggregatorsIterator_->iterator_++;
            }
            return false;
        }

        std::string getGroupBy() {
            if (!hasGroup_) {
                return "";
            }
            if (nullptr == aggregatorsIterator_) {
                return "";
            }
            return aggregatorsIterator_->iterator_->first;
        };

        void getResult(std::string *result_) {
            if (!hasGroup_) {
                if (nullptr == aggregator_) {
                    return;
                }
                aggregator_->getResult(result_);
                return;
            }
            if (nullptr == aggregatorsIterator_ || aggregatorsIterator_->iterator_ == aggregators_.end()) {
                return;
            }
            aggregatorsIterator_->iterator_->second->getResult(result_);
        }

        virtual void reset()=0;

        virtual Aggregator *getOrCreateAgg(std::string groupBy)=0;

    protected:
        virtual Aggregator *newAggregatorImpl()=0;

    };


    extern Aggregators *NewAggregatorsImpl(char metric_type, bool hasGroup, uint32_t groupByLimit);
}
#endif //ROCKSDB_AGGREGATOR_H