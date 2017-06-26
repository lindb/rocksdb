// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TransactionNotifier.

#ifndef JAVA_ROCKSJNI_TRANSACTION_NOTIFIER_JNICALLBACK_H_
#define JAVA_ROCKSJNI_TRANSACTION_NOTIFIER_JNICALLBACK_H_

#include <jni.h>

#include "rocksdb/utilities/transaction.h"

namespace rocksdb {

/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB TransactionDB or OptimisticTransactionDB (C++),
 * we then callback to the appropriate Java method
 * this enables TransactionNotifier to be implemented in Java.
 *
 * Unlike RocksJava's Comparator JNI Callback, we do not attempt
 * to reduce Java object allocations by caching the Snapshot object
 * presented to the callback. This could be revisited in future
 * if performance is lacking.
 */
    class TransactionNotifierJniCallback : public TransactionNotifier {
    public:
        TransactionNotifierJniCallback(JNIEnv* env, jobject jtransaction_notifier);
        ~TransactionNotifierJniCallback();
        virtual void SnapshotCreated(const Snapshot* newSnapshot);

    private:
        JavaVM* m_jvm;
        jobject m_jtransaction_notifier;
        jmethodID m_jsnapshot_created_methodID;
        JNIEnv* getJniEnv() const;
    };
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_TRANSACTION_NOTIFIER_JNICALLBACK_H_