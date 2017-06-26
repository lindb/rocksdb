// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++
// for rocksdb::TransactionNotifier.

#include <jni.h>

#include "include/org_rocksdb_AbstractTransactionNotifier.h"
#include "rocksjni/transaction_notifier_jnicallback.h"

/*
 * Class:     org_rocksdb_AbstractTransactionNotifier
 * Method:    createNewTransactionNotifier
 * Signature: ()J
 */
jlong Java_org_rocksdb_AbstractTransactionNotifier_createNewTransactionNotifier(
        JNIEnv* env, jobject jobj) {
    auto* transaction_notifier =
            new rocksdb::TransactionNotifierJniCallback(env, jobj);
    auto* sptr_transaction_notifier =
            new std::shared_ptr<rocksdb::TransactionNotifierJniCallback>(
                    transaction_notifier);
    // TODO(AR) this uses a ptr to a shared_ptr and has the same issues as
    // other such uses in Java, this leaks as we never know when to delete
    // the ptr to the shared_ptr
    return reinterpret_cast<jlong>(sptr_transaction_notifier);
}

/*
 * Class:     org_rocksdb_AbstractTransactionNotifier
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractTransactionNotifier_disposeInternal(JNIEnv* env,
                                                                  jobject jobj, jlong jhandle) {
    // TODO(AR) refactor to use JniCallback::JniCallback
    // when https://github.com/facebook/rocksdb/pull/1241/ is merged
    std::shared_ptr<rocksdb::TransactionNotifierJniCallback>* handle =
            reinterpret_cast<std::shared_ptr<
            rocksdb::TransactionNotifierJniCallback>*>(jhandle);
    handle->reset();
}