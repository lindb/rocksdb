// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++
// for rocksdb::TransactionOptions.

#include <jni.h>

#include "include/org_rocksdb_TransactionOptions.h"

#include "rocksdb/utilities/transaction_db.h"

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    newTransactionOptions
 * Signature: ()J
 */
jlong Java_org_rocksdb_TransactionOptions_newTransactionOptions(JNIEnv* env,
                                                                jclass jcls) {
    rocksdb::TransactionOptions* opts = new rocksdb::TransactionOptions();
    return reinterpret_cast<jlong>(opts);
}

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    isSetSnapshot
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_TransactionOptions_isSetSnapshot(JNIEnv* env,
                                                           jobject jobj, jlong jhandle) {
    auto* opts = reinterpret_cast<rocksdb::TransactionOptions*>(jhandle);
    return opts->set_snapshot;
}

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    setSetSnapshot
 * Signature: (JZ)V
 */
void Java_org_rocksdb_TransactionOptions_setSetSnapshot(JNIEnv* env,
                                                        jobject jobj, jlong jhandle, jboolean jset_snapshot) {
    auto* opts = reinterpret_cast<rocksdb::TransactionOptions*>(jhandle);
    opts->set_snapshot = jset_snapshot;
}

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    getLockTimeout
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TransactionOptions_getLockTimeout(JNIEnv* env,
                                                         jobject jobj, jlong jhandle) {
    auto* opts = reinterpret_cast<rocksdb::TransactionOptions*>(jhandle);
    return opts->lock_timeout;
}

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    setLockTimeout
 * Signature: (JJ)V
 */
void Java_org_rocksdb_TransactionOptions_setLockTimeout(JNIEnv* env,
                                                        jobject jobj, jlong jhandle, jlong jlock_timeout) {
    auto* opts = reinterpret_cast<rocksdb::TransactionOptions*>(jhandle);
    opts->lock_timeout = jlock_timeout;
}

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    getExpiration
 * Signature: (J)J
 */
jlong Java_org_rocksdb_TransactionOptions_getExpiration(JNIEnv* env,
                                                        jobject jobj, jlong jhandle) {
    auto* opts = reinterpret_cast<rocksdb::TransactionOptions*>(jhandle);
    return opts->expiration;
}

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    setExpiration
 * Signature: (JJ)V
 */
void Java_org_rocksdb_TransactionOptions_setExpiration(JNIEnv* env,
                                                       jobject jobj, jlong jhandle, jlong jexpiration) {
    auto* opts = reinterpret_cast<rocksdb::TransactionOptions*>(jhandle);
    opts->expiration = jexpiration;
}

/*
 * Class:     org_rocksdb_TransactionOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_TransactionOptions_disposeInternal(JNIEnv* env,
                                                         jobject jobj, jlong jhandle) {
    delete reinterpret_cast<rocksdb::TransactionOptions*>(jhandle);
}