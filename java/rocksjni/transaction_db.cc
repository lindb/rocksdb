// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++
// for rocksdb::TransactionDB.

#include <jni.h>

#include "include/org_rocksdb_TransactionDB.h"

#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    open
 * Signature: (JJLjava/lang/String;)J
 */
jlong Java_org_rocksdb_TransactionDB_open__JJLjava_lang_String_2(JNIEnv* env,
                                                                 jclass jcls, jlong joptions_handle, jlong jtxn_db_options_handle,
                                                                 jstring jdb_path) {
    auto* options = reinterpret_cast<rocksdb::Options*>(joptions_handle);
    auto* txn_db_options =
            reinterpret_cast<rocksdb::TransactionDBOptions*>(jtxn_db_options_handle);
    rocksdb::TransactionDB* tdb = nullptr;
    const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);
    rocksdb::Status s =
            rocksdb::TransactionDB::Open(*options, *txn_db_options, db_path, &tdb);
    env->ReleaseStringUTFChars(jdb_path, db_path);

    if (s.ok()) {
        return reinterpret_cast<jlong>(tdb);
    } else {
        rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
        return 0;
    }
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    open
 * Signature: (JJLjava/lang/String;[[B[J)[J
 */
jlongArray Java_org_rocksdb_TransactionDB_open__JJLjava_lang_String_2_3_3B_3J(
        JNIEnv* env, jclass jcls, jlong jdb_options_handle,
        jlong jtxn_db_options_handle, jstring jdb_path,
        jobjectArray jcolumn_names,
        jlongArray jcolumn_options_handles) {
    auto* db_options = reinterpret_cast<rocksdb::DBOptions*>(jdb_options_handle);
    auto* txn_db_options =
            reinterpret_cast<rocksdb::TransactionDBOptions*>(jtxn_db_options_handle);
    const char* db_path = env->GetStringUTFChars(jdb_path, nullptr);

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

    jsize len_cols = env->GetArrayLength(jcolumn_names);
    jlong* jco = env->GetLongArrayElements(jcolumn_options_handles, nullptr);
    for (int i = 0; i < len_cols; i++) {
        jobject jcn = env->GetObjectArrayElement(jcolumn_names, i);
        jbyteArray jcn_ba = reinterpret_cast<jbyteArray>(jcn);
        jbyte* jcf_name = env->GetByteArrayElements(jcn_ba, nullptr);

        const int jcf_name_len = env->GetArrayLength(jcn_ba);
        std::string cf_name(reinterpret_cast<char *>(jcf_name), jcf_name_len);
        rocksdb::ColumnFamilyOptions* cf_options =
                reinterpret_cast<rocksdb::ColumnFamilyOptions*>(jco[i]);
        column_families.push_back(
                rocksdb::ColumnFamilyDescriptor(cf_name, *cf_options));

        env->ReleaseByteArrayElements(jcn_ba, jcf_name, JNI_ABORT);
        env->DeleteLocalRef(jcn);
    }
    env->ReleaseLongArrayElements(jcolumn_options_handles, jco, JNI_ABORT);

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::TransactionDB* tdb = nullptr;
    rocksdb::Status s = rocksdb::TransactionDB::Open(*db_options, *txn_db_options,
                                                     db_path, column_families, &handles, &tdb);

    // check if open operation was successful
    if (s.ok()) {
        jsize resultsLen = 1 + len_cols;  // db handle + column family handles
        std::unique_ptr<jlong[]> results =
                std::unique_ptr<jlong[]>(new jlong[resultsLen]);
        results[0] = reinterpret_cast<jlong>(tdb);
        for (int i = 1; i <= len_cols; i++) {
            results[i] = reinterpret_cast<jlong>(handles[i - 1]);
        }

        jlongArray jresults = env->NewLongArray(resultsLen);
        env->SetLongArrayRegion(jresults, 0, resultsLen, results.get());
        return jresults;
    } else {
        rocksdb::RocksDBExceptionJni::ThrowNew(env, s);
        return nullptr;
    }
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction__JJ(JNIEnv* env,
                                                          jobject jobj, jlong jhandle, jlong jwrite_options_handle) {
    auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
    auto* write_options =
            reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
    rocksdb::Transaction* txn = txn_db->BeginTransaction(*write_options);
    return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction__JJJ(JNIEnv* env,
                                                           jobject jobj, jlong jhandle, jlong jwrite_options_handle,
                                                           jlong jtxn_options_handle) {
    auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
    auto* write_options =
            reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
    auto* txn_options =
            reinterpret_cast<rocksdb::TransactionOptions*>(jtxn_options_handle);
    rocksdb::Transaction* txn =
            txn_db->BeginTransaction(*write_options, *txn_options);
    return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction_1withOld__JJJ(
        JNIEnv* env, jobject jobj, jlong jhandle, jlong jwrite_options_handle,
        jlong jold_txn_handle) {
    auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
    auto* write_options =
            reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
    auto* old_txn = reinterpret_cast<rocksdb::Transaction*>(jold_txn_handle);
    rocksdb::TransactionOptions txn_options;
    rocksdb::Transaction* txn =
            txn_db->BeginTransaction(*write_options, txn_options, old_txn);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(txn == old_txn);

    return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    beginTransaction_withOld
 * Signature: (JJJJ)J
 */
jlong Java_org_rocksdb_TransactionDB_beginTransaction_1withOld__JJJJ(
        JNIEnv* env, jobject jobj, jlong jhandle, jlong jwrite_options_handle,
        jlong jtxn_options_handle, jlong jold_txn_handle) {
    auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
    auto* write_options =
            reinterpret_cast<rocksdb::WriteOptions*>(jwrite_options_handle);
    auto* txn_options =
            reinterpret_cast<rocksdb::TransactionOptions*>(jtxn_options_handle);
    auto* old_txn = reinterpret_cast<rocksdb::Transaction*>(jold_txn_handle);
    rocksdb::Transaction* txn = txn_db->BeginTransaction(*write_options,
                                                         *txn_options, old_txn);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(txn == old_txn);

    return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    getTransactionByName
 * Signature: (JLjava/lang/String;)J
 */
jlong Java_org_rocksdb_TransactionDB_getTransactionByName(JNIEnv* env,
                                                          jobject jobj, jlong jhandle, jstring jname) {
    auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
    const char* name = env->GetStringUTFChars(jname, nullptr);
    rocksdb::Transaction* txn = txn_db->GetTransactionByName(name);
    env->ReleaseStringUTFChars(jname, name);
    return reinterpret_cast<jlong>(txn);
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    getAllPreparedTransactions
 * Signature: (J)[J
 */
jlongArray Java_org_rocksdb_TransactionDB_getAllPreparedTransactions(
        JNIEnv* env, jobject jobj, jlong jhandle) {
    auto* txn_db = reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
    std::vector<rocksdb::Transaction*> txns;
    txn_db->GetAllPreparedTransactions(&txns);

    const size_t size = txns.size();
    assert(size < UINT32_MAX);  // does it fit in a jint?

    jsize len = static_cast<jsize>(size);
    jlong* tmp = new jlong[len];
    for (jsize i = 0; i < len; ++i) {
        tmp[i] = reinterpret_cast<jlong>(txns[i]);
    }

    jlongArray jtxns = env->NewLongArray(len);
    env->SetLongArrayRegion(jtxns, 0, len, tmp);
    delete [] tmp;

    return jtxns;
}

/*
 * Class:     org_rocksdb_TransactionDB
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_TransactionDB_disposeInternal(JNIEnv* env, jobject jobj,
                                                    jlong jhandle) {
    delete reinterpret_cast<rocksdb::TransactionDB*>(jhandle);
}