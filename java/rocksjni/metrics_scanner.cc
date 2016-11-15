//
// Created by jie.huang1 on 16/11/9.
//
#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_MetricsScanner.h"
#include "rocksjni/portal.h"
#include "rocksdb/metrics_scanner.h"

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    setMetrics
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_setMetrics
        (JNIEnv *env, jobject jobj, jlong handle, jint metricsId) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->metricsId = metricsId;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    setTime
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_setTime
        (JNIEnv *env, jobject jobj, jlong handle, jint time) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->time = time;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    enableCount
 * Signature: (JZ)V
 */
void Java_org_rocksdb_MetricsScanner_enableCount
        (JNIEnv *env, jobject jobj, jlong handle, jboolean enable) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->countEnable = enable;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    enableSum
 * Signature: (JZ)V
 */
void Java_org_rocksdb_MetricsScanner_enableSum
        (JNIEnv *env, jobject jobj, jlong handle, jboolean enable) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->sumEnable = enable;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    enableMin
 * Signature: (JZ)V
 */
void Java_org_rocksdb_MetricsScanner_enableMin
        (JNIEnv *env, jobject jobj, jlong handle, jboolean enable) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->minEnable = enable;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    enableMax
 * Signature: (JZ)V
 */
void Java_org_rocksdb_MetricsScanner_enableMax
        (JNIEnv *env, jobject jobj, jlong handle, jboolean enable) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->maxEnable = enable;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    setStartSlot
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_setSlotRange
        (JNIEnv *env, jobject jobj, jlong handle, jint startSlot,jint endSlot) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->startSlot = startSlot;
    scanner->endSlot = endSlot;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    doScan
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_doScan
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->doScan();
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getCountResult
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsScanner_getCountResult
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getCountResult();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getSumResult
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsScanner_getSumResult
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getSumResult();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getMinResult
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsScanner_getMinResult
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getMinResult();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getMaxResult
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsScanner_getMaxResult
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getMaxResult();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_MetricsScanner_disposeInternal
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    delete scanner;
}


