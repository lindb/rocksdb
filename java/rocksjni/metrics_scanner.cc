//
// Created by jie.huang on 16/11/15.
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
 * Method:    start
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_enableLog
        (JNIEnv *env, jobject jobj, jlong handle, jboolean enableLog) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->enableLog = enableLog;
}
/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    start
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_enableProfiler
        (JNIEnv *env, jobject jobj, jlong handle, jboolean enableProfiler) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->enableProfiler = enableProfiler;
}
/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    start
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_metric
        (JNIEnv *env, jobject jobj, jlong handle, jint metric) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->metric = metric;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    startHour
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_start
        (JNIEnv *env, jobject jobj, jlong handle, jint start) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->start = start;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    endHour
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_end
        (JNIEnv *env, jobject jobj, jlong handle, jint end) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->end = end;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    maxPointCount
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_maxPointCount
        (JNIEnv *env, jobject jobj, jlong handle, jint maxPointCount) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->pointCount = maxPointCount;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    doScan
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_next
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->next();
}
/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    hasNextBaseTime
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_MetricsScanner_hasNextBaseTime
        (JNIEnv *env, jobject jobj, jlong handle,jbyte baseTime) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    return scanner->hasNextBaseTime(baseTime);
}
/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    hasNext
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_MetricsScanner_hasNext
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    return scanner->hasNext();
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getCurrentHour
 * Signature: (J)I
 */
jint Java_org_rocksdb_MetricsScanner_getCurrentBaseTime
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    return scanner->getCurrentBaseTime();
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getResultSet
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsScanner_getResultSet
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getResultSet();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getGroupBy
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsScanner_getGroupBy
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getGroupBy();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    getStat
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsScanner_getStat
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getStat();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}
/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    setTagFilters
 * Signature: (JI[B)V
 */
void Java_org_rocksdb_MetricsScanner_setTagFilters
        (JNIEnv *env, jobject jobj, jlong handler, jint jtarget_len, jbyteArray jtarget) {
    auto *scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handler);

    jbyte *target = env->GetByteArrayElements(jtarget, 0);
    rocksdb::Slice target_slice(
            reinterpret_cast<char *>(target), jtarget_len);

    scanner->setTagFilter(target_slice);

    env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    setGroupBy
 * Signature: (JI[B)V
 */
void Java_org_rocksdb_MetricsScanner_setGroupBy
        (JNIEnv *env, jobject jobj, jlong handler, jint jtarget_len, jbyteArray jtarget) {
    auto *scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handler);

    jbyte *target = env->GetByteArrayElements(jtarget, 0);
    rocksdb::Slice target_slice(
            reinterpret_cast<char *>(target), jtarget_len);

    scanner->setGroupBy(target_slice);

    env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
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