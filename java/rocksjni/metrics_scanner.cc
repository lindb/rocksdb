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
void Java_org_rocksdb_MetricsScanner_startHour
        (JNIEnv *env, jobject jobj, jlong handle, jint startHour) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->startHour = startHour;
}

/*
 * Class:     org_rocksdb_MetricsScanner
 * Method:    endHour
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsScanner_endHour
        (JNIEnv *env, jobject jobj, jlong handle, jint endHour) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    scanner->endHour = endHour;
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
jint Java_org_rocksdb_MetricsScanner_getCurrentHour
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    return scanner->getCurrentHour();
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
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_MetricsScanner_disposeInternal
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsScanner *>(handle);
    delete scanner;
}