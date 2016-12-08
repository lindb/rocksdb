//
// Created by jie.huang on 16/11/22.
//

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_MetricsGroupByScanner.h"
#include "rocksjni/portal.h"
#include "rocksdb/metrics_groupby_scanner.h"

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    maxPointCount
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsGroupByScanner_maxPointCount
        (JNIEnv *env, jobject jobj, jlong handle, jint maxPointCount) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    scanner->pointCount = maxPointCount;
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    metric
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsGroupByScanner_metric
        (JNIEnv *env, jobject jobj, jlong handle, jint metric) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    scanner->metric = metric;
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    startHour
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsGroupByScanner_startHour
        (JNIEnv *env, jobject jobj, jlong handle, jint startHour) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    scanner->startHour = startHour;
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    endHour
 * Signature: (JI)V
 */
void Java_org_rocksdb_MetricsGroupByScanner_endHour(JNIEnv *env, jobject jobj, jlong handle, jint endHour) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    scanner->endHour = endHour;
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    addGroupBy
 * Signature: (JI[BI)V
 */
void Java_org_rocksdb_MetricsGroupByScanner_addGroupBy
        (JNIEnv *env, jobject jobj, jlong handle, jint tagNamesId, jbyteArray jtarget, jint jtarget_len) {
    auto *scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);

    jbyte *target = env->GetByteArrayElements(jtarget, 0);
    rocksdb::Slice target_slice(
            reinterpret_cast<char *>(target), jtarget_len);

    scanner->addGroupBy(tagNamesId, target_slice);

    env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    next
 * Signature: (J)V
 */
void Java_org_rocksdb_MetricsGroupByScanner_next
        (JNIEnv *eng, jobject obj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    scanner->next();
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    hasNext
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_MetricsGroupByScanner_hasNext
        (JNIEnv *env, jobject obj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    return scanner->hasNext();
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    getCurrentHour
 * Signature: (J)I
 */
jint Java_org_rocksdb_MetricsGroupByScanner_getCurrentHour
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    return scanner->getCurrentHour();
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    getGroupBy
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsGroupByScanner_getGroupBy
        (JNIEnv *env, jobject obj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getGroupBy();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    getResultSet
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_MetricsGroupByScanner_getResultSet
        (JNIEnv *env, jobject jobj, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    rocksdb::Slice value_slice = scanner->getResultSet();
    jsize size = static_cast<jsize>(value_slice.size());
    jbyteArray jkeyValue = env->NewByteArray(size);
    env->SetByteArrayRegion(jkeyValue, 0, size,
                            reinterpret_cast<const jbyte *>(value_slice.data()));
    return jkeyValue;
}

/*
 * Class:     org_rocksdb_MetricsGroupByScanner
 * Method:    disposeInternal0
 * Signature: (J)V
 */
void Java_org_rocksdb_MetricsGroupByScanner_disposeInternal
        (JNIEnv *env, jobject jojb, jlong handle) {
    auto scanner = reinterpret_cast<rocksdb::MetricsGroupByScanner *>(handle);
    delete scanner;
}
