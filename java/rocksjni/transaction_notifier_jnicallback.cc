// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::TransactionNotifier.

#include "rocksjni/transaction_notifier_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

    TransactionNotifierJniCallback::TransactionNotifierJniCallback(JNIEnv* env,
                                                                   jobject jtransaction_notifier) {
        // Note: TransactionNotifier methods may be accessed by multiple threads,
        // so we ref the jvm not the env
        const jint rs __attribute__((unused)) = env->GetJavaVM(&m_jvm);
        assert(rs == JNI_OK);

        // Note: we want to access the Java TransactionNotifier instance
        // across multiple method calls, so we create a global ref
        m_jtransaction_notifier = env->NewGlobalRef(jtransaction_notifier);

        // we also cache the method id for the JNI callback
        m_jsnapshot_created_methodID =
                AbstractTransactionNotifierJni::getSnapshotCreatedMethodId(env);
    }

    void TransactionNotifierJniCallback::SnapshotCreated(
            const Snapshot* newSnapshot) {
        // attach the current thread
        JNIEnv* m_env = getJniEnv();

        m_env->CallVoidMethod(m_jtransaction_notifier,
                              m_jsnapshot_created_methodID, reinterpret_cast<jlong>(newSnapshot));

        m_jvm->DetachCurrentThread();
    }

/**
 * Attach/Get a JNIEnv for the current native thread
 */
    JNIEnv* TransactionNotifierJniCallback::getJniEnv() const {
        JNIEnv *env;
        jint rs __attribute__((unused)) =
                m_jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
        assert(rs == JNI_OK);
        return env;
    }

    TransactionNotifierJniCallback::~TransactionNotifierJniCallback() {
        JNIEnv* m_env = getJniEnv();

        m_env->DeleteGlobalRef(m_jtransaction_notifier);

        // Note: do not need to explicitly detach, as this function is effectively
        // called from the Java class's disposeInternal method, and so already
        // has an attached thread, getJniEnv above is just a no-op Attach to get
        // the env jvm->DetachCurrentThread();
    }


}  // namespace rocksdb