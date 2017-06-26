// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

public class OptimisticTransactionOptions extends RocksObject
    implements TransactionalOptions {

  public OptimisticTransactionOptions() {
    super(newOptimisticTransactionOptions());
  }

  @Override
  public boolean isSetSnapshot() {
    assert(isOwningHandle());
    return isSetSnapshot(nativeHandle_);
  }

  @Override
  public OptimisticTransactionOptions setSetSnapshot(
      final boolean setSnapshot) {
    assert(isOwningHandle());
    setSetSnapshot(nativeHandle_, setSnapshot);
    return this;
  }

  /**
   *  Should be set if the DB has a non-default comparator.
   *  See comment in
   *  {@link WriteBatchWithIndex#WriteBatchWithIndex(AbstractComparator, int, boolean)}
   *  constructor.
   *
   * @return this OptimisticTransactionOptions instance
   */
  public OptimisticTransactionOptions setComparator(
      final AbstractComparator<? extends AbstractSlice<?>> comparator) {
    assert(isOwningHandle());
    setComparator(nativeHandle_, comparator.getNativeHandle());
    return this;
  }

  private native static long newOptimisticTransactionOptions();
  private native boolean isSetSnapshot(final long handle);
  private native void setSetSnapshot(final long handle,
      final boolean setSnapshot);
  private native void setComparator(final long handle,
      final long comparatorHandle);
  @Override protected final native void disposeInternal(final long handle);
}