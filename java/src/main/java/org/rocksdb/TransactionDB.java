// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.ArrayList;
import java.util.List;

/**
 * Database with Transaction support
 */
public class TransactionDB extends RocksDB
    implements TransactionalDB<TransactionOptions> {

  private TransactionDBOptions transactionDbOptions_;

  /**
   * Private constructor.
   *
   * @param nativeHandle The native handle of the C++ TransactionDB object
   */
  private TransactionDB(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * Open a TransactionDB, similar to {@link RocksDB#open(Options, String)}
   */
  public static TransactionDB open(final Options options,
      final TransactionDBOptions transactionDbOptions, final String path)
      throws RocksDBException {
    final TransactionDB tdb = new TransactionDB(open(options.nativeHandle_,
        transactionDbOptions.nativeHandle_, path));

    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    tdb.storeOptionsInstance(options);
    tdb.storeTransactionDbOptions(transactionDbOptions);

    return tdb;
  }

  /**
   * Open a TransactionDB, similar to
   * {@link RocksDB#open(DBOptions, String, List, List)}
   */
  public static TransactionDB open(final DBOptions dbOptions,
      final TransactionDBOptions transactionDbOptions,
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.columnFamilyName();
      cfOptionHandles[i] = cfDescriptor.columnFamilyOptions().nativeHandle_;
    }

    final long[] handles = open(dbOptions.nativeHandle_,
        transactionDbOptions.nativeHandle_, path, cfNames, cfOptionHandles);
    final TransactionDB tdb = new TransactionDB(handles[0]);

    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    tdb.storeOptionsInstance(dbOptions);
    tdb.storeTransactionDbOptions(transactionDbOptions);

    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(tdb, handles[i]));
    }

    return tdb;
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions) {
    return new Transaction(this, beginTransaction(nativeHandle_,
        writeOptions.nativeHandle_));
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final TransactionOptions transactionOptions) {
    return new Transaction(this, beginTransaction(nativeHandle_,
        writeOptions.nativeHandle_, transactionOptions.nativeHandle_));
  }

  // TODO(AR) consider having beingTransaction(... oldTransaction) set a
  // reference count inside Transaction, so that we can always call
  // Transaction#close but the object is only disposed when there are as many
  // closes as beginTransaction. Makes the try-with-resources paradigm easier for
  // java developers

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final Transaction oldTransaction) {
    final long jtxnHandle = beginTransaction_withOld(nativeHandle_,
        writeOptions.nativeHandle_, oldTransaction.nativeHandle_);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(jtxnHandle == oldTransaction.nativeHandle_);

    return oldTransaction;
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final TransactionOptions transactionOptions,
      final Transaction oldTransaction) {
    final long jtxn_handle = beginTransaction_withOld(nativeHandle_,
        writeOptions.nativeHandle_, transactionOptions.nativeHandle_,
        oldTransaction.nativeHandle_);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(jtxn_handle == oldTransaction.nativeHandle_);

    return oldTransaction;
  }

  public Transaction getTransactionByName(final String transactionName) {
    final long jtxnHandle = getTransactionByName(nativeHandle_, transactionName);
    if(jtxnHandle == 0) {
      return null;
    }

    final Transaction txn = new Transaction(this, jtxnHandle);

    // this instance doesn't own the underlying C++ object
    txn.disOwnNativeHandle();

    return txn;
  }

  public List<Transaction> getAllPreparedTransactions() {
    final long[] jtxnHandles = getAllPreparedTransactions(nativeHandle_);

    final List<Transaction> txns = new ArrayList<>();
    for(final long jtxnHandle : jtxnHandles) {
      final Transaction txn = new Transaction(this, jtxnHandle);

      // this instance doesn't own the underlying C++ object
      txn.disOwnNativeHandle();

      txns.add(txn);
    }
    return txns;
  }

  private void storeTransactionDbOptions(
      final TransactionDBOptions transactionDbOptions) {
    this.transactionDbOptions_ = transactionDbOptions;
  }

  private static native long open(final long optionsHandle,
      final long transactionDbOptionsHandle, final String path)
      throws RocksDBException;
  private static native long[] open(final long dbOptionsHandle,
      final long transactionDbOptionsHandle, final String path,
      final byte[][] columnFamilyNames, final long[] columnFamilyOptions);
  private native long beginTransaction(final long handle,
      final long writeOptionsHandle);
  private native long beginTransaction(final long handle,
      final long writeOptionsHandle, final long transactionOptionsHandle);
  private native long beginTransaction_withOld(final long handle,
      final long writeOptionsHandle, final long oldTransactionHandle);
  private native long beginTransaction_withOld(final long handle,
      final long writeOptionsHandle, final long transactionOptionsHandle,
      final long oldTransactionHandle);
  private native long getTransactionByName(final long handle,
      final String name);
  private native long[] getAllPreparedTransactions(final long handle);
  @Override protected final native void disposeInternal(final long handle);
}