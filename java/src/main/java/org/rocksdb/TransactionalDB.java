// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;


interface TransactionalDB<T extends TransactionalOptions>
    extends AutoCloseable {

  /**
   * Starts a new Transaction.
   *
   * Caller is responsible for calling {@link #close()} on the returned
   * transaction when it is no longer needed.
   *
   * @param writeOptions Any write options for the transaction
   * @return a new transaction
   */
  Transaction beginTransaction(final WriteOptions writeOptions);

  /**
   * Starts a new Transaction.
   *
   * Caller is responsible for calling {@link #close()} on the returned
   * transaction when it is no longer needed.
   *
   * @param writeOptions Any write options for the transaction
   * @param transactionOptions Any options for the transaction
   * @return a new transaction
   */
  Transaction beginTransaction(final WriteOptions writeOptions,
      final T transactionOptions);

  /**
   * Starts a new Transaction.
   *
   * Caller is responsible for calling {@link #close()} on the returned
   * transaction when it is no longer needed.
   *
   * @param writeOptions Any write options for the transaction
   * @param oldTransaction this Transaction will be reused instead of allocating
   *     a new one. This is an optimization to avoid extra allocations
   *     when repeatedly creating transactions.
   * @return The oldTransaction which has been reinitialized as a new
   *     transaction
   */
  Transaction beginTransaction(final WriteOptions writeOptions,
      final Transaction oldTransaction);

  /**
   * Starts a new Transaction.
   *
   * Caller is responsible for calling {@link #close()} on the returned
   * transaction when it is no longer needed.
   *
   * @param writeOptions Any write options for the transaction
   * @param transactionOptions Any options for the transaction
   * @param oldTransaction this Transaction will be reused instead of allocating
   *     a new one. This is an optimization to avoid extra allocations
   *     when repeatedly creating transactions.
   * @return The oldTransaction which has been reinitialized as a new
   *     transaction
   */
  Transaction beginTransaction(final WriteOptions writeOptions,
      final T transactionOptions, final Transaction oldTransaction);
}