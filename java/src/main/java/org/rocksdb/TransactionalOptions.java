// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;


interface TransactionalOptions extends AutoCloseable {

  /**
   * True indicates snapshots will be set, just like if
   * {@link Transaction#setSnapshot()} had been called
   *
   * @return whether a snapshot will be set
   */
  boolean isSetSnapshot();

  /**
   * Setting the setSnapshot to true is the same as calling
   * {@link Transaction#setSnapshot()}
   *
   * Default: false
   *
   * @param setSnapshot Whether to set a snapshot
   *
   * @return this TransactionalOptions instance
   */
  <T extends TransactionalOptions> T setSetSnapshot(final boolean setSnapshot);
}