// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionDBOptionsTest {

  private static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void maxNumLocks() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxNumLocks(longValue);
      assertThat(opt.getMaxNumLocks()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxNumStripes() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setNumStripes(longValue);
      assertThat(opt.getNumStripes()).isEqualTo(longValue);
    }
  }

  @Test
  public void transactionLockTimeout() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setTransactionLockTimeout(longValue);
      assertThat(opt.getTransactionLockTimeout()).isEqualTo(longValue);
    }
  }

  @Test
  public void defaultLockTimeout() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setDefaultLockTimeout(longValue);
      assertThat(opt.getDefaultLockTimeout()).isEqualTo(longValue);
    }
  }

}