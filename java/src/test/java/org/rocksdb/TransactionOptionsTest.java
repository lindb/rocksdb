// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionOptionsTest {

  private static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void setSnapshot() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setSetSnapshot(boolValue);
      assertThat(opt.isSetSnapshot()).isEqualTo(boolValue);
    }
  }

  @Test
  public void lockTimeout() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final long longValue = rand.nextLong();
      opt.setLockTimeout(longValue);
      assertThat(opt.getLockTimeout()).isEqualTo(longValue);
    }
  }

  @Test
  public void expiration() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final long longValue = rand.nextLong();
      opt.setExpiration(longValue);
      assertThat(opt.getExpiration()).isEqualTo(longValue);
    }
  }
}