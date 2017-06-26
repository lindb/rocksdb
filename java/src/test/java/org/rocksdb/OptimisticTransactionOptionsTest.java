// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import org.junit.Test;
import org.rocksdb.util.DirectBytewiseComparator;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class OptimisticTransactionOptionsTest {

  private static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void setSnapshot() {
    try (final OptimisticTransactionOptions opt = new OptimisticTransactionOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setSetSnapshot(boolValue);
      assertThat(opt.isSetSnapshot()).isEqualTo(boolValue);
    }
  }

  @Test
  public void comparator() {
    try (final OptimisticTransactionOptions opt = new OptimisticTransactionOptions();
         final ComparatorOptions copt = new ComparatorOptions();
         final DirectComparator comparator = new DirectBytewiseComparator(copt)) {
      opt.setComparator(comparator);
    }
  }
}