/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.postgresql.replication.fluent;

import org.postgresql.replication.LogSequenceNumber;

import java.util.concurrent.TimeUnit;

public abstract class AbstractStreamBuilder<T extends ChainedCommonStreamBuilder<T>>
    implements ChainedCommonStreamBuilder<T> {
  private static final int DEFAULT_STATUS_INTERVAL = (int) TimeUnit.SECONDS.toMillis(10L);
  protected int statusIntervalMs = DEFAULT_STATUS_INTERVAL;
  protected LogSequenceNumber startPosition = LogSequenceNumber.INVALID_LSN;
  protected String slotName;

  protected abstract T self();

  @Override
  public T withStatusInterval(int time, TimeUnit format) {
    statusIntervalMs = (int) TimeUnit.MILLISECONDS.convert(time, format);
    return self();
  }

  @Override
  public T withStartPosition(LogSequenceNumber lsn) {
    this.startPosition = lsn;
    return self();
  }

  @Override
  public T withSlotName(String slotName) {
    this.slotName = slotName;
    return self();
  }
}
