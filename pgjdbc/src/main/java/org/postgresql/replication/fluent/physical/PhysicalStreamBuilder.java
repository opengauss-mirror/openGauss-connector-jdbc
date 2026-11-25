/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.postgresql.replication.fluent.physical;

import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.AbstractStreamBuilder;

import java.sql.SQLException;

public class PhysicalStreamBuilder extends AbstractStreamBuilder<ChainedPhysicalStreamBuilder>
    implements ChainedPhysicalStreamBuilder, PhysicalReplicationOptions {

  private final StartPhysicalReplicationCallback startCallback;

  /**
   * @param startCallback not null callback that should be execute after build parameters for start
   *                      replication
   */
  public PhysicalStreamBuilder(StartPhysicalReplicationCallback startCallback) {
    this.startCallback = startCallback;
  }

  @Override
  protected ChainedPhysicalStreamBuilder self() {
    return this;
  }

  @Override
  public PGReplicationStream start() throws SQLException {
    return this.startCallback.start(this);
  }

  @Override
  public String getSlotName() {
    return slotName;
  }

  @Override
  public LogSequenceNumber getStartLSNPosition() {
    return startPosition;
  }

  @Override
  public int getStatusInterval() {
    return statusIntervalMs;
  }
}
