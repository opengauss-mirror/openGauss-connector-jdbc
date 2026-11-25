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

package org.postgresql.replication;

import org.postgresql.PGProperty;
import org.postgresql.replication.fluent.ChainedCreateReplicationSlotBuilder;
import org.postgresql.replication.fluent.ChainedStreamBuilder;

import java.sql.SQLException;

/**
 * Api available only if connection was create with required for replication properties: {@link
 * PGProperty#REPLICATION} and {@link PGProperty#ASSUME_MIN_SERVER_VERSION}. Without it property
 * building replication stream fail with exception.
 */
public interface PGReplicationConnection {

  /**
   * After start replication stream this connection not available to use for another queries until
   * replication stream will not close.
   *
   * @return not null fluent api for build replication stream
   */
  ChainedStreamBuilder replicationStream();

  /**
   * <p>Create replication slot, that can be next use in {@link PGReplicationConnection#replicationStream()}</p>
   *
   * <p>Replication slots provide an automated way to ensure that the master does not remove WAL
   * segments until they have been received by all standbys, and that the master does not remove
   * rows which could cause a recovery conflict even when the standby is disconnected.</p>
   *
   * @return not null fluent api for build create replication slot
   */
  ChainedCreateReplicationSlotBuilder createReplicationSlot();

  /**
   * @param slotName not null replication slot name exists in database that should be drop
   * @throws SQLException if the replication slot cannot be dropped.
   */
  void dropReplicationSlot(String slotName) throws SQLException;
}
