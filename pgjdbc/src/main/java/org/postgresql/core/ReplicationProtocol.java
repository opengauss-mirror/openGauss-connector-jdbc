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

package org.postgresql.core;

import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.LogicalReplicationOptions;
import org.postgresql.replication.fluent.physical.PhysicalReplicationOptions;

import java.sql.SQLException;

/**
 * <p>Abstracts the protocol-specific details of physic and logic replication.</p>
 *
 * <p>With each connection open with replication options associate own instance ReplicationProtocol.</p>
 */
public interface ReplicationProtocol {
  /**
   * @param options not null options for logical replication stream
   * @return not null stream instance from which available fetch wal logs that was decode by output
   *     plugin
   * @throws SQLException on error
   */
  PGReplicationStream startLogical(LogicalReplicationOptions options) throws SQLException;

  /**
   * @param options not null options for physical replication stream
   * @return not null stream instance from which available fetch wal logs
   * @throws SQLException on error
   */
  PGReplicationStream startPhysical(PhysicalReplicationOptions options) throws SQLException;
}
