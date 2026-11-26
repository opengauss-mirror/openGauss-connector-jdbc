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
