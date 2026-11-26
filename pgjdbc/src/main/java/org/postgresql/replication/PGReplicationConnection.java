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
