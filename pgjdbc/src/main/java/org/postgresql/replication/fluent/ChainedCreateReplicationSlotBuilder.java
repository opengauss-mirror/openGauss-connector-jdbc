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

import org.postgresql.replication.fluent.logical.ChainedLogicalCreateSlotBuilder;
import org.postgresql.replication.fluent.physical.ChainedPhysicalCreateSlotBuilder;

/**
 * Fluent interface for specify common parameters for Logical and Physical replication.
 */
public interface ChainedCreateReplicationSlotBuilder {
  /**
   * Get the logical slot builder.
   * Example usage:
   * <pre>
   *   {@code
   *
   *    pgConnection
   *        .getReplicationAPI()
   *        .createReplicationSlot()
   *        .logical()
   *        .withSlotName("mySlot")
   *        .withOutputPlugin("test_decoding")
   *        .make();
   *
   *    PGReplicationStream stream =
   *        pgConnection
   *            .getReplicationAPI()
   *            .replicationStream()
   *            .logical()
   *            .withSlotName("mySlot")
   *            .withSlotOption("include-xids", false)
   *            .withSlotOption("skip-empty-xacts", true)
   *            .start();
   *
   *    while (true) {
   *      ByteBuffer buffer = stream.read();
   *      //process logical changes
   *    }
   *
   *   }
   * </pre>
   * @return not null fluent api
   */
  ChainedLogicalCreateSlotBuilder logical();

  /**
   * <p>Create physical replication stream for process wal logs in binary form.</p>
   *
   * <p>Example usage:</p>
   * <pre>
   *   {@code
   *
   *    pgConnection
   *        .getReplicationAPI()
   *        .createReplicationSlot()
   *        .physical()
   *        .withSlotName("mySlot")
   *        .make();
   *
   *    PGReplicationStream stream =
   *        pgConnection
   *            .getReplicationAPI()
   *            .replicationStream()
   *            .physical()
   *            .withSlotName("mySlot")
   *            .start();
   *
   *    while (true) {
   *      ByteBuffer buffer = stream.read();
   *      //process binary WAL logs
   *    }
   *
   *   }
   * </pre>
   *
   * @return not null fluent api
   */
  ChainedPhysicalCreateSlotBuilder physical();
}
