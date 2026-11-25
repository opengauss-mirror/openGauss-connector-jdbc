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

package org.postgresql.replication.fluent;

import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.postgresql.replication.fluent.physical.ChainedPhysicalStreamBuilder;

/**
 * Start point for fluent API that build replication stream(logical or physical).
 * Api not thread safe, and can be use only for crate single stream.
 */
public interface ChainedStreamBuilder {
  /**
   * <p>Create logical replication stream that decode raw wal logs by output plugin to logical form.
   * Default about logical decoding you can see by following link
   * <a href="http://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html">
   *     Logical Decoding Concepts
   * </a>.
   * </p>
   *
   * <p>Example usage:</p>
   * <pre>
   *   {@code
   *
   *    PGReplicationStream stream =
   *        pgConnection
   *            .getReplicationAPI()
   *            .replicationStream()
   *            .logical()
   *            .withSlotName("test_decoding")
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
   *
   * @return not null fluent api
   */
  ChainedLogicalStreamBuilder logical();

  /**
   * <p>Create physical replication stream for process wal logs in binary form.</p>
   *
   * <p>Example usage:</p>
   * <pre>
   *   {@code
   *
   *    LogSequenceNumber lsn = getCurrentLSN();
   *
   *    PGReplicationStream stream =
   *        pgConnection
   *            .getReplicationAPI()
   *            .replicationStream()
   *            .physical()
   *            .withStartPosition(lsn)
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
  ChainedPhysicalStreamBuilder physical();
}
