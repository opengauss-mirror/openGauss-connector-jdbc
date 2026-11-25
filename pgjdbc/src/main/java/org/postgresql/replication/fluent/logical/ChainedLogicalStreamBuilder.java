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

package org.postgresql.replication.fluent.logical;

import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.ChainedCommonStreamBuilder;

import java.sql.SQLException;
import java.util.Properties;

public interface ChainedLogicalStreamBuilder
    extends ChainedCommonStreamBuilder<ChainedLogicalStreamBuilder> {
  /**
   * Open logical replication stream.
   *
   * @return not null PGReplicationStream available for fetch data in logical form
   * @throws SQLException  if there are errors
   */
  PGReplicationStream start() throws SQLException;

  /**
   *
   * @param optionName name of option
   * @param optionValue boolean value
   * @return ChainedLogicalStreamBuilder
   */

  ChainedLogicalStreamBuilder withSlotOption(String optionName, boolean optionValue);

  /**
   *
   * @param optionName name of option
   * @param optionValue integer value
   * @return ChainedLogicalStreamBuilder
   */
  ChainedLogicalStreamBuilder withSlotOption(String optionName, int optionValue);

  /**
   *
   * @param optionName name of option
   * @param optionValue String value
   * @return ChainedLogicalStreamBuilder
   */
  ChainedLogicalStreamBuilder withSlotOption(String optionName, String optionValue);

  /**
   *
   * @param options properties
   * @return ChainedLogicalStreamBuilder
   */
  ChainedLogicalStreamBuilder withSlotOptions(Properties options);

}
