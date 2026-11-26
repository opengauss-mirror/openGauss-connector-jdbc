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
