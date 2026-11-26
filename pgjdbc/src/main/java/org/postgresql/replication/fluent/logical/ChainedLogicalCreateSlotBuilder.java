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

import org.postgresql.replication.fluent.ChainedCommonCreateSlotBuilder;

/**
 * Logical replication slot specific parameters.
 */
public interface ChainedLogicalCreateSlotBuilder
    extends ChainedCommonCreateSlotBuilder<ChainedLogicalCreateSlotBuilder> {

  /**
   * <p>Output plugin that should be use for decode physical represent WAL to some logical form.
   * Output plugin should be installed on server(exists in shared_preload_libraries).</p>
   *
   * <p>Package postgresql-contrib provides sample output plugin <b>test_decoding</b> that can be
   * use for test logical replication api</p>
   *
   * @param outputPlugin not null name of the output plugin used for logical decoding
   * @return the logical slot builder
   */
  ChainedLogicalCreateSlotBuilder withOutputPlugin(String outputPlugin);
}
