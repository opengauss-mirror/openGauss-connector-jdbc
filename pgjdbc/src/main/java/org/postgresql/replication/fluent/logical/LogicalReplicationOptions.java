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

import org.postgresql.replication.fluent.CommonOptions;

import java.util.Properties;

public interface LogicalReplicationOptions extends CommonOptions {
  /**
   * Required parameter for logical replication.
   *
   * @return not null logical replication slot name that already exists on server and free.
   */
  String getSlotName();

  /**
   * Parameters for output plugin. Parameters will be set to output plugin that register for
   * specified replication slot name.
   *
   * @return list options that will be pass to output_plugin for that was create replication slot
   */
  Properties getSlotOptions();
}
