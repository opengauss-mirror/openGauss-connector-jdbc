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

import java.sql.SQLException;

/**
 * Fluent interface for specify common parameters for create Logical and Physical replication slot.
 */
public interface ChainedCommonCreateSlotBuilder<T extends ChainedCommonCreateSlotBuilder<T>>  {

  /**
   * Replication slots provide an automated way to ensure that the master does not remove WAL
   * segments until they have been received by all standbys, and that the master does not remove
   * rows which could cause a recovery conflict even when the standby is disconnected.
   *
   * @param slotName not null unique replication slot name for create.
   * @return T a slot builder
   */
  T withSlotName(String slotName);

  /**
   * Create slot with specified parameters in database.
   * @throws SQLException on error
   */
  void make() throws SQLException;
}
