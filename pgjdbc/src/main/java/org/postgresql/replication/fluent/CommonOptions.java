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

import org.postgresql.replication.LogSequenceNumber;

/**
 * Common parameters for logical and physical replication.
 */
public interface CommonOptions {
  /**
   * Replication slots provide an automated way to ensure that the master does not remove WAL
   * segments until they have been received by all standbys, and that the master does not remove
   * rows which could cause a recovery conflict even when the standby is disconnected.
   *
   * @return nullable replication slot name that already exists on server and free.
   */
  String getSlotName();

  /**
   * @return the position to start replication. This cannot be null.
   */
  LogSequenceNumber getStartLSNPosition();

  /**
   * Specifies the number of millisecond between status packets sent back to the server. This allows
   * for easier monitoring of the progress from server. A value of zero disables the periodic status
   * updates completely, although an update will still be sent when requested by the server, to
   * avoid timeout disconnect. The default value is 10 seconds.
   *
   * @return the current status interval
   */
  int getStatusInterval();
}
