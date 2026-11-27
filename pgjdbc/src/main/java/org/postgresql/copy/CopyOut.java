/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
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

package org.postgresql.copy;

import java.sql.SQLException;

public interface CopyOut extends CopyOperation {
  /**
   * Blocks wait for a row of data to be received from server on an active copy operation.
   *
   * @return byte array received from server, null if server complete copy operation
   * @throws SQLException if something goes wrong for example socket timeout
   */
  byte[] readFromCopy() throws SQLException;

  /**
   * Wait for a row of data to be received from server on an active copy operation.
   *
   * @param block {@code true} if need wait data from server otherwise {@code false} and will read
   *              pending message from server
   * @return byte array received from server, if pending message from server absent and use no
   * blocking mode return null
   * @throws SQLException if something goes wrong for example socket timeout
   */
  byte[] readFromCopy(boolean block) throws SQLException;
}
