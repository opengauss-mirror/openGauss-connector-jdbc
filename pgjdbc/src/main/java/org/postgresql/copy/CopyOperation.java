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

/**
 * Exchange bulk data between client and PostgreSQL database tables. See CopyIn and CopyOut for full
 * interfaces for corresponding copy directions.
 */
public interface CopyOperation {

  /**
   * @return number of fields in each row for this operation
   */
  int getFieldCount();

  /**
   * @return overall format of each row: 0 = textual, 1 = binary
   */
  int getFormat();

  /**
   * @param field number of field (0..fieldCount()-1)
   * @return format of requested field: 0 = textual, 1 = binary
   */
  int getFieldFormat(int field);

  /**
   * @return is connection reserved for this Copy operation?
   */
  boolean isActive();

  /**
   * Cancels this copy operation, discarding any exchanged data.
   *
   * @throws SQLException if cancelling fails
   */
  void cancelCopy() throws SQLException;

  /**
   * After successful end of copy, returns the number of database records handled in that operation.
   * Only implemented in PostgreSQL server version 8.2 and up. Otherwise, returns -1.
   *
   * @return number of handled rows or -1
   */
  long getHandledRowCount();
}
