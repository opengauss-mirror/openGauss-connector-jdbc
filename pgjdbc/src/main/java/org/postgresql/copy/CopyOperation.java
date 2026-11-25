/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
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
