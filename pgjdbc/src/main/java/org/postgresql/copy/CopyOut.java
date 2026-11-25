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
