/*
 * Copyright (c) 2011, PostgreSQL Global Development Group
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

package org.postgresql.util;

import java.sql.SQLException;

/**
 * PGBinaryObject is a inteface that classes extending {@link PGobject} can use to take advantage of
 * more optimal binary encoding of the data type.
 */
public interface PGBinaryObject {
  /**
   * This method is called to set the value of this object.
   *
   * @param value data containing the binary representation of the value of the object
   * @param offset the offset in the byte array where object data starts
   * @throws SQLException thrown if value is invalid for this type
   */
  void setByteValue(byte[] value, int offset) throws SQLException;

  /**
   * This method is called to return the number of bytes needed to store this object in the binary
   * form required by org.postgresql.
   *
   * @return the number of bytes needed to store this object
   */
  int lengthInBytes();

  /**
   * This method is called the to store the value of the object, in the binary form required by
   * org.postgresql.
   *
   * @param bytes the array to store the value, it is guaranteed to be at lest
   *        {@link #lengthInBytes} in size.
   * @param offset the offset in the byte array where object must be stored
   */
  void toBytes(byte[] bytes, int offset);
}
