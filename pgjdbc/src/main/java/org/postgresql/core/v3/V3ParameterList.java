/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core.v3;

import org.postgresql.core.ParameterList;

import java.sql.SQLException;

/**
 * Common interface for all V3 parameter list implementations.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
interface V3ParameterList extends ParameterList {
  /**
   * Ensure that all parameters in this list have been assigned values. Return silently if all is
   * well, otherwise throw an appropriate exception.
   *
   * @throws SQLException if not all parameters are set.
   */
  void checkAllParametersSet() throws SQLException;

  /**
   * Convert any function output parameters to the correct type (void) and set an ignorable value
   * for it.
   */
  void convertFunctionOutParameters();

  /**
   * Return a list of the SimpleParameterList objects that make up this parameter list. If this
   * object is already a SimpleParameterList, returns null (avoids an extra array construction in
   * the common case).
   *
   * @return an array of single-statement parameter lists, or <code>null</code> if this object is
   *         already a single-statement parameter list.
   */
  SimpleParameterList[] getSubparams();

  /**
   * Return the parameter type information.
   * @return an array of {@link org.postgresql.core.Oid} type information
   */
  int[] getParamTypes();

  /**
   * Return the flags for each parameter.
   * @return an array of bytes used to store flags.
   */
  byte[] getFlags();

  /**
   * Return the encoding for each parameter.
   * @return nested byte array of bytes with encoding information.
   */
  byte[][] getEncoding();
}
