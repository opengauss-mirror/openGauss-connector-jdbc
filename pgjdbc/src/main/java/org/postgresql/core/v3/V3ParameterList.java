/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
