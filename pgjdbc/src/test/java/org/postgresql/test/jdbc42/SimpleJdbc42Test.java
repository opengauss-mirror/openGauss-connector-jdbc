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

package org.postgresql.test.jdbc42;

import static org.junit.Assert.assertTrue;

import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Test;

/**
 * Most basic test to check that the right package is compiled.
 */
public class SimpleJdbc42Test extends BaseTest4 {

  /**
   * Test presence of JDBC 4.2 specific methods.
   */
  @Test
  public void testSupportsRefCursors() throws Exception {
    assertTrue(con.getMetaData().supportsRefCursors());
  }
}
