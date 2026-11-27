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
