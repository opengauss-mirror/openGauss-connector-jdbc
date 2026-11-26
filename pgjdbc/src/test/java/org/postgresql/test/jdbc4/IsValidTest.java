/*
 * Copyright (c) 2007, PostgreSQL Global Development Group
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

package org.postgresql.test.jdbc4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class IsValidTest extends BaseTest4 {

  private TransactionState getTransactionState(Connection conn) {
    return ((BaseConnection) conn).getTransactionState();
  }

  @Test
  public void testIsValid() throws Exception {
    try {
      assertTrue(con.isValid(0));
    } finally {
      TestUtil.closeDB(con);
    }
    assertFalse(con.isValid(0));
  }

  /**
   * Test that the transaction state is left unchanged.
   */
  @Test
  public void testTransactionState() throws Exception {
    TransactionState transactionState = getTransactionState(con);
    con.isValid(0);
    assertEquals("Transaction state has been changed", transactionState,
        getTransactionState(con));

    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    con.setAutoCommit(false);
    try {
      transactionState = getTransactionState(con);
      con.isValid(0);
      assertEquals("Transaction state has been changed", transactionState,
          getTransactionState(con));

      Statement stmt = con.createStatement();
      stmt.execute("SELECT 1");
      transactionState = getTransactionState(con);
      con.isValid(0);
      assertEquals("Transaction state has been changed", transactionState,
          getTransactionState(con));
    } finally {
      try {
        con.setAutoCommit(true);
      } catch (final Exception e) {
      }
    }
  }

}
