/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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

import static org.junit.Assert.assertTrue;

import org.postgresql.test.TestUtil;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


public class PSQLWarningTest {

  @Test
  public void testPSQLLogsToDriverManagerMessage() throws Exception {
    Connection con = TestUtil.openDB();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DriverManager.setLogWriter(new PrintWriter(new OutputStreamWriter(baos, "ASCII")));

    Statement stmt = con.createStatement();
    stmt.execute("DO language plpgsql $$ BEGIN RAISE NOTICE 'test notice'; END $$;");
    assertTrue(baos.toString().contains("NOTICE: test notice"));

    stmt.close();
    con.close();
  }
}
