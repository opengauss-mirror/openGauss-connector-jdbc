/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
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


package org.postgresql.test.jdbc2;

import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

public class ResultSetRefreshTest extends BaseTest4 {
    
    @Test
    public void testWithDataColumnThatRequiresEscaping() throws Exception {
        TestUtil.dropTable(con, "refresh_row_bad_ident");
        TestUtil.execute("CREATE TABLE refresh_row_bad_ident (id int PRIMARY KEY, \"1 FROM refresh_row_bad_ident; SELECT 2; SELECT *\" int)", con);
        TestUtil.execute("INSERT INTO refresh_row_bad_ident (id) VALUES (1), (2), (3)", con);
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM refresh_row_bad_ident");
        assertTrue(rs.next());
        try {
            rs.refreshRow();
        } catch (SQLException ex) {
            throw new RuntimeException("ResultSet.refreshRow() did not handle escaping data column identifiers", ex);
        }
        rs.close();
        stmt.close();
    }
    
    @Test
    public void testWithKeyColumnThatRequiresEscaping() throws Exception {
        TestUtil.dropTable(con, "refresh_row_bad_ident");
        TestUtil.execute("CREATE TABLE refresh_row_bad_ident (\"my key\" int PRIMARY KEY)", con);
        TestUtil.execute("INSERT INTO refresh_row_bad_ident VALUES (1), (2), (3)", con);
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM refresh_row_bad_ident");
        assertTrue(rs.next());
        try {
            rs.refreshRow();
        } catch (SQLException ex) {
            throw new RuntimeException("ResultSet.refreshRow() did not handle escaping key column identifiers", ex);
        }
        rs.close();
        stmt.close();
    }
}