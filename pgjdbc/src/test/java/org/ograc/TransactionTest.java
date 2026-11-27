/*
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

package org.ograc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * test transaction
 *
 * @author zhangting
 * @since  2026-10-13
 */
public class TransactionTest {
    private static final String TABLE = "ograc_txn_t";

    private Connection conn;
    private Statement stmt;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        conn.setAutoCommit(false);
        stmt = conn.createStatement();
        TestUtil.dropTable(conn, TABLE);
        stmt.execute("create table " + TABLE + "(c1 int, c2 bigint)");
        conn.commit();
    }

    @After
    public void tearDown() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.setAutoCommit(true);
                TestUtil.dropTable(conn, TABLE);
            }
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(stmt);
        TestUtil.closeQuietly(conn);
    }

    @Test
    public void testCommitPersistsData() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 2)");
        conn.commit();
        try (ResultSet rs = stmt.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testRollbackDiscardsData() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 2)");
        conn.rollback();
        try (ResultSet rs = stmt.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    public void testRollbackAfterMultipleInserts() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 1)");
        stmt.executeUpdate("insert into " + TABLE + " values(2, 2)");
        stmt.executeUpdate("insert into " + TABLE + " values(3, 3)");
        conn.rollback();
        try (ResultSet rs = stmt.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    public void testCommitThenRollbackOnlyAffectsNewWork() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 1)");
        conn.commit();
        stmt.executeUpdate("insert into " + TABLE + " values(2, 2)");
        conn.rollback();
        try (ResultSet rs = stmt.executeQuery("select c1 from " + TABLE + " order by c1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }
}
