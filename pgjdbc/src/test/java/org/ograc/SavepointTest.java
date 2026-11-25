/*
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

package org.ograc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test savepoint
 *
 * @author zhangting
 * @since  2026-09-23
 */
public class SavepointTest {
    private static final String TABLE = "ograc_sv_t";

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
        conn.setAutoCommit(false);
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
    public void testUnnamedSavepointRollback() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 2)");
        stmt.executeUpdate("insert into " + TABLE + " values(2, 2)");
        conn.setSavepoint();
        stmt.executeUpdate("insert into " + TABLE + " values(3, 2)");
        Savepoint sv1 = conn.setSavepoint();
        stmt.executeUpdate("insert into " + TABLE + " values(4, 2)");
        stmt.executeUpdate("insert into " + TABLE + " values(5, 2)");
        conn.rollback(sv1);
        conn.commit();
        assertEquals(3, countRows());
    }

    @Test
    public void testNamedSavepointRollback() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 2)");
        stmt.executeUpdate("insert into " + TABLE + " values(2, 2)");
        conn.setSavepoint("test");
        stmt.executeUpdate("insert into " + TABLE + " values(3, 2)");
        conn.setSavepoint("test1 test");
        stmt.executeUpdate("insert into " + TABLE + " values(4, 2)");
        Savepoint sv2 = conn.setSavepoint("test");
        stmt.executeUpdate("insert into " + TABLE + " values(5, 2)");
        conn.rollback(sv2);
        conn.commit();
        assertEquals(4, countRows());
    }

    @Test
    public void testReleaseSavepoint() throws SQLException {
        stmt.executeUpdate("insert into " + TABLE + " values(1, 2)");
        stmt.executeUpdate("insert into " + TABLE + " values(2, 2)");
        conn.setSavepoint("p1");
        stmt.executeUpdate("insert into " + TABLE + " values(3, 2)");
        conn.setSavepoint("p2");
        stmt.executeUpdate("insert into " + TABLE + " values(4, 2)");
        Savepoint sv2 = conn.setSavepoint("p3");
        stmt.executeUpdate("insert into " + TABLE + " values(5, 2)");
        conn.releaseSavepoint(sv2);
        conn.commit();
        assertEquals(5, countRows());
    }

    @Test
    public void testMultipleNamedSavepointsThenCommit() throws SQLException {
        for (int i = 0; i < 8; i++) {
            stmt.executeUpdate("insert into " + TABLE + " values(" + i + ", 2)");
            conn.setSavepoint("test" + i);
        }
        conn.commit();
        assertEquals(8, countRows());
    }

    private int countRows() throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
