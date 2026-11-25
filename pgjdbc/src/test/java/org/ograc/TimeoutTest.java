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

import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.Driver;
import org.postgresql.core.ORStream;
import org.postgresql.jdbc.ORConnection;

import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test timeout
 *
 * @author zhangting
 * @since  2026-10-13
 */
public class TimeoutTest {
    private static final String UNREACHABLE_HOST = "10.255.255.1";

    @BeforeClass
    public static void initDriver() throws Exception {
        TestUtil.init();
    }

    @Test
    public void testParseTimeoutUrlParameters() throws Exception {
        Properties props = Driver.parseURL(
                "jdbc:oGRAC://127.0.0.1:10029?loginTimeout=10000&socketTimeout=30&connectTimeout=5",
                new Properties());
        assertNotNull(props);
        assertEquals("10000", props.getProperty("loginTimeout"));
        assertEquals("30", props.getProperty("socketTimeout"));
        assertEquals("5", props.getProperty("connectTimeout"));
    }

    @Test
    public void testLoginTimeoutAllowsSuccessfulConnection() throws Exception {
        try (Connection conn = TestUtil.openDB("loginTimeout=10000")) {
            assertTrue(conn.isValid(5));
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testLoginTimeoutUnreachableHostFailsWithinLimit() throws Exception {
        final int loginTimeoutMs = 2000;
        final long start = System.currentTimeMillis();
        String url = TestUtil.URL_PREFIX + UNREACHABLE_HOST + ":5432?loginTimeout=" + loginTimeoutMs;
        try {
            DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword());
            fail("expected SQLException for unreachable host");
        } catch (SQLException e) {
            long elapsed = System.currentTimeMillis() - start;
            assertTrue("connection should fail, elapsed=" + elapsed, elapsed < 15000);
            assertTrue("expected connect/timeout related failure: " + e, isTimeoutRelatedFailure(e));
        }
    }

    @Test
    public void testSocketTimeoutAppliedToSocket() throws Exception {
        try (Connection conn = TestUtil.openDB("socketTimeout=15")) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertNotNull(stream);
            assertNotNull(stream.getSocket());
            assertEquals(15000, stream.getSocket().getSoTimeout());
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testSocketTimeoutZero() throws Exception {
        try (Connection conn = TestUtil.openDB("socketTimeout=0")) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertNotNull(stream.getSocket());
            assertEquals(0, stream.getSocket().getSoTimeout());
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testSocketTimeoutAllowsQuery() throws Exception {
        try (Connection conn = TestUtil.openDB("socketTimeout=60")) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("select 2 as c")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    public void testConnectTimeoutPropertyOnSuccessfulConnection() throws Exception {
        try (Connection conn = TestUtil.openDB("connectTimeout=5&loginTimeout=10000")) {
            assertTrue(conn.isValid(5));
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testAllTimeoutPropertiesViaPropertiesObject() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", TestUtil.getUser());
        props.setProperty("password", TestUtil.getPassword());
        props.setProperty("loginTimeout", "10000");
        props.setProperty("socketTimeout", "20");
        props.setProperty("connectTimeout", "8");
        try (Connection conn = TestUtil.openDB(props)) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertEquals(20000, stream.getSocket().getSoTimeout());
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testCombinedTimeoutUrlParameters() throws Exception {
        String query = "loginTimeout=10000&socketTimeout=25&connectTimeout=10";
        try (Connection conn = TestUtil.openDB(query)) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertEquals(25000, stream.getSocket().getSoTimeout());
            assertTrue(conn.isValid(3));
        }
    }

    @Test
    public void testStatementWorksWithSocketTimeout() throws Exception {
        try (Connection conn = TestUtil.openDB("socketTimeout=45&loginTimeout=10000")) {
            try (Statement st = conn.createStatement()) {
                assertTrue(st.execute("select 1"));
                try (ResultSet rs = st.getResultSet()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            }
        }
    }

    private static boolean isTimeoutRelatedFailure(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof SocketTimeoutException) {
                return true;
            }
            String message = cause.getMessage();
            if (message != null) {
                String lowerMessage = message.toLowerCase(Locale.ROOT);
                if (lowerMessage.contains("timeout")) {
                    return true;
                }
                if (lowerMessage.contains("refused")) {
                    return true;
                }
                if (lowerMessage.contains("connect")) {
                    return true;
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static int queryOne(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select 1")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
