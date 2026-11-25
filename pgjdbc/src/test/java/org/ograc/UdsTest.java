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
import org.postgresql.core.ORStream;
import org.postgresql.jdbc.ORConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * test uds connection
 *
 * @author zhangting
 * @since  2026-10-13
 */
public class UdsTest {
    private static final String SOCKET_FACTORY = OgracUdsSocketFactory.class.getName();
    private static final String SOCKET_FACTORY_ARG = "ograc-uds-conn";

    @Before
    public void setUp() throws Exception {
        TestUtil.init();
        OgracUdsSocketFactory.reset();
    }

    @After
    public void tearDown() {
        OgracUdsSocketFactory.reset();
    }

    @Test
    public void testUdsConnectionOpensSuccessfully() throws Exception {
        try (Connection conn = openUdsConnection()) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }
    }

    @Test
    public void testUdsConnectionIsORConnection() throws Exception {
        try (Connection conn = openUdsConnection()) {
            assertTrue(conn instanceof ORConnection);
        }
    }

    @Test
    public void testUdsConnectionUsesCustomSocketFactory() throws Exception {
        try (Connection conn = openUdsConnection()) {
            assertNotNull(conn);
            OgracUdsSocketFactory factory = OgracUdsSocketFactory.getInstance();
            assertNotNull(factory);
            assertEquals(SOCKET_FACTORY_ARG, factory.getArgument());
            assertTrue(factory.getSocketCreated() >= 1);
        }
    }

    @Test
    public void testUdsConnectionHasOrStream() throws Exception {
        try (Connection conn = openUdsConnection()) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertNotNull(stream);
            assertTrue(conn.isValid(5));
        }
    }

    @Test
    public void testUdsConnectionExecuteQuery() throws Exception {
        try (Connection conn = openUdsConnection();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select 1 as c")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt("c"));
        }
    }

    @Test
    public void testUdsConnectionPreparedStatement() throws Exception {
        try (Connection conn = openUdsConnection();
             PreparedStatement ps = conn.prepareStatement("select cast(? as int) as c")) {
            ps.setInt(1, 42);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
        }
    }

    @Test
    public void testUdsConnectionIsValid() throws Exception {
        try (Connection conn = openUdsConnection()) {
            assertTrue(conn.isValid(5));
        }
    }

    @Test
    public void testUdsConnectionCloseAndReconnect() throws Exception {
        Connection conn = openUdsConnection();
        conn.close();
        assertTrue(conn.isClosed());
        try (Connection conn2 = openUdsConnection()) {
            assertTrue(conn2.isValid(5));
            assertEquals(1, queryOne(conn2));
        }
    }

    @Test
    public void testUdsConnectionWithExplicitUdsQueryParameter() throws Exception {
        String url = TestUtil.URL_PREFIX + "127.0.0.1?"
                + "UDS=1"
                + "&PGHOST=" + TestUtil.getServer()
                + "&PGPORT=" + TestUtil.getPort()
                + "&loginTimeout=10000"
                + "&socketFactory=" + SOCKET_FACTORY
                + "&socketFactoryArg=" + SOCKET_FACTORY_ARG;
        try (Connection conn = DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword())) {
            assertTrue(conn instanceof ORConnection);
            assertEquals(1, queryOne(conn));
            assertNotNull(OgracUdsSocketFactory.getInstance());
        }
    }

    @Test
    public void testUdsConnectionWithoutCustomSocketFactory() throws Exception {
        String url = TestUtil.URL_PREFIX + "localhost?"
                + "PGHOST=" + TestUtil.getServer()
                + "&PGPORT=" + TestUtil.getPort()
                + "&loginTimeout=10000";
        try (Connection conn = DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword())) {
            assertTrue(conn instanceof ORConnection);
            assertTrue(conn.isValid(5));
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testUdsLocalhostUrlEnablesConnection() throws Exception {
        String url = buildUdsUrl("loginTimeout=10000");
        assertTrue(url.startsWith(TestUtil.URL_PREFIX + "localhost?"));
        try (Connection conn = DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword())) {
            assertEquals(1, queryOne(conn));
        }
    }

    private static Connection openUdsConnection() throws Exception {
        return DriverManager.getConnection(
                buildUdsUrl("loginTimeout=10000"),
                TestUtil.getUser(),
                TestUtil.getPassword());
    }

    private static String buildUdsUrl(String extraQuery) {
        StringBuilder query = new StringBuilder();
        query.append("PGHOST=").append(TestUtil.getServer());
        query.append("&PGPORT=").append(TestUtil.getPort());
        query.append("&socketFactory=").append(SOCKET_FACTORY);
        query.append("&socketFactoryArg=").append(SOCKET_FACTORY_ARG);
        if (extraQuery != null && !extraQuery.isEmpty()) {
            query.append('&').append(extraQuery);
        }
        return TestUtil.URL_PREFIX + "localhost?" + query;
    }

    private static int queryOne(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select 1")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    /**
     * Custom socket factory loaded by the driver during UDS connections.
     */
    public static class OgracUdsSocketFactory extends javax.net.SocketFactory {
        private static volatile OgracUdsSocketFactory instance;

        private final String argument;
        private int socketCreated;

        public OgracUdsSocketFactory(String argument) {
            this.argument = argument;
            instance = this;
        }

        public static OgracUdsSocketFactory getInstance() {
            return instance;
        }

        public static void reset() {
            instance = null;
        }

        public String getArgument() {
            return argument;
        }

        public int getSocketCreated() {
            return socketCreated;
        }

        @Override
        public java.net.Socket createSocket() {
            socketCreated++;
            return new java.net.Socket();
        }

        @Override
        public java.net.Socket createSocket(String host, int port) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.net.Socket createSocket(String host, int port, java.net.InetAddress localHost, int localPort) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.net.Socket createSocket(java.net.InetAddress host, int port) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.net.Socket createSocket(java.net.InetAddress address, int port,
                java.net.InetAddress localAddress, int localPort) {
            throw new UnsupportedOperationException();
        }
    }
}
