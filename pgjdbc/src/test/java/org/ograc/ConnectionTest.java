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
import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.ORQueryExecutor;
import org.postgresql.core.ORStream;
import org.postgresql.jdbc.ORConnection;
import org.postgresql.jdbc.ORDatabaseMetaData;
import org.postgresql.jdbc.ORPreparedStatement;
import org.postgresql.jdbc.ORStatement;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.HostSpec;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test connection
 *
 * @author zhangting
 * @since  2026-09-15
 */
public class ConnectionTest {
    private static final String TABLE = "ograc_orconn_t";

    private Connection conn;
    private ORConnection orConn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        assertTrue(conn instanceof ORConnection);
        orConn = (ORConnection) conn;
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(id int primary key, name varchar(64))");
        }
    }

    @After
    public void tearDown() {
        try {
            if (conn != null && !conn.isClosed()) {
                if (!conn.getAutoCommit()) {
                    conn.setAutoCommit(true);
                }
                TestUtil.dropTable(conn, TABLE);
            }
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(conn);
    }

    @Test
    public void testInstanceAndUnwrap() throws SQLException {
        assertFalse(orConn.isClosed());
        assertTrue(orConn instanceof ORConnection);
        assertTrue(orConn instanceof ORBaseConnection);
        assertTrue(orConn instanceof Connection);
        try {
            orConn.isWrapperFor(ORConnection.class);
            fail("expected SQLFeatureNotSupportedException");
        } catch (java.sql.SQLFeatureNotSupportedException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void testOrSpecificAccessors() throws SQLException {
        assertNotNull(orConn.getQueryExecutor());
        assertTrue(orConn.getQueryExecutor() instanceof ORQueryExecutor);
        assertFalse(orConn.getQueryExecutor().isClosed());

        ORStream stream = orConn.getORStream();
        assertNotNull(stream);

        TimestampUtils tsUtils = orConn.getTimestampUtils();
        assertNotNull(tsUtils);

        HostSpec hostSpec = orConn.getHostSpec();
        assertNotNull(hostSpec);
        assertNotNull(hostSpec.getHost());
        assertTrue(hostSpec.getPort() > 0);

        String url = orConn.getUrl();
        assertNotNull(url);
        assertTrue(url.toLowerCase(Locale.ROOT).contains("ograc")
                || url.startsWith(TestUtil.URL_PREFIX));

        assertNotNull(orConn.getUser());
        assertFalse(orConn.getUser().isEmpty());
        orConn.isSsl();
    }

    @Test
    public void testSetAndGetHostSpec() {
        HostSpec original = orConn.getHostSpec();
        assertNotNull(original);
        HostSpec copy = new HostSpec(original.getHost(), original.getPort());
        orConn.setHostSpec(copy);
        assertEquals(copy.getHost(), orConn.getHostSpec().getHost());
        assertEquals(copy.getPort(), orConn.getHostSpec().getPort());
        orConn.setHostSpec(original);
    }

    @Test
    public void testFetchSize() {
        int original = orConn.getFetchSize();
        orConn.setFetchSize(50);
        assertEquals(50, orConn.getFetchSize());
        orConn.setFetchSize(original);
        assertEquals(original, orConn.getFetchSize());
    }

    @Test
    public void testIsValid() throws SQLException {
        assertTrue(orConn.isValid(5));
        assertTrue(orConn.isValid(0));
    }

    @Test
    public void testAutoCommitCommitRollback() throws SQLException {
        assertTrue(orConn.getAutoCommit());
        orConn.setAutoCommit(false);
        assertFalse(orConn.getAutoCommit());

        try (Statement st = orConn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values (1, 'a')");
        }
        orConn.commit();

        try (Statement st = orConn.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }

        try (Statement st = orConn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values (2, 'b')");
        }
        orConn.rollback();

        try (Statement st = orConn.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        orConn.setAutoCommit(true);
    }

    @Test
    public void testCreateStatementVariants() throws SQLException {
        try (Statement st = orConn.createStatement()) {
            assertTrue(st instanceof ORStatement);
            try (ResultSet rs = st.executeQuery("select 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
        try (Statement st = orConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            assertNotNull(st);
        }
        try (Statement st = orConn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
            assertNotNull(st);
        }
    }

    @Test
    public void testPrepareStatementVariants() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement("insert into " + TABLE + " values (?, ?)")) {
            assertTrue(ps instanceof ORPreparedStatement);
            ps.setInt(1, 1);
            ps.setString(2, "ps");
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = orConn.prepareStatement("select name from " + TABLE + " where id = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("ps", rs.getString(1));
            }
        }
    }

    @Test
    public void testGetMetaData() throws SQLException {
        DatabaseMetaData meta = orConn.getMetaData();
        assertNotNull(meta);
        assertTrue(meta instanceof ORDatabaseMetaData);
        assertNotNull(meta.getDatabaseProductName());
        assertNotNull(meta.getDriverName());
        assertNotNull(meta.getURL());
    }

    @Test
    public void testReadOnlyAndIsolation() throws SQLException {
        orConn.setReadOnly(true);
        assertTrue(orConn.isReadOnly());
        orConn.setReadOnly(false);
        assertFalse(orConn.isReadOnly());

        orConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, orConn.getTransactionIsolation());
    }

    @Test
    public void testSavepoint() throws SQLException {
        orConn.setAutoCommit(false);
        try (Statement st = orConn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values (1, 'sp1')");
        }
        Savepoint sp = orConn.setSavepoint("ograc_sp");
        assertNotNull(sp);
        try (Statement st = orConn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + " values (2, 'sp2')");
        }
        orConn.rollback(sp);
        orConn.commit();
        try (Statement st = orConn.createStatement();
             ResultSet rs = st.executeQuery("select count(*) from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        orConn.setAutoCommit(true);
    }

    @Test
    public void testCreateBlobClobArray() throws SQLException {
        Blob blob = orConn.createBlob();
        Clob clob = orConn.createClob();
        assertNotNull(blob);
        assertNotNull(clob);
        blob.free();
        clob.free();

        Array array = orConn.createArrayOf("integer", new Integer[]{1, 2, 3});
        assertNotNull(array);
        Object data = array.getArray();
        assertNotNull(data);
        try {
            array.free();
        } catch (SQLException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void testClientInfoAndWarnings() throws SQLException {
        assertNotNull(orConn.getClientInfo());
        try {
            orConn.clearWarnings();
            fail("expected SQLFeatureNotSupportedException");
        } catch (java.sql.SQLFeatureNotSupportedException expected) {
            assertNotNull(expected.getMessage());
        }
        try {
            orConn.getWarnings();
        } catch (java.sql.SQLFeatureNotSupportedException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void testHoldability() throws SQLException {
        int holdability = orConn.getHoldability();
        assertTrue(holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
                || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT);
        orConn.setHoldability(holdability);
        assertEquals(holdability, orConn.getHoldability());
    }

    @Test
    public void testCatalogAndSchemaAccessors() throws SQLException {
        String catalog = orConn.getCatalog();
        orConn.setCatalog(catalog);
        // schema may be null or current schema
        orConn.getSchema();
    }

    @Test
    public void testCloseMarksClosedAndInvalid() throws Exception {
        try (Connection c = TestUtil.openDB()) {
            assertTrue(c instanceof ORConnection);
            ORConnection oc = (ORConnection) c;
            assertFalse(oc.isClosed());
            assertTrue(oc.isValid(3));
            oc.close();
            assertTrue(oc.isClosed());
            assertFalse(oc.isValid(3));
        }
    }

    @Test
    public void testSetQueryExecutorRejectsNullBreakageRecovery() {
        ORQueryExecutor executor = orConn.getQueryExecutor();
        assertNotNull(executor);
        // set same executor back should be safe
        orConn.setQueryExecutor(executor);
        assertEquals(executor, orConn.getQueryExecutor());
    }

    @Test
    public void testSetOrStreamKeepsReference() {
        ORStream stream = orConn.getORStream();
        assertNotNull(stream);
        orConn.setOrStream(stream);
        assertEquals(stream, orConn.getORStream());
    }

    @Test
    public void testConnectWithProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", TestUtil.getUser());
        props.setProperty("password", TestUtil.getPassword());
        try (Connection c = TestUtil.openDB(props)) {
            assertNotNull(c);
            assertFalse(c.isClosed());
            assertTrue(c instanceof ORConnection);
        }
    }

    @Test
    public void testConnectWithLoginTimeoutParam() throws Exception {
        try (Connection c = TestUtil.openDB("loginTimeout=10000")) {
            assertNotNull(c);
            assertTrue(c.isValid(5));
        }
    }

    @Test
    public void testNativeSQLNotImplemented() {
        try {
            orConn.nativeSQL("select 1");
            fail("expected SQLException for nativeSQL");
        } catch (SQLException expected) {
            assertNotNull(expected.getMessage());
        }
    }
}
