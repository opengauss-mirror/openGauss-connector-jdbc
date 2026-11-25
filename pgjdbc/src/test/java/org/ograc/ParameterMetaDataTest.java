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
import org.postgresql.jdbc.ORConnection;
import org.postgresql.jdbc.ORParameterMetaData;
import org.postgresql.jdbc.ORPreparedStatement;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test ParameterMetaData
 *
 * @author zhangting
 * @since  2026-09-15
 */
public class ParameterMetaDataTest {
    private static final String TABLE = "ograc_pmd_t";

    private Connection conn;
    private ORConnection orConn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        assertTrue(conn instanceof ORConnection);
        orConn = (ORConnection) conn;
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_int int, "
                    + "c_varchar varchar(64), "
                    + "c_double double precision, "
                    + "c_decimal decimal(12,4), "
                    + "c_bool boolean, "
                    + "c_bytea bytea, "
                    + "c_date date, "
                    + "c_ts timestamp)");
        }
    }

    @After
    public void tearDown() {
        try {
            if (conn != null && !conn.isClosed()) {
                TestUtil.dropTable(conn, TABLE);
            }
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(conn);
    }

    @Test
    public void testGetParameterMetaDataReturnsORParameterMetaData() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            assertTrue(ps instanceof ORPreparedStatement);
            bindTwoIntParams(ps);
            ParameterMetaData pmd = ps.getParameterMetaData();
            assertNotNull(pmd);
            assertTrue(pmd instanceof ORParameterMetaData);
        }
    }

    @Test
    public void testGetParameterCountBeforeBind() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "insert into " + TABLE + "(id, c_int, c_varchar) values (?, ?, ?)")) {
            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(3, pmd.getParameterCount());
        }
    }

    @Test
    public void testParameterTypesAfterBind() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement("insert into " + TABLE
                        + "(id, c_int, c_varchar, c_double, c_decimal, c_bool, c_bytea, c_date, c_ts)"
                        + " values (?,?,?,?,?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 42);
            ps.setString(3, "text");
            ps.setDouble(4, 3.14d);
            ps.setBigDecimal(5, new BigDecimal("12.34"));
            ps.setBoolean(6, true);
            ps.setBytes(7, new byte[]{1, 2, 3});
            ps.setDate(8, Date.valueOf("2025-06-29"));
            ps.setTimestamp(9, Timestamp.valueOf("2025-06-29 12:00:00"));
            ps.addBatch();

            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(9, pmd.getParameterCount());

            assertParam(pmd, 1, Types.INTEGER, "INT", "java.lang.Integer");
            assertParam(pmd, 2, Types.INTEGER, "INT", "java.lang.Integer");
            assertParam(pmd, 3, Types.VARCHAR, "TEXT", "java.lang.String");
            assertParam(pmd, 4, Types.DOUBLE, "REAL", "java.lang.Double");
            assertParam(pmd, 5, Types.NUMERIC, "NUMERIC", "java.math.BigDecimal");
            assertParam(pmd, 6, Types.INTEGER, "INT", "java.lang.Integer");
            assertParam(pmd, 7, Types.BINARY, "BINARY", "[B");
            assertParam(pmd, 8, Types.DATE, "DATE", "java.sql.Date");
            assertParam(pmd, 9, Types.TIMESTAMP, "TIMESTAMP", "java.sql.Timestamp");
        }
    }

    @Test
    public void testInheritedMetadataFields() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "update " + TABLE + " set c_varchar = ? where id = ?")) {
            ps.setString(1, "upd");
            ps.setInt(2, 1);
            ps.addBatch();

            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(2, pmd.getParameterCount());
            assertEquals(Types.VARCHAR, pmd.getParameterType(1));
            assertEquals(Types.INTEGER, pmd.getParameterType(2));
            try {
                pmd.getParameterMode(1);
                fail("expected exception for getParameterMode");
            } catch (SQLException | NullPointerException expected) {
                assertNotNull(expected);
            }
            try {
                pmd.isNullable(1);
                fail("expected exception for isNullable");
            } catch (SQLException | NullPointerException expected) {
                assertNotNull(expected);
            }
            try {
                pmd.getPrecision(1);
                fail("expected exception for getPrecision");
            } catch (SQLException | NullPointerException expected) {
                assertNotNull(expected);
            }
        }
    }

    @Test
    public void testSelectWhereParameterMetadata() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "select c_varchar from " + TABLE + " where id = ? and c_int = ?")) {
            ps.setInt(1, 1);
            ps.setInt(2, 100);
            ps.addBatch();

            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(2, pmd.getParameterCount());
            assertEquals(Types.INTEGER, pmd.getParameterType(1));
            assertEquals("INT", pmd.getParameterTypeName(1));
            assertEquals("java.lang.Integer", pmd.getParameterClassName(1));
            assertEquals(Types.INTEGER, pmd.getParameterType(2));
        }
    }

    @Test
    public void testEmptyParameterListThrows() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(2, pmd.getParameterCount());
            try {
                pmd.getParameterType(1);
                fail("expected SQLException when parameter list is empty");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("empty"));
            }
        }
    }

    @Test
    public void testInvalidParameterIndex() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            bindTwoIntParams(ps);
            ParameterMetaData pmd = ps.getParameterMetaData();
            assertInvalidIndex(pmd, 0);
            assertInvalidIndex(pmd, 3);
        }
    }

    @Test
    public void testMetadataUsesLatestBatchParameters() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 10);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setLong(2, 20L);
            ps.addBatch();

            ParameterMetaData pmd = ps.getParameterMetaData();
            assertEquals(2, pmd.getParameterCount());
            assertEquals(Types.INTEGER, pmd.getParameterType(2));
            assertEquals("INT", pmd.getParameterTypeName(2));
            assertEquals("java.lang.Integer", pmd.getParameterClassName(2));
        }
    }

    @Test
    public void testUnwrapNotImplemented() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement("select 1 from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            ps.addBatch();
            ParameterMetaData pmd = ps.getParameterMetaData();
            try {
                pmd.unwrap(ParameterMetaData.class);
                fail("expected SQLException for unwrap");
            } catch (SQLException expected) {
                assertNotNull(expected.getMessage());
            }
            try {
                pmd.isWrapperFor(ParameterMetaData.class);
                fail("expected SQLException for isWrapperFor");
            } catch (SQLException expected) {
                assertNotNull(expected.getMessage());
            }
        }
    }

    @Test
    public void testClosedStatementThrows() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "insert into " + TABLE + "(id, c_int) values (?, ?)")) {
            ps.close();
            try {
                ps.getParameterMetaData();
                fail("expected SQLException on closed statement");
            } catch (SQLException expected) {
                assertNotNull(expected.getMessage());
            }
        }
    }

    private static void bindTwoIntParams(PreparedStatement ps) throws SQLException {
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.addBatch();
    }

    private static void assertParam(ParameterMetaData pmd, int index, int sqlType,
            String typeName, String className) throws SQLException {
        assertEquals(sqlType, pmd.getParameterType(index));
        assertEquals(typeName, pmd.getParameterTypeName(index));
        assertEquals(className, pmd.getParameterClassName(index));
    }

    private static void assertInvalidIndex(ParameterMetaData pmd, int index) {
        try {
            pmd.getParameterType(index);
            fail("expected SQLException for index " + index);
        } catch (SQLException expected) {
            assertTrue(expected.getMessage().toLowerCase().contains("out of bounds")
                    || expected.getMessage().toLowerCase().contains("index"));
        }
    }
}
