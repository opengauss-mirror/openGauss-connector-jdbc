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
import org.postgresql.jdbc.ORResultSetMetaData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test ResultSetMetaData
 *
 * @author zhangting
 * @since  2026-09-17
 */
public class ResultSetMetaDataTest {
    private static final String TABLE = "ograc_rsmd_t";

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
                    + "c_int int not null, "
                    + "c_varchar varchar(64), "
                    + "c_decimal decimal(12,4), "
                    + "c_double double precision, "
                    + "c_bool boolean, "
                    + "c_bytea bytea, "
                    + "c_date date, "
                    + "c_ts timestamp)");
        }
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE
                        + " (id, c_int, c_varchar, c_decimal, c_double, c_bool, c_bytea, c_date, c_ts)"
                        + " values (?,?,?,?,?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setInt(2, 42);
            ps.setString(3, "hello");
            ps.setBigDecimal(4, new java.math.BigDecimal("12.3400"));
            ps.setDouble(5, 3.14d);
            ps.setBoolean(6, true);
            ps.setBytes(7, new byte[]{0x01, 0x02});
            ps.setDate(8, java.sql.Date.valueOf("2025-06-29"));
            ps.setTimestamp(9, java.sql.Timestamp.valueOf("2025-06-29 12:00:00"));
            ps.executeUpdate();
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
    public void testGetMetaDataReturnsORResultSetMetaData() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select id, c_int, c_varchar from " + TABLE)) {
            ResultSetMetaData md = rs.getMetaData();
            assertNotNull(md);
            assertTrue(md instanceof ORResultSetMetaData);
        }
    }

    @Test
    public void testColumnCountAndNames() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select id, c_int, c_varchar, c_decimal, c_double, c_bool, c_bytea, c_date, c_ts"
                             + " from " + TABLE)) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(9, md.getColumnCount());
            assertEquals("ID", md.getColumnName(1).toUpperCase());
            assertEquals("C_INT", md.getColumnName(2).toUpperCase());
            assertEquals("C_VARCHAR", md.getColumnName(3).toUpperCase());
            assertEquals(md.getColumnName(1), md.getColumnLabel(1));
            assertEquals(md.getColumnName(3), md.getColumnLabel(3));
        }
    }

    @Test
    public void testColumnAliasLabel() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select c_int as my_int, c_varchar as my_str from " + TABLE + " where id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(2, md.getColumnCount());
            assertEquals("MY_INT", md.getColumnName(1).toUpperCase());
            assertEquals("MY_STR", md.getColumnName(2).toUpperCase());
            assertEquals(md.getColumnName(1), md.getColumnLabel(1));
        }
    }

    @Test
    public void testColumnTypesAndClassNames() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select c_int, c_varchar, c_decimal, c_double, c_bool, c_bytea, c_date, c_ts"
                             + " from " + TABLE + " where id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(8, md.getColumnCount());

            assertColumnType(md, 1, Types.INTEGER, "INT", "java.lang.Integer");
            assertStringTypeName(md, 2, "java.lang.String");
            assertColumnType(md, 3, Types.NUMERIC, "NUMERIC", "java.math.BigDecimal");
            assertColumnType(md, 4, Types.DOUBLE, "REAL", "java.lang.Double");
            assertColumnType(md, 5, Types.BOOLEAN, "BOOL", "java.lang.Boolean");
            assertBinaryTypeName(md, 6);
            assertColumnType(md, 7, Types.DATE, "DATE", "java.sql.Date");
            assertColumnType(md, 8, Types.TIMESTAMP, "TIMESTAMP", "java.sql.Timestamp");
        }
    }

    @Test
    public void testColumnFlags() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select c_int, c_varchar, c_decimal, c_double, c_bool from " + TABLE + " where id = 1")) {
            ResultSetMetaData md = rs.getMetaData();

            assertTrue(md.isSigned(1));
            assertTrue(md.isSigned(3));
            assertTrue(md.isSigned(4));
            assertFalse(md.isSigned(5));

            assertFalse(md.isCaseSensitive(1));
            assertTrue(md.isCaseSensitive(2));
            assertFalse(md.isCaseSensitive(4));

            assertFalse(md.isCurrency(1));
            assertFalse(md.isCurrency(2));
            assertTrue(md.isSearchable(1));
            assertTrue(md.isSearchable(2));
            assertFalse(md.isWritable(1));
            assertFalse(md.isReadOnly(1));
            assertFalse(md.isDefinitelyWritable(1));
        }
    }

    @Test
    public void testPrecisionScaleAndDisplaySize() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select c_varchar, c_decimal, c_bytea from " + TABLE + " where id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertTrue(md.getColumnDisplaySize(1) >= 0);
            assertTrue(md.getPrecision(1) >= 0);
            assertTrue(md.getPrecision(2) >= 0);
            assertTrue(md.getScale(2) >= 0);
            assertTrue(md.getPrecision(3) >= 0);
        }
    }

    @Test
    public void testNullableFlags() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select id, c_int, c_varchar from " + TABLE + " where id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            int idNullable = md.isNullable(1);
            int intNullable = md.isNullable(2);
            int varcharNullable = md.isNullable(3);
            assertTrue(idNullable == ResultSetMetaData.columnNoNulls
                    || idNullable == ResultSetMetaData.columnNullable
                    || idNullable == ResultSetMetaData.columnNullableUnknown);
            assertTrue(intNullable == ResultSetMetaData.columnNoNulls
                    || intNullable == ResultSetMetaData.columnNullable
                    || intNullable == ResultSetMetaData.columnNullableUnknown);
            assertTrue(varcharNullable == ResultSetMetaData.columnNoNulls
                    || varcharNullable == ResultSetMetaData.columnNullable
                    || varcharNullable == ResultSetMetaData.columnNullableUnknown);
        }
    }

    @Test
    public void testSchemaTableCatalogNames() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_int from " + TABLE)) {
            ResultSetMetaData md = rs.getMetaData();
            assertNull(md.getSchemaName(1));
            assertNull(md.getTableName(1));
            assertEquals("", md.getCatalogName(1));
        }
    }

    @Test
    public void testPreparedStatementResultSetMetaData() throws SQLException {
        try (PreparedStatement ps = orConn.prepareStatement(
                "select c_int, c_varchar from " + TABLE + " where id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertTrue(md instanceof ORResultSetMetaData);
                assertEquals(2, md.getColumnCount());
                assertEquals(Types.INTEGER, md.getColumnType(1));
                assertEquals(Types.VARCHAR, md.getColumnType(2));
            }
        }
    }

    @Test
    public void testLiteralSelectMetaData() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select 1 as one_col, 'x' as str_col")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(2, md.getColumnCount());
            assertEquals("ONE_COL", md.getColumnName(1).toUpperCase());
            assertEquals("STR_COL", md.getColumnName(2).toUpperCase());
            assertNotNull(md.getColumnTypeName(1));
            assertNotNull(md.getColumnClassName(2));
        }
    }

    @Test
    public void testInvalidColumnIndex() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_int from " + TABLE)) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(1, md.getColumnCount());
            try {
                md.getColumnName(0);
                fail("expected SQLException for index 0");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("out of range"));
            }
            try {
                md.getColumnType(2);
                fail("expected SQLException for index 2");
            } catch (SQLException expected) {
                assertTrue(expected.getMessage().toLowerCase().contains("out of range"));
            }
        }
    }

    @Test
    public void testUnwrapNotImplemented() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_int from " + TABLE)) {
            ResultSetMetaData md = rs.getMetaData();
            try {
                md.unwrap(ResultSetMetaData.class);
                fail("expected SQLException for unwrap");
            } catch (SQLException expected) {
                assertNotNull(expected.getMessage());
            }
            try {
                md.isWrapperFor(ResultSetMetaData.class);
                fail("expected SQLException for isWrapperFor");
            } catch (SQLException expected) {
                assertNotNull(expected.getMessage());
            }
        }
    }

    @Test
    public void testAutoIncrementFlag() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select id, c_int from " + TABLE)) {
            ResultSetMetaData md = rs.getMetaData();
            md.isAutoIncrement(1);
            assertFalse(md.isAutoIncrement(2));
        }
    }

    private static void assertColumnType(ResultSetMetaData md, int index, int sqlType,
            String typeName, String className) throws SQLException {
        assertEquals(sqlType, md.getColumnType(index));
        assertEquals(typeName, md.getColumnTypeName(index));
        assertEquals(className, md.getColumnClassName(index));
    }

    private static void assertStringTypeName(ResultSetMetaData md, int index, String className)
            throws SQLException {
        assertEquals(Types.VARCHAR, md.getColumnType(index));
        String typeName = md.getColumnTypeName(index);
        assertTrue("TEXT".equals(typeName) || "VARCHAR".equals(typeName));
        assertEquals(className, md.getColumnClassName(index));
    }

    private static void assertBinaryTypeName(ResultSetMetaData md, int index)
            throws SQLException {
        int sqlType = md.getColumnType(index);
        String typeName = md.getColumnTypeName(index);
        String className = md.getColumnClassName(index);
        assertTrue("unexpected binary sqlType " + sqlType,
                sqlType == Types.BINARY || sqlType == Types.VARBINARY || sqlType == Types.BLOB);
        assertTrue("unexpected binary typeName " + typeName,
                "BINARY".equals(typeName) || "VARBINARY".equals(typeName) || "BLOB".equals(typeName));
        assertTrue("unexpected binary className " + className,
                "[B".equals(className) || "java.sql.Blob".equals(className));
    }
}
