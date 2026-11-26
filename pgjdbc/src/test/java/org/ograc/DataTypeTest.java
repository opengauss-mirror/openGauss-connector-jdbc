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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test data type
 *
 * @author zhangting
 * @since  2026-09-18
 */
public class DataTypeTest {
    private static final String TABLE = "ograc_dtype_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "c_int int, "
                    + "c_bigint bigint, "
                    + "c_varchar varchar(64), "
                    + "c_decimal decimal(10,2), "
                    + "c_float double precision, "
                    + "c_bool boolean, "
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
    public void testNumericAndStringTypes() throws SQLException {
        String sql = "insert into " + TABLE
                + "(c_int, c_bigint, c_varchar, c_decimal, c_float, c_bool) values(?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 42);
            ps.setLong(2, 4200000000L);
            ps.setString(3, "hello-ograc");
            ps.setBigDecimal(4, new BigDecimal("12.34"));
            ps.setDouble(5, 3.14);
            ps.setBoolean(6, true);
            ps.executeUpdate();
        }

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select c_int, c_bigint, c_varchar, c_decimal, c_float, c_bool from " + TABLE)) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals(4200000000L, rs.getLong(2));
            assertEquals("hello-ograc", rs.getString(3));
            assertEquals(0, new BigDecimal("12.34").compareTo(rs.getBigDecimal(4)));
            assertEquals(3.14, rs.getDouble(5), 0.0001);
            assertTrue(rs.getBoolean(6));
        }
    }

    @Test
    public void testDateTimeTypes() throws SQLException {
        Date date = Date.valueOf("2025-06-29");
        Timestamp ts = Timestamp.valueOf("2025-06-29 12:34:56");

        String sql = "insert into " + TABLE + "(c_int, c_date, c_ts) values(1,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setDate(1, date);
            ps.setTimestamp(2, ts);
            ps.executeUpdate();
        }

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_date, c_ts from " + TABLE + " where c_int = 1")) {
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1));
            assertEquals(ts, rs.getTimestamp(2));
        }
    }

    @Test
    public void testNullAllNullableColumns() throws SQLException {
        String sql = "insert into " + TABLE
                + "(c_int, c_bigint, c_varchar, c_decimal, c_float, c_bool, c_date, c_ts)"
                + " values(?,?,?,?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 99);
            ps.setNull(2, Types.BIGINT);
            ps.setNull(3, Types.VARCHAR);
            ps.setNull(4, Types.DECIMAL);
            ps.setNull(5, Types.DOUBLE);
            ps.setNull(6, Types.BOOLEAN);
            ps.setNull(7, Types.DATE);
            ps.setNull(8, Types.TIMESTAMP);
            ps.executeUpdate();
        }

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select c_varchar from " + TABLE + " where c_int = 99")) {
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertTrue(rs.wasNull());
        }
    }
}
