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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * test date type
 *
 * @author zhangting
 * @since  2026-09-15
 */
public class DateTest {
    private static final String TABLE = "ograc_date_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(id int primary key, c_date date)");
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
    public void testDateRoundTrip() throws SQLException {
        Date expected = Date.valueOf("2025-06-29");
        insertDate(1, expected);
        assertEquals(expected, getDate(1));
    }

    @Test
    public void testDateGetByColumnLabel() throws SQLException {
        Date expected = Date.valueOf("2024-12-31");
        insertDate(1, expected);
        try (PreparedStatement ps = conn.prepareStatement("select c_date from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getDate("c_date"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testDateRepresentativeValues() throws SQLException {
        Date[] values = {
                Date.valueOf("1970-01-01"),
                Date.valueOf("1999-12-31"),
                Date.valueOf("2000-02-29"),
                Date.valueOf("2038-01-19"),
                Date.valueOf("9999-12-31")
        };
        for (int i = 0; i < values.length; i++) {
            insertDate(i + 1, values[i]);
        }
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], getDate(i + 1));
        }
    }

    @Test
    public void testDateWithCalendar() throws SQLException {
        Date expected = Date.valueOf("2025-07-01");
        Calendar local = Calendar.getInstance();
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_date) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setDate(2, expected, local);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_date from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Date got = rs.getDate(1, local);
                assertEquals(expected, got);
            }
        }
    }

    @Test
    public void testDateNullAndWasNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_date) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.DATE);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_date from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getDate(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testDateSetFromStringAndObject() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_date) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "2025-08-22");
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setObject(2, Date.valueOf("2025-08-23"));
            ps.executeUpdate();
        }
        assertEquals(Date.valueOf("2025-08-22"), getDate(1));
        assertEquals(Date.valueOf("2025-08-23"), getDate(2));
    }

    @Test
    public void testDateGetAsStringAndTimestamp() throws SQLException {
        Date expected = Date.valueOf("2025-03-15");
        insertDate(1, expected);
        try (PreparedStatement ps = conn.prepareStatement("select c_date from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expected.toString(), rs.getString(1));
                Timestamp ts = rs.getTimestamp(1);
                assertEquals(expected.toString(), ts.toString().substring(0, 10));
            }
        }
    }

    @Test
    public void testStatementLiteralInsert() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(id, c_date) values (1, date '2025-01-15')");
        }
        assertEquals(Date.valueOf("2025-01-15"), getDate(1));
    }

    @Test
    public void testDateBatchInsert() throws SQLException {
        Date[] values = {
                Date.valueOf("2025-01-01"),
                Date.valueOf("2025-06-15"),
                Date.valueOf("2025-12-31")
        };
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_date) values (?, ?)")) {
            for (int i = 0; i < values.length; i++) {
                ps.setInt(1, i + 1);
                ps.setDate(2, values[i]);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(values.length, counts.length);
            for (int count : counts) {
                assertTrue("unexpected update count " + count,
                        count == Statement.SUCCESS_NO_INFO || count >= 0);
            }
        }
        // Verify by reading back regardless of reported update counts
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], getDate(i + 1));
        }
    }

    @Test
    public void testDateResultSetMetaData() throws SQLException {
        insertDate(1, Date.valueOf("2025-05-20"));
        try (PreparedStatement ps = conn.prepareStatement("select c_date from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(1, md.getColumnCount());
                assertEquals("c_date", md.getColumnName(1).toLowerCase());
                assertEquals(Types.DATE, md.getColumnType(1));
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testDateUpdate() throws SQLException {
        insertDate(1, Date.valueOf("2025-01-01"));
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_date = ? where id = ?")) {
            ps.setDate(1, Date.valueOf("2025-12-31"));
            ps.setInt(2, 1);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(Date.valueOf("2025-12-31"), getDate(1));
    }

    @Test
    public void testDatetimeColumnRoundTrip() throws SQLException {
        final String table = "ograc_datetime_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table + " (c1 date, c2 datetime)");
        }
        try {
            Date expected = new Date(System.currentTimeMillis());
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into " + table + " (c1, c2) values (?, ?)")) {
                ps.setDate(1, expected);
                ps.setDate(2, expected);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = conn.prepareStatement("select * from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expected.toString(), rs.getDate(1).toString());
                assertEquals(expected.toString(), rs.getDate(2).toString());
            }
        } finally {
            TestUtil.dropTable(conn, table);
        }
    }

    @Test
    public void testBatchDateInsertWithNullEndDate() throws SQLException {
        final String table = "ograc_date_batch_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table
                    + "(id int, employee_code varchar(32), start_date date, end_date date)");
        }
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into " + table + "(id, employee_code, start_date, end_date) values(?,?,?,?)")) {
                ps.setInt(1, 2);
                ps.setString(2, "DFAS0001");
                ps.setDate(3, new Date(System.currentTimeMillis()));
                ps.setDate(4, null);
                ps.addBatch();
                ps.executeBatch();
            }
            conn.commit();
            try (PreparedStatement ps = conn.prepareStatement("select count(*) from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException ignore) {
                // ignore
            }
            TestUtil.dropTable(conn, table);
        }
    }

    private void insertDate(int id, Date value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_date) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setDate(2, value);
            ps.executeUpdate();
        }
    }

    private Date getDate(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_date from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getDate(1);
            }
        }
    }
}
