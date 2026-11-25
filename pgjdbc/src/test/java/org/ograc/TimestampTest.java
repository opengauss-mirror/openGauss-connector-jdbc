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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * test timestamp
 *
 * @author zhangting
 * @since  2026-10-13
 */
public class TimestampTest {
    private static final String TABLE = "ograc_timestamp_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "("
                    + "id int primary key, "
                    + "c_ts timestamp, "
                    + "c_ts0 timestamp(0), "
                    + "c_ts3 timestamp(3), "
                    + "c_ts6 timestamp(6), "
                    + "c_tstz timestamp with time zone)");
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
    public void testTimestampRoundTrip() throws SQLException {
        Timestamp ts = Timestamp.valueOf("2025-06-29 12:34:56");
        insertTs(1, ts);
        assertEquals(ts, getTs(1));
    }

    @Test
    public void testTimestampWithFractionalSeconds() throws SQLException {
        Timestamp ts = Timestamp.valueOf("2025-01-02 03:04:05.123456");
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts6) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setTimestamp(2, ts);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_ts6 from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(ts, rs.getTimestamp(1));
                assertEquals(ts, rs.getTimestamp("c_ts6"));
            }
        }
    }

    @Test
    public void testTimestampPrecisionTruncation() throws SQLException {
        Timestamp withNanos = Timestamp.valueOf("2025-03-15 10:20:30.987654");
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_ts0, c_ts3) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setTimestamp(2, withNanos);
            ps.setTimestamp(3, withNanos);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_ts0, c_ts3 from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Timestamp ts0 = rs.getTimestamp(1);
                Timestamp ts3 = rs.getTimestamp(2);
                assertEquals(Timestamp.valueOf("2025-03-15 10:20:31"), stripFraction(ts0));
                assertEquals(0, ts0.getNanos());
                assertEquals("2025-03-15 10:20:30", ts3.toString().substring(0, 19));
                assertTrue(ts3.getNanos() >= 987000000 && ts3.getNanos() < 988000000
                        || Math.abs(ts3.getTime() - withNanos.getTime()) < 2);
            }
        }
    }

    @Test
    public void testTimestamptzRoundTrip() throws SQLException {
        Timestamp ts = Timestamp.valueOf("2024-12-31 23:59:59.123456");
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_tstz) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setTimestamp(2, ts);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_tstz from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTimestampMillisEquals(ts, rs.getTimestamp(1));
            }
        }
    }

    @Test
    public void testTimestampWithCalendar() throws SQLException {
        Timestamp ts = Timestamp.valueOf("2025-07-01 08:00:00");
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setTimestamp(2, ts, utc);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Timestamp got = rs.getTimestamp(1, utc);
                assertEquals(ts.getTime(), got.getTime());
            }
        }
    }

    @Test
    public void testTimestampNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_ts, c_ts6, c_tstz) values (?, ?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.TIMESTAMP);
            ps.setNull(3, Types.TIMESTAMP);
            ps.setNull(4, Types.TIMESTAMP_WITH_TIMEZONE);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_ts, c_ts6, c_tstz from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getTimestamp(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getTimestamp(2));
                assertTrue(rs.wasNull());
                assertNull(rs.getTimestamp(3));
                assertTrue(rs.wasNull());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testTimestampSetFromString() throws SQLException {
        String value = "2025-08-22 11:22:33";
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, value);
            ps.executeUpdate();
        }
        assertEquals(Timestamp.valueOf(value), getTs(1));
    }

    @Test
    public void testTimestampGetAsStringAndObject() throws SQLException {
        Timestamp ts = Timestamp.valueOf("2025-05-05 05:05:05");
        insertTs(1, ts);
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getString(1).startsWith("2025-05-05 05:05:05"));
                Object obj = rs.getObject(1);
                assertTrue(obj instanceof Timestamp);
                assertEquals(ts, obj);
            }
        }
    }

    @Test
    public void testTimestampCompareAndRangeQuery() throws SQLException {
        insertTs(1, Timestamp.valueOf("2025-01-01 00:00:00"));
        insertTs(2, Timestamp.valueOf("2025-06-15 12:00:00"));
        insertTs(3, Timestamp.valueOf("2025-12-31 23:59:59"));

        try (PreparedStatement ps = conn.prepareStatement(
                "select id from " + TABLE + " where c_ts >= ? and c_ts < ? order by id")) {
            ps.setTimestamp(1, Timestamp.valueOf("2025-06-01 00:00:00"));
            ps.setTimestamp(2, Timestamp.valueOf("2025-07-01 00:00:00"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testTimestampUpdate() throws SQLException {
        insertTs(1, Timestamp.valueOf("2020-01-01 00:00:00"));
        Timestamp updated = Timestamp.valueOf("2026-02-03 04:05:06");
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_ts = ? where id = ?")) {
            ps.setTimestamp(1, updated);
            ps.setInt(2, 1);
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(updated, getTs(1));
    }

    @Test
    public void testTimestampBatchInsert() throws SQLException {
        Timestamp[] values = new Timestamp[]{
                Timestamp.valueOf("2021-01-01 01:01:01"),
                Timestamp.valueOf("2022-02-02 02:02:02"),
                Timestamp.valueOf("2023-03-03 03:03:03")
        };
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            for (int i = 0; i < values.length; i++) {
                ps.setInt(1, i + 1);
                ps.setTimestamp(2, values[i]);
                ps.addBatch();
            }
            assertEquals(values.length, ps.executeBatch().length);
        }
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], getTs(i + 1));
        }
    }

    @Test
    public void testTimestampMinAndFarFuture() throws SQLException {
        Timestamp early = Timestamp.valueOf("1970-01-01 00:00:01");
        Timestamp late = Timestamp.valueOf("2099-12-31 23:59:59");
        insertTs(1, early);
        insertTs(2, late);
        assertEquals(early, getTs(1));
        assertEquals(late, getTs(2));
    }

    @Test
    public void testAllTimestampColumnsTogether() throws SQLException {
        Timestamp base = Timestamp.valueOf("2025-09-09 09:09:09.123456");
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_ts, c_ts0, c_ts3, c_ts6, c_tstz) values (?,?,?,?,?,?)")) {
            ps.setInt(1, 1);
            ps.setTimestamp(2, base);
            ps.setTimestamp(3, base);
            ps.setTimestamp(4, base);
            ps.setTimestamp(5, base);
            ps.setTimestamp(6, base);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "select c_ts, c_ts0, c_ts3, c_ts6, c_tstz from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTimestampMillisEquals(base, rs.getTimestamp("c_ts6"));
                assertTimestampMillisEquals(base, rs.getTimestamp("c_tstz"));
                assertEquals(0, rs.getTimestamp("c_ts0").getNanos());
                Timestamp ts3 = rs.getTimestamp("c_ts3");
                assertEquals("2025-09-09 09:09:09", ts3.toString().substring(0, 19));
                assertTrue(ts3.getNanos() / 1000000 == 123);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testTimeRoundTrip() throws SQLException {
        Time expected = Time.valueOf("12:34:56");
        insertTime(1, expected);
        assertTimeEquals(expected, getTime(1));
    }

    @Test
    public void testTimeGetByColumnLabel() throws SQLException {
        Time expected = Time.valueOf("08:15:30");
        insertTime(1, expected);
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTimeEquals(expected, rs.getTime("c_ts"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testTimeRepresentativeValues() throws SQLException {
        Time[] values = {
                Time.valueOf("00:00:00"),
                Time.valueOf("12:00:00"),
                Time.valueOf("23:59:59")
        };
        for (int i = 0; i < values.length; i++) {
            insertTime(i + 1, values[i]);
        }
        for (int i = 0; i < values.length; i++) {
            assertTimeEquals(values[i], getTime(i + 1));
        }
    }

    @Test
    public void testTimeWithCalendar() throws SQLException {
        Time expected = Time.valueOf("18:45:10");
        Calendar local = Calendar.getInstance();
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setTime(2, expected, local);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTimeEquals(expected, rs.getTime(1, local));
            }
        }
    }

    @Test
    public void testTimeNullAndWasNull() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "insert into " + TABLE + "(id, c_ts, c_ts3) values (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setNull(2, Types.TIMESTAMP);
            ps.setNull(3, Types.TIMESTAMP);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_ts, c_ts3 from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNull(rs.getTime(1));
                assertTrue(rs.wasNull());
                assertNull(rs.getTime(2));
                assertTrue(rs.wasNull());
                assertNull(rs.getObject(1));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    public void testTimeSetFromStringAndObject() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "1970-01-01 09:10:11");
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.setObject(2, Time.valueOf("09:10:12"));
            ps.executeUpdate();
        }
        assertTimeEquals(Time.valueOf("09:10:11"), getTime(1));
        assertTimeEquals(Time.valueOf("09:10:12"), getTime(2));
    }

    @Test
    public void testTimeGetAsString() throws SQLException {
        Time expected = Time.valueOf("14:25:36");
        insertTime(1, expected);
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String actual = rs.getString(1);
                assertTrue(actual.contains(expected.toString()));
            }
        }
    }

    @Test
    public void testTimeFractionalSeconds() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts3) values (?, ?)")) {
            ps.setInt(1, 1);
            ps.setTimestamp(2, Timestamp.valueOf("1970-01-01 10:20:30.123"));
            ps.executeUpdate();
        }
        try (PreparedStatement ps = conn.prepareStatement("select c_ts3 from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String actual = rs.getString(1);
                assertTrue(actual.contains("10:20:30"));
                assertTrue(actual.contains("123"));
                assertTimeEquals(Time.valueOf("10:20:30"), rs.getTime(1));
            }
        }
    }

    @Test
    public void testTimeStatementLiteralInsert() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(id, c_ts) values (1, timestamp '1970-01-01 16:30:45')");
        }
        assertTimeEquals(Time.valueOf("16:30:45"), getTime(1));
    }

    @Test
    public void testTimeBatchInsert() throws SQLException {
        Time[] values = {
                Time.valueOf("01:02:03"),
                Time.valueOf("11:22:33"),
                Time.valueOf("21:43:05")
        };
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            for (int i = 0; i < values.length; i++) {
                ps.setInt(1, i + 1);
                ps.setTime(2, values[i]);
                ps.addBatch();
            }
            assertEquals(values.length, ps.executeBatch().length);
        }
        for (int i = 0; i < values.length; i++) {
            assertTimeEquals(values[i], getTime(i + 1));
        }
    }

    @Test
    public void testTimeResultSetMetaData() throws SQLException {
        insertTime(1, Time.valueOf("07:08:09"));
        try (PreparedStatement ps = conn.prepareStatement("select c_ts, c_ts3 from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(2, md.getColumnCount());
                assertEquals("C_TS", md.getColumnName(1));
                assertEquals(Types.TIMESTAMP, md.getColumnType(1));
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testTimeUpdate() throws SQLException {
        insertTime(1, Time.valueOf("01:00:00"));
        try (PreparedStatement ps = conn.prepareStatement("update " + TABLE + " set c_ts = ? where id = ?")) {
            ps.setTime(1, Time.valueOf("02:00:00"));
            ps.setInt(2, 1);
            assertEquals(1, ps.executeUpdate());
        }
        assertTimeEquals(Time.valueOf("02:00:00"), getTime(1));
    }

    @Test
    public void testTimeGetAsTimestamp() throws SQLException {
        Time expected = Time.valueOf("13:14:15");
        insertTime(1, expected);
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Timestamp ts = rs.getTimestamp(1);
                assertTrue(ts != null);
                assertEquals(expected.toString(), ts.toString().substring(11, 19));
            }
        }
    }

    @Test
    public void testTimestampPlainRoundTripFromTimeTest() throws SQLException {
        final String table = "ograc_ts_plain_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table + " (c1 timestamp)");
        }
        try {
            Timestamp expected = Timestamp.valueOf("2025-06-29 12:34:56");
            try (PreparedStatement ps = conn.prepareStatement("insert into " + table + " (c1) values (?)")) {
                ps.setTimestamp(1, expected);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = conn.prepareStatement("select * from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getTimestamp(1));
            }
        } finally {
            TestUtil.dropTable(conn, table);
        }
    }

    @Test
    public void testTimestampWithTimeZoneFromTimeTest() throws SQLException {
        final String table = "ograc_tstz_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table + " (c1 timestamp with time zone)");
        }
        try {
            Timestamp timestamp = Timestamp.valueOf("2025-06-29 12:34:56.123");
            try (PreparedStatement ps = conn.prepareStatement("insert into " + table + " (c1) values (?)")) {
                ps.setTimestamp(1, timestamp);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = conn.prepareStatement("select c1 from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(timestamp.getTime(), rs.getTimestamp(1).getTime());
            }
        } finally {
            TestUtil.dropTable(conn, table);
        }
    }

    @Test
    public void testTimestampWithLocalTimeZoneFromTimeTest() throws SQLException {
        final String table = "ograc_tsltz_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table + " (c1 timestamp with local time zone)");
        }
        try {
            Timestamp timestamp = Timestamp.valueOf("2025-06-29 12:34:56.123");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
            String expected = dateFormat.format(timestamp);

            try (PreparedStatement ps = conn.prepareStatement("insert into " + table + " (c1) values (?)")) {
                ps.setTimestamp(1, timestamp);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "select to_char(c1, 'YYYY-MM-DD HH24:MI:SS:FF3') from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getString(1));
            }
        } finally {
            TestUtil.dropTable(conn, table);
        }
    }

    @Test
    public void testIntervalYearToMonth() throws SQLException {
        final String table = "ograc_interval_ym_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table + " (c1 interval year to month)");
        }
        try {
            String expected = "25-06";
            try (PreparedStatement ps = conn.prepareStatement("insert into " + table + " (c1) values (?)")) {
                ps.setString(1, expected);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = conn.prepareStatement("select * from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getString(1));
            }
        } finally {
            TestUtil.dropTable(conn, table);
        }
    }

    @Test
    public void testIntervalDayToSecond() throws SQLException {
        final String table = "ograc_interval_ds_t";
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
            st.execute("create table " + table + " (c1 interval day to second)");
        }
        try {
            String inserted = "7 10:16:32";
            try (PreparedStatement ps = conn.prepareStatement("insert into " + table + " (c1) values (?)")) {
                ps.setString(1, inserted);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = conn.prepareStatement("select * from " + table);
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String actual = rs.getString(1);
                assertTrue(actual.replace(".0", "").equals(inserted)
                        || actual.startsWith("7 10:16:32")
                        || actual.startsWith("07 10:16:32"));
            }
        } finally {
            TestUtil.dropTable(conn, table);
        }
    }

    private void insertTs(int id, Timestamp ts) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setTimestamp(2, ts);
            ps.executeUpdate();
        }
    }

    private Timestamp getTs(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getTimestamp(1);
            }
        }
    }

    private void insertTime(int id, Time value) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(id, c_ts) values (?, ?)")) {
            ps.setInt(1, id);
            ps.setTime(2, value);
            ps.executeUpdate();
        }
    }

    private Time getTime(int id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select c_ts from " + TABLE + " where id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getTime(1);
            }
        }
    }

    private static Timestamp stripFraction(Timestamp ts) {
        return new Timestamp(ts.getTime() / 1000 * 1000);
    }

    private static void assertTimestampMillisEquals(Timestamp expected, Timestamp actual) {
        assertTrue(actual != null);
        assertEquals(expected.getTime(), actual.getTime());
    }

    private static void assertTimeEquals(Time expected, Time actual) {
        assertTrue(actual != null);
        assertEquals(expected.toString(), actual.toString());
    }
}
