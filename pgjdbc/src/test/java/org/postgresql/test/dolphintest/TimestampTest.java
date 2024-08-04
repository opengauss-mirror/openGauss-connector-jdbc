package org.postgresql.test.dolphintest;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TimestampTest extends BaseTest4B {
    @Test
    public void testTimeZone() throws Exception {
        TestUtil.createTable(con, "test_timestamp", "id int");
        String createSql = "INSERT INTO test_timestamp VALUES (1)";
        try (PreparedStatement pstmt = con.prepareStatement(createSql)) {
            pstmt.execute();
        }

        String updateSql = "UPDATE test_timestamp SET id = timestampdiff(second,?,?)";
        try (PreparedStatement pstmt2 = con.prepareStatement(updateSql)) {
            java.util.Date start = new java.util.Date();
            pstmt2.setTimestamp(1, new Timestamp(start.getTime()));
            pstmt2.setTimestamp(2, new Timestamp(start.getTime() + 6000));
            pstmt2.execute();
        }

        String selectSql = "SELECT id FROM test_timestamp";
        try (Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(selectSql)) {
            assertTrue(rs.next());
            int r1 = rs.getInt(1);
            assertNotNull(r1);
            assertEquals(6, r1);
        }

        TestUtil.dropTable(con, "test_timestamp");
    }

    @Test
    public void testTimeZone2() throws Exception {
        TestUtil.createTable(con, "test_timeZone", "id timestamp");
        String sql = "INSERT INTO test_timeZone VALUES (?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            java.util.Date start = new java.util.Date();
            pstmt.setTimestamp(1, new Timestamp(start.getTime()));
            pstmt.execute();
        }

        TestUtil.dropTable(con, "test_timeZone");
    }

    @Test
    public void testTimeRange() throws Exception {
        TestUtil.createTable(con, "test_TimeRange", "id int, c2 timestamp");
        String sql = "INSERT INTO test_TimeRange VALUES (?, ?)";
        java.util.Date start = new java.util.Date();
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setTimestamp(2, new Timestamp(start.getTime() - 100000));
            pstmt.execute();

            pstmt.setInt(1, 2);
            pstmt.setTimestamp(2, new Timestamp(start.getTime()));
            pstmt.execute();

            pstmt.setInt(1, 3);
            pstmt.setTimestamp(2, new Timestamp(start.getTime() + 500000));
            pstmt.execute();

            pstmt.setInt(1, 4);
            pstmt.setTimestamp(2, new Timestamp(start.getTime() + 1000000000));
            pstmt.execute();
        }

        String selectSql = "select id, c2 from test_TimeRange where c2 between ? and ?;";
        try (PreparedStatement pstmt2 = con.prepareStatement(selectSql)) {
            pstmt2.setTimestamp(1, new Timestamp(start.getTime()-5000));
            pstmt2.setTimestamp(2, new Timestamp(start.getTime() + 800000));
            try (ResultSet rs = pstmt2.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }

        String selectSql2 = "select id, c2 from test_TimeRange where c2 >= ? and c2 <= ?;";
        try (PreparedStatement pstmt2 = con.prepareStatement(selectSql2)) {
            pstmt2.setTimestamp(1, new Timestamp(start.getTime()-5000));
            pstmt2.setTimestamp(2, new Timestamp(start.getTime() + 800000));
            try (ResultSet rs = pstmt2.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }

        TestUtil.dropTable(con, "test_TimeRange");
    }

    @Test
    public void testYear2() throws Exception {
        TestUtil.createTable(con, "test_year2", "id year(2)");
        try (Statement stat = con.createStatement()) {
            stat.execute("INSERT INTO test_year2 VALUES (8),(69),(70),(82)");
        }

        String sql5 = "select id from test_year2";
        try (PreparedStatement pstmt2 = con.prepareStatement(sql5);
             ResultSet rs = pstmt2.executeQuery()) {
            assertTrue(rs.next());
            Date d1 = rs.getDate(1);
            assertNotNull(d1);
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2008-01-01");
            assertEquals(date, d1);
            String s1 = rs.getString(1);
            assertEquals("2008-01-01", s1);

            assertTrue(rs.next());
            Date d2 = rs.getDate(1);
            assertNotNull(d2);
            Date date2 = new SimpleDateFormat("yyyy-MM-dd").parse("2069-01-01");
            assertEquals(date2, d2);
            String s2 = rs.getString(1);
            assertEquals("2069-01-01", s2);

            assertTrue(rs.next());
            Date d3 = rs.getDate(1);
            assertNotNull(d3);
            Date date3 = new SimpleDateFormat("yyyy-MM-dd").parse("1970-01-01");
            assertEquals(date3, d3);
            String s3 = rs.getString(1);
            assertEquals("1970-01-01", s3);

            assertTrue(rs.next());
            Date d4 = rs.getDate(1);
            assertNotNull(d4);
            Date date4 = new SimpleDateFormat("yyyy-MM-dd").parse("1982-01-01");
            assertEquals(date4, d4);
            String s4 = rs.getString(1);
            assertEquals("1982-01-01", s4);
        }

        TestUtil.dropTable(con, "test_year2");
    }
}