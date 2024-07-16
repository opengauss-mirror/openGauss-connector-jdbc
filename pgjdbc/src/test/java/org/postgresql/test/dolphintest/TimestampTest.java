package org.postgresql.test.dolphintest;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TimestampTest extends BaseTest4 {
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
}