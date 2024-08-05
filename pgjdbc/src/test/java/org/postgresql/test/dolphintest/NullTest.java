package org.postgresql.test.dolphintest;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * test null param
 */
public class NullTest extends BaseTest4 {
    @Before
    public void setProperty() throws Exception {
        Properties props = new Properties();
        props.put("nullDefaultType", "12");
        con = TestUtil.openDB(props);
    }

    @Test
    public void testStringNull() throws Exception {
        TestUtil.createTable(con, "test_null", "id varchar, id2 varchar");
        try (Statement statement = con.createStatement()) {
            statement.execute("INSERT INTO test_null VALUES ('acc','uu'),('ptv','bb'),('mtt','gf')");
        }

        PreparedStatement pstmt = con.prepareStatement("select id,id2 from test_null where id in (?, ?)");
        pstmt.setObject(1, "ptv");
        pstmt.setNull(2, -3);
        try (ResultSet rs = pstmt.executeQuery()) {
            assertTrue(rs.next());
            String s1 = rs.getString(1);
            assertNotNull(s1);
            assertEquals("ptv", s1);

            String s2 = rs.getString(2);
            assertNotNull(s2);
            assertEquals("bb", s2);
        }
        pstmt.close();
        TestUtil.dropTable(con, "test_null");
    }

    @Test
    public void testIntNull() throws Exception {
        TestUtil.createTable(con, "test_null", "id int, id2 int");
        try (Statement statement = con.createStatement()) {
            statement.execute("INSERT INTO test_null VALUES (51,34),(92,44),(67,88)");
        }

        PreparedStatement pstmt = con.prepareStatement("select id,id2 from test_null where id in (?, ?)");
        pstmt.setInt(1, 92);
        pstmt.setNull(2, 1111);
        try (ResultSet rs = pstmt.executeQuery()) {
            assertTrue(rs.next());
            int r1 = rs.getInt(1);
            assertNotNull(r1);
            assertEquals(92, r1);

            int r2 = rs.getInt(2);
            assertNotNull(r2);
            assertEquals(44, r2);
        }
        pstmt.close();
        TestUtil.dropTable(con, "test_null");
    }

    @Test
    public void testDateNull() throws Exception {
        TestUtil.createTable(con, "test_null", "id date, id2 varchar");
        try (Statement statement = con.createStatement()) {
            statement.execute("INSERT INTO test_null VALUES ('2024-05-02','n1'),('2024-03-15','n2'),('2024-06-07','n3')");
        }

        PreparedStatement pstmt = con.prepareStatement("select id,id2 from test_null where id in (?, ?)");
        pstmt.setObject(1, "2024-03-15");
        pstmt.setNull(2, -2);
        try (ResultSet rs = pstmt.executeQuery()) {
            assertTrue(rs.next());
            Date r1 = rs.getDate(1);
            assertNotNull(r1);
            Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("2024-03-15");
            assertEquals(d1, r1);

            String r2 = rs.getString(2);
            assertNotNull(r2);
            assertEquals("n2", r2);
        }
        pstmt.close();
        TestUtil.dropTable(con, "test_null");
    }
}
