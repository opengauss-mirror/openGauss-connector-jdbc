package org.postgresql.test.jdbc4;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test-case is only for JDBC4 time methods. Take a look at
 * {@link org.postgresql.test.jdbc2.TimeTest} for base tests concerning blobs
 */
public class TimeTest {

    private Connection con;

    @Before
    public void setUp() throws Exception {
        con = TestUtil.openDB();
    }

    @After
    public void tearDown() throws Exception {
        TestUtil.closeDB(con);
    }

    @Test
    public void testIntToTime() throws SQLException {
        TestUtil.createTable(con, "test_time", "id int");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_time VALUES (?)");
        pstmt.setObject(1, 11, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, -11, Types.INTEGER);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cast(id as time) FROM test_time");

        assertTrue(rs.next());
        String str = rs.getString(1);
        assertNotNull(str);

        assertTrue(rs.next());
        String str2 = rs.getString(1);
        assertNotNull(str2);
        assertEquals(str, str2);

        TestUtil.dropTable(con, "test_time");
    }
}
