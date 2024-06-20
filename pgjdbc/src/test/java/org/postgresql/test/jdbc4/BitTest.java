package org.postgresql.test.jdbc4;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class BitTest extends BaseTest4 {
    /*
     * Tests bit type
     */
    @Test
    public void testBit() throws Exception {
        TestUtil.createTable(con, "test_bit", "c1 bit(1),c2 bit(10),c3 bit(6)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_bit VALUES (0::bit(1), 1234::bit(10), 88::bit(6))");
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT c1,c2,c3 FROM test_bit");

        assertTrue(rs.next());
        Object o1 = rs.getObject(1);
        assertNotNull(o1);
        assertEquals(false, o1);

        Object o2 = rs.getObject(2);
        assertNotNull(o2);
        assertEquals(true, o2);

        Object o3 = rs.getObject(3);
        assertNotNull(o3);
        assertEquals(true, o3);
        TestUtil.dropTable(con, "test_bit");
    }
}
