package org.postgresql.test.dolphintest;

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

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_bit VALUES (1, 12.569, 8.753)");
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT c1,c2,c3 FROM test_bit");

        assertTrue(rs.next());
        Object o1 = rs.getObject(1);
        assertNotNull(o1);
        assertEquals(true, o1);

        String o2 = rs.getObject(2).getClass().toString();
        assertNotNull(o2);
        assertEquals("class [B", o2);

        String o3 = rs.getObject(3).getClass().toString();
        assertNotNull(o3);
        assertEquals("class [B", o3);
        TestUtil.dropTable(con, "test_bit");
    }
}
