package org.postgresql.test.jdbc4;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class EncodingTest extends BaseTest4 {
    /*
     * Tests encoding change
     */
    @Test
    public void testEncodingChange() throws Exception {
        Properties properties = new Properties();
        properties.put("preferQueryMode", "simple");
        properties.put("allowEncodingChanges", "true");
        properties.put("characterEncoding", "SQL_ASCII");
        con = TestUtil.openDB(properties);

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("show client_encoding");

        assertTrue(rs.next());
        String e = rs.getString(1);
        assertNotNull(e);
        assertEquals("SQL_ASCII", e);

        TestUtil.createTable(con, "test_encode", "id varchar");
        String str = "abcde1234l&&&&&7$$";
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_encode VALUES (?);");
        pstmt.setObject(1, str, Types.VARCHAR);
        pstmt.executeUpdate();
        ResultSet rsv = stmt.executeQuery("SELECT id FROM test_encode;");
        assertTrue(rsv.next());
        String s = rsv.getString(1);
        assertNotNull(s);
        assertEquals(str, s);
        TestUtil.dropTable(con, "test_encode");

        Properties properties2 = new Properties();
        properties2.put("characterEncoding", "GBK");
        con = TestUtil.openDB(properties2);

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("show client_encoding");

        assertTrue(rs2.next());
        String e2 = rs2.getString(1);
        assertNotNull(e2);
        assertEquals("GBK", e2);

        Properties properties3 = new Properties();
        properties3.put("characterEncoding", "LATIN2");
        con = TestUtil.openDB(properties3);

        Statement stmt3 = con.createStatement();
        ResultSet rs3 = stmt3.executeQuery("show client_encoding");

        assertTrue(rs3.next());
        String e3 = rs3.getString(1);
        assertNotNull(e3);
        assertEquals("LATIN2", e3);
    }
}
