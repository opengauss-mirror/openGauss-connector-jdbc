package org.postgresql.test.dolphintest;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.sql.*;

import static org.junit.Assert.*;

public class DolphinTest extends BaseTest4B {

    /**
     * test db type and dolphin mode
     * @throws Exception
     */
    @Test
    public void testUint1() throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rsDBType = stmt.executeQuery("show sql_compatibility;");

        assertTrue(rsDBType.next());
        String r1 = rsDBType.getString(1);
        assertNotNull(r1);
        assertEquals("B", r1);

        ResultSet rsMode = stmt.executeQuery("show dolphin.b_compatibility_mode;");

        assertTrue(rsMode.next());
        String r2 = rsMode.getString(1);
        assertNotNull(r2);
        assertEquals("on", r2);
    }
}
