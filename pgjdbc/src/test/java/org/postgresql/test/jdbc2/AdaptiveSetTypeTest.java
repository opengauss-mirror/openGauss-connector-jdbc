package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.fail;

public class AdaptiveSetTypeTest extends BaseTest4{
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtil.createTable(con, "test_numeric", "f_member_id character(6) NOT NULL,f_register_capital numeric(18,0)");
    }

    @After
    public void tearDown() throws SQLException {
        TestUtil.dropTable(con, "test_numeric");
    }
    @Override
    protected void updateProperties(Properties props) {
        props.setProperty("adaptiveSetSQLType","true");
    }
    @Test
    public void AdaptiveSetTypeTrue() throws SQLException {
        PreparedStatement ps = null;
        Long a = new Long("2180000000");
        try {
            ps = con.prepareStatement("INSERT INTO test_numeric (F_MEMBER_ID,F_REGISTER_CAPITAL) VALUES (   ?,  ?)");
            ps.setString(1,"2097  ");
            ps.setNull(2, Types.INTEGER);
            ps.addBatch();
            ps.setString(1,"3020  "  );
            ps.setLong(2,a);
            ps.addBatch();
            ps.executeBatch();
        } catch (SQLException e) {
            fail(e.getMessage());
        }finally {
            TestUtil.closeQuietly(ps);
        }
    }
}
