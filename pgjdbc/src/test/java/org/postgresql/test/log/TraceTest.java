package org.postgresql.test.log;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.UUID;

public class TraceTest extends BaseTest4 {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TestUtil.createTable(con, "test_trace_id", "id int,name varchar(100)");
    }

    @After
    public void tearDown() throws SQLException {
        TestUtil.dropTable(con, "test_trace_id");
    }

    /**
     * Use statement to execute single group and multiple groups of SQL, traceId is correctly associated.
     *
     * @throws SQLException if a JDBC or database problem occurs.
     */
    @Test
    public void testStatementSendTraceId() throws SQLException {
        OpenGaussTraceImpl openGaussTrace = new OpenGaussTraceImpl();
        Statement stmt = null;
        try {
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            // simulate one request with one sql.
            String traceId1 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId1);
            stmt.execute("insert into test_trace_id values(1,'test1')");
            openGaussTrace.reset();
            ResultSet rs1 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId1 + "'");
            rs1.last();
            Assert.assertEquals(1, rs1.getRow());

            // simulate one request with multiple sql.
            String traceId2 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId2);
            stmt.execute("insert into test_trace_id values(2,'test2')");
            stmt.execute("select count(1) from test_trace_id");
            openGaussTrace.reset();
            ResultSet rs2 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId2 + "'");
            rs2.last();
            Assert.assertEquals(2, rs1.getRow());
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    /**
     * Use PreparedStatement to execute single group and multiple groups of SQL, traceId is correctly associated.
     *
     * @throws SQLException if a JDBC or database problem occurs.
     */
    @Test
    public void testPrepareStatementSendTraceId() throws SQLException {
        OpenGaussTraceImpl openGaussTrace = new OpenGaussTraceImpl();
        PreparedStatement pstm = null;
        Statement stmt = null;
        try {
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            // simulate one request with one sql.
            String traceId1 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId1);
            pstm = con.prepareStatement("insert into test_trace_id values(?,?)");
            pstm.setInt(1, 3);
            pstm.setString(2, "test3");
            pstm.execute();
            openGaussTrace.reset();
            ResultSet rs1 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId1 + "'");
            rs1.last();
            Assert.assertEquals(1, rs1.getRow());

            // simulate one request with multiple sql.
            String traceId2 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId2);
            pstm = con.prepareStatement("select * from test_trace_id where id = ?");
            pstm.setInt(1, 3);
            pstm.execute();
            pstm = con.prepareStatement("insert into test_trace_id values(?,?)");
            pstm.setInt(1, 4);
            pstm.setString(2, "test4");
            pstm.execute();
            openGaussTrace.reset();
            openGaussTrace.reset();
            ResultSet rs2 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId2 + "'");
            rs1.last();
            Assert.assertEquals(2, rs2.getRow());
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(pstm);
        }
    }

    /**
     * Use CallableStatement to execute single group and multiple groups of SQL, traceId is correctly associated.
     *
     * @throws SQLException if a JDBC or database problem occurs.
     */
    @Test
    public void testCallableStatementendTraceId() throws SQLException {
        OpenGaussTraceImpl openGaussTrace = new OpenGaussTraceImpl();
        Statement stmt = null;
        CallableStatement cs = null;
        try {
            stmt.execute("create function test_func1() " +
                    "RETURNS int AS " +
                    "$$ " +
                    "DECLARE " +
                    "total int; " +
                    "BEGIN " +
                    "select 255 into total;" +
                    "return total; " +
                    "END " +
                    "$$ LANGUAGE PLPGSQL;");
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            // simulate one request with one sql.
            String traceId1 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId1);
            cs = con.prepareCall("{? = call test_func1()}");
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            openGaussTrace.reset();
            ResultSet rs1 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId1 + "'");
            rs1.last();
            Assert.assertEquals(1, rs1.getRow());

            // simulate one request with multiple sql.
            String traceId2 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId2);
            cs = con.prepareCall("{? = call test_func1()}");
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            cs = con.prepareCall("{? = call test_func1()}");
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            openGaussTrace.reset();
            ResultSet rs2 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId2 + "'");
            rs2.last();
            Assert.assertEquals(2, rs1.getRow());
        } finally {
            TestUtil.closeQuietly(stmt);
            TestUtil.closeQuietly(cs);
        }
    }

    /**
     * Batch execution scenarios.
     *
     * @throws SQLException if a JDBC or database problem occurs.
     */
    @Test
    public void testBatchTraceId() throws SQLException {
        OpenGaussTraceImpl openGaussTrace = new OpenGaussTraceImpl();
        PreparedStatement pstm = null;
        Statement stmt = null;
        try {
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            // if it exceeds 32 bits, it will be intercepted
            String traceId1 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId1 + "test");
            pstm = con.prepareStatement("insert into test_trace_id values(?,?)");
            pstm.setInt(1, 8);
            pstm.setString(2, "test8");
            pstm.addBatch();
            pstm.setInt(1, 9);
            pstm.setString(2, "test9");
            pstm.addBatch();
            pstm.execute();
            openGaussTrace.reset();
            ResultSet rs1 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId1 + "'");
            rs1.last();
            Assert.assertEquals(1, rs1.getRow());
        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

    /**
     * Verify that the length exceeds 32 bits or is empty
     *
     * @throws SQLException if a JDBC or database problem occurs.
     */
    @Test
    public void testLengthNotmatchTraceId() throws SQLException {
        OpenGaussTraceImpl openGaussTrace = new OpenGaussTraceImpl();
        Statement stmt = null;
        try {
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            // if it exceeds 32 bits, it will be intercepted
            String traceId1 = UUID.randomUUID().toString().replaceAll("-", "");
            openGaussTrace.set(traceId1 + "test");
            stmt.execute("insert into test_trace_id values(5,'test5')");
            openGaussTrace.reset();
            ResultSet rs1 =
                    stmt.executeQuery("select query,trace_id from statement_history where trace_id='" + traceId1 + "'");
            rs1.last();
            Assert.assertEquals(1, rs1.getRow());

            // empty string scene,output warning log
            openGaussTrace.set("");
            stmt.execute("insert into test_trace_id values(6,'test6')");
            openGaussTrace.reset();

            // null.
            openGaussTrace.set(null);
            stmt.execute("insert into test_trace_id values(7,'test7')");
            openGaussTrace.reset();

        } finally {
            TestUtil.closeQuietly(stmt);
        }
    }

}