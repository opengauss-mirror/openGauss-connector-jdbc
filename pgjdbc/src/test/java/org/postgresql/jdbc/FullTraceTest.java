/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.jdbc;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4PG;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * test full trace.
 *
 * @author hwhbj
 * @since  2024-08-20
 */
public class FullTraceTest extends BaseTest4PG {
    private static final Integer WAIT_FLUSH_TIME = 1000;
    private static final String QUERY_STMT_HISTORY = "select db_time, net_trans_time from statement_history "
            + "where query like '%s'";
    private static final String RECORD_FULL_SQL = "set track_stmt_stat_level = 'L0,L0'";
    private static final String ENABLE_TRACE_SQL = "set enable_record_nettime = on";

    private static Connection createConnection() throws Exception {
        return TestUtil.openDB();
    }

    private static Connection createConnection(Properties props) throws Exception {
        return TestUtil.openDB(props);
    }

    private static Connection createConnection(String dbName) throws Exception {
        return TestUtil.openDB(dbName);
    }

    private List<Integer> recordCount(String sql) throws Exception {
        String querySql = String.format(QUERY_STMT_HISTORY, sql);
        try (Connection conn = createConnection("postgres");
             PreparedStatement pstmt = conn.prepareStatement(querySql);
             ResultSet rs = pstmt.executeQuery()) {
            int count1 = 0;
            int count2 = 0;
            int count3 = 0;
            while (rs.next()) {
                int dbTime = rs.getInt(1);
                int netTransTime = rs.getInt(2);
                if (dbTime == 0 && netTransTime == 0) {
                    count1++;
                } else if (dbTime > 0 && netTransTime == 0) {
                    count2++;
                } else if (dbTime > 0 && netTransTime > 0) {
                    count3++;
                }
            }
            return Arrays.asList(count1, count2, count3);
        }
    }

    private void setRecordFullSql(Connection conn) throws Exception {
        try (PreparedStatement pstmt = conn.prepareStatement(ENABLE_TRACE_SQL + ";" + RECORD_FULL_SQL)) {
            pstmt.execute();
        }
    }

    private void sendPreTimeAndFlush(Connection conn) throws Exception {
        /* send previous net_time */
        try (PreparedStatement pstmt = conn.prepareStatement("select 1;")) {
            pstmt.executeQuery();
            Thread.sleep(WAIT_FLUSH_TIME);
        }
    }

    @Test
    public void testExecuteMultiSqlPBE() throws Exception {
        String sql1 = "drop table if exists t1";
        String sql2 = "create table t1(id int)";
        List<Integer> beforeInfoSql1 = recordCount(sql1);
        List<Integer> beforeInfoSql2 = recordCount(sql2);

        /* PBEPBES */
        try (Connection conn = createConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql1 + ";" + sql2)) {
            setRecordFullSql(conn);
            pstmt.execute();
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql1 = recordCount(sql1);
            List<Integer> afterInfoSql2 = recordCount(sql2);

            assertTrue(beforeInfoSql1.get(0) + 1 == afterInfoSql1.get(0));
            assertTrue(beforeInfoSql1.get(1) + 1 == afterInfoSql1.get(1));
            assertTrue(beforeInfoSql1.get(2) == afterInfoSql1.get(2));

            assertTrue(beforeInfoSql2.get(0) + 1 == afterInfoSql2.get(0));
            assertTrue(beforeInfoSql2.get(1) == afterInfoSql2.get(1));
            assertTrue(beforeInfoSql2.get(2) + 1 == afterInfoSql2.get(2));
        }
    }

    @Test
    public void testExecuteBatchPBENoCache() throws Exception {
        Properties props = new Properties();
        props.setProperty("prepareThreshold", "0");
        String sql1 = "drop table if exists t1; create table t1(id int);";
        String sql2 = "insert into t1 values(?)";
        String sql2Record = "insert into t1 values(%)";
        List<Integer> beforeInfoSql2 = recordCount(sql2Record);
        /* PUES / PUES */
        try (Connection conn = createConnection(props);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(sql2)) {
            stmt.execute(sql1);
            setRecordFullSql(conn);

            for (int i = 0; i < 5000; ++i) {
                pstmt.setInt(1, i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            for (int i = 0; i < 5000; ++i) {
                pstmt.setInt(1, i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql2 = recordCount(sql2Record);

            assertTrue(beforeInfoSql2.get(0) + 2 == afterInfoSql2.get(0));
            assertTrue(beforeInfoSql2.get(1) == afterInfoSql2.get(1));
            assertTrue(beforeInfoSql2.get(2) + 2 == afterInfoSql2.get(2));
        }
    }

    @Test
    public void testExecuteBatchPBEUseCache() throws Exception {
        Properties props = new Properties();
        props.setProperty("prepareThreshold", "1");
        String sql1 = "drop table if exists t1; create table t1(id int);";

        String sql2 = "insert into t1 values(?)";
        String sql2Record = "insert into t1 values(%)";
        List<Integer> beforeInfoSql2 = recordCount(sql2Record);

        /* P/DS/S / UES / UES */
        try (Connection conn = createConnection(props);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(sql2)) {
            stmt.execute(sql1);
            setRecordFullSql(conn);
            for (int i = 0; i < 5000; ++i) {
                pstmt.setInt(1, i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            for (int i = 0; i < 5000; ++i) {
                pstmt.setInt(1, i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql2 = recordCount(sql2Record);

            assertTrue(beforeInfoSql2.get(0) + 1 == afterInfoSql2.get(0));
            assertTrue(beforeInfoSql2.get(1) == afterInfoSql2.get(1));
            assertTrue(beforeInfoSql2.get(2) + 2 == afterInfoSql2.get(2));
        }
    }

    @Test
    public void testExecutePBENoCache() throws Exception {
        Properties props = new Properties();
        props.setProperty("prepareThreshold", "0");
        String sql1 = "drop table if exists t1; create table t1(id int, age int);";

        String sql2 = "select ? from t1;";
        String sql2Record = "select%from t1";
        List<Integer> beforeInfoSql2 = recordCount(sql2Record);

        /* PBES / PBES */
        try (Connection conn = createConnection(props);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(sql2)) {
            stmt.execute(sql1);
            setRecordFullSql(conn);

            pstmt.setString(1, "id");
            pstmt.execute();
            pstmt.setString(1, "age");
            pstmt.execute();
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql2 = recordCount(sql2Record);
            assertTrue(beforeInfoSql2.get(0) + 2 == afterInfoSql2.get(0));
            assertTrue(beforeInfoSql2.get(1) == afterInfoSql2.get(1));
            assertTrue(beforeInfoSql2.get(2) + 2 == afterInfoSql2.get(2));
        }
    }

    @Test
    public void testExecutePBEUseCache() throws Exception {
        Properties props = new Properties();
        props.setProperty("prepareThreshold", "1");
        String sql1 = "drop table if exists t1; create table t1(id int, age int);";

        String sql2 = "select ? from t1;";
        String sql2Record = "select%from t1";
        List<Integer> beforeInfoSql2 = recordCount(sql2Record);

        /* PBES/BES */
        try (Connection conn = createConnection(props);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(sql2)) {
            stmt.execute(sql1);
            setRecordFullSql(conn);

            pstmt.setString(1, "id");
            pstmt.execute();
            pstmt.setString(1, "age");
            pstmt.execute();
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql2 = recordCount(sql2Record);
            assertTrue(beforeInfoSql2.get(0) + 1 == afterInfoSql2.get(0));
            assertTrue(beforeInfoSql2.get(1) == afterInfoSql2.get(1));
            assertTrue(beforeInfoSql2.get(2) + 2 == afterInfoSql2.get(2));
        }
    }

    @Test
    public void testExecuteFetchSize() throws Exception {
        String sql1 = "drop table if exists t1; create table t1(id int)";
        String sql2 = "insert into t1 values(?)";
        String sql3 = "select * from t1";
        List<Integer> beforeInfoSql3 = recordCount(sql3);
        ResultSet rst = null;

        try (Connection conn = createConnection();
        Statement stmt = conn.createStatement();
        PreparedStatement pstmt = conn.prepareStatement(sql2);
        PreparedStatement pstmt1 = conn.prepareStatement(sql3)) {
            /* prepare data */
            stmt.execute(sql1);

            for (int i = 0; i < 5000; ++i) {
                pstmt.setInt(1, i);
                pstmt.addBatch();
            }
            pstmt.executeBatch();

            conn.setAutoCommit(false);
            setRecordFullSql(conn);

            pstmt1.setFetchSize(1000);
            /* PBDES/ES/ES/ES/ES/ES */
            rst = pstmt1.executeQuery();
            while (rst.next()) {
                rst.getInt(1);
            }
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql3 = recordCount(sql3);
            assertTrue(beforeInfoSql3.get(0) + 1 == afterInfoSql3.get(0));
            assertTrue(beforeInfoSql3.get(1) == afterInfoSql3.get(1));
            assertTrue(beforeInfoSql3.get(2) + 6 == afterInfoSql3.get(2));
        } finally {
            if (rst != null) {
                rst.close();
            }
        }
    }

    @Test
    public void testExecuteMultiSqlQ() throws Exception {
        Properties props = new Properties();
        props.setProperty("preferQueryMode", "simple");

        String sql1 = "drop table if exists t1";
        String sql2 = "create table t1(id int)";
        List<Integer> beforeInfoSql1 = recordCount(sql1);
        List<Integer> beforeInfoSql2 = recordCount(sql2);
        try (Connection conn = createConnection(props);
             PreparedStatement pstmt = conn.prepareStatement(sql1 + ";" + sql2)) {
            setRecordFullSql(conn);
            /* Q -> Q */
            pstmt.execute();
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql1 = recordCount(sql1);
            List<Integer> afterInfoSql2 = recordCount(sql2);

            assertTrue(beforeInfoSql1.get(0) == afterInfoSql1.get(0));
            assertTrue(beforeInfoSql1.get(1) + 1 == afterInfoSql1.get(1));
            assertTrue(beforeInfoSql1.get(2) == afterInfoSql1.get(2));

            assertTrue(beforeInfoSql2.get(0) == afterInfoSql2.get(0));
            assertTrue(beforeInfoSql2.get(1) == afterInfoSql2.get(1));
            assertTrue(beforeInfoSql2.get(2) + 1 == afterInfoSql2.get(2));
        }
    }

    @Test
    public void testExecuteQ() throws Exception {
        Properties props = new Properties();
        props.setProperty("preferQueryMode", "simple");

        String sql1 = "drop table if exists t1; create table t1(id int, age int)";
        String sql2 = "select ? from t1";
        String sql2Record = "select%from t1";
        List<Integer> beforeInfoSql2 = recordCount(sql2Record);

        try (Connection conn = createConnection(props);
             Statement stmt = conn.createStatement();
             PreparedStatement pstmt = conn.prepareStatement(sql2)) {
            stmt.execute(sql1);
            setRecordFullSql(conn);

            /* Q / Q */
            pstmt.setString(1, "id");
            pstmt.execute();
            pstmt.setString(1, "age");
            pstmt.execute();
            sendPreTimeAndFlush(conn);

            List<Integer> afterInfoSql2 = recordCount(sql2Record);
            assertTrue(beforeInfoSql2.get(0) == afterInfoSql2.get(0));
            assertTrue(beforeInfoSql2.get(1) == afterInfoSql2.get(1));
            assertTrue(beforeInfoSql2.get(2) + 2 == afterInfoSql2.get(2));
        }
    }
}
