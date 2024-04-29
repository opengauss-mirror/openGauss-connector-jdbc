/*
 * Copyright (c) Huawei Technologies Co.,Ltd. 2023. All rights reserved.
 */

package org.postgresql.v511;

import org.junit.Assert;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.util.ExecuteUtil;
import org.postgresql.util.RsParser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;

/**
 * Title: the BitTest class.
 * <p>
 * Description:
 *
 * @author justbk
 * @version [Tools 0.0.1, 2023/11/3]
 * @since 2023/11/3
 */
public class SelectFunctionTest {
 
    public static class Memory {
        long total;
        long free;
        int count;
    }
    
    @Test
    public void testFunctionQuery() throws Exception {
        String sql = "SELECT 'PL/pgSQL function context' context_name\n" +
                ",sum(totalsize) / 1024 / 1024 / 1024 AS \"total\"\n" +
                ",sum(freesize) / 1024 / 1024 / 1024 AS \"free\"\n" +
                ",count(*) as count\n" +
                "FROM gs_thread_memory_context\n" +
                "WHERE contextname LIKE 'PL/pgSQL%'";
        try (Connection conn = createConnection()) {
            List<Memory> results = ExecuteUtil.execute(conn, sql, new RsParser<Memory>() {});
            Assert.assertNotNull(results);
        }
    }

    @Test
    public void testFunctionQuery1() throws Exception {
        String sql = "    SELECT 'PL/pgSQL function context' context_name\n" +
                ",sum(totalsize) / 1024 / 1024 / 1024 AS \"total\"\n" +
                ",sum(freesize) / 1024 / 1024 / 1024 AS \"free\"\n" +
                ",count(*) as count\n" +
                "FROM gs_thread_memory_context\n" +
                "WHERE contextname LIKE 'PL/pgSQL%'";
        try (Connection conn = createConnection()) {
            List<Memory> results = ExecuteUtil.execute(conn, sql, new RsParser<Memory>() {});
            Assert.assertNotNull(results);
        }
    }
    @Test
    public void testBatchInsert() throws Exception {
        Properties props = new Properties();
        props.put("preparedStatementCacheQueries", "2");
        props.put("prepareThreshold", "2");
        props.put("fetchSize", "5");
        props.put("batchMode", "OFF");
        props.put("reWriteBatchedInserts", "true");
        try (Connection conn = TestUtil.openDB(props)) {
            for (int j = 1; j <= 100; j++) {
                ExecuteUtil.execute(conn, "set session_timeout = 0;");
                ExecuteUtil.execute(conn, "drop table if exists t" + j);
                ExecuteUtil.execute(conn, "create table t" + j
                    + "(id int, id1 int, id2 int, id3 int, id4 int, id5 int, data varchar(2048));");
                String batchInsert = "insert into t" + j + " values (?,?,?,?,?,?,?)";
                PreparedStatement preparedStatement = conn.prepareStatement(batchInsert);
                for (int i = 1; i <= 100; i++) {
                    preparedStatement.setInt(1, 1);
                    preparedStatement.setInt(2, i);
                    preparedStatement.setInt(3, i);
                    preparedStatement.setInt(4, i);
                    preparedStatement.setInt(5, i);
                    preparedStatement.setInt(6, i);
                    preparedStatement.setString(7, "Huawei");
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                preparedStatement.close();
                ExecuteUtil.execute(conn, "drop table if exists t" + j);
            }
            // block
        }
    }

    private static Connection createConnection() throws Exception {
        return TestUtil.openDB();
    }
}
