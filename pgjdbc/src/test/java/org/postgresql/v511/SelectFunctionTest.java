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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
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

    private static Connection createConnection() throws Exception {
        return TestUtil.openDB();
    }
}
