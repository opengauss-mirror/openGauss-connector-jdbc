/*
 * Copyright (c) Huawei Technologies Co.,Ltd. 2023. All rights reserved.
 */
package org.postgresql.v511;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.test.TestUtil;
import org.postgresql.util.ExecuteUtil;
import org.postgresql.util.RsParser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Title: the BatchTest class.
 * <p>
 * Description:
 *
 * @author justbk
 * @version [Tools 0.0.1, 2023/11/11]
 * @since 2023/11/11
 */
@RunWith(Parameterized.class)
public class BatchAutoGenerateKeysTest {
    private static Connection connStatic;
    private Connection conn;
    
    @Parameter
    public int count;
    
    @Parameter(1)
    public String sql;
    
    @Parameter(2)
    public boolean needGenerateKey;
    
    @Parameter(3)
    public int repeatBatch;
    
    @Parameter(4)
    public boolean batchMode;
    
//    @Parameters
    public static Iterable<Object[]> data1() {
        Object[] one = {1, "insert into t1 (data) values (?) returning *", true, 1, true};
        LinkedList<Object[]> results = new LinkedList<>();
        results.add(one);
        return results;
    }
    @Parameters
    public static Iterable<Object[]> datas() {
        List<Object[]> datas = new LinkedList<>();
        String[] sqls = {"insert into t1 (data) values(?) returning *", "insert into t1 (data) values(?)"};
        Integer[] counts = {1, 2, 127, 200};
        Integer[] repeatBatchs = {1, 2, 5, 6};
        for (boolean batchMode: new boolean[]{true, false}) {
            for (Integer count: counts) {
                for (String sql:sqls) {
                    for (Integer repeatBatch: repeatBatchs) {
                        datas.add(new Object[] {count, sql, true, repeatBatch, batchMode});
                        datas.add(new Object[] {count, sql, false, repeatBatch, batchMode});
                    }
                }
            }
        }
        return datas;
    }
    
    public static class BatchData {
        public int id;
        public String data;
    }

    @BeforeClass
    public static void onlyOneSetUp() throws Exception {
        Properties props = new Properties();
        props.put("batchMode", "ON");
        connStatic = createConnection(props);
        ExecuteUtil.execute(connStatic, "drop table if exists t1;");
        ExecuteUtil.execute(connStatic, "create table t1 (id serial primary key, data varchar2(100))");
    }
    
    @AfterClass
    public static void onlyOneTeardown() throws SQLException {
        ExecuteUtil.execute(connStatic, "drop table if exists t1;");
        connStatic.close();
    }
    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.put("batchMode", batchMode ? "ON" :"OFF");
        conn = createConnection(props);
        ExecuteUtil.execute(conn, "truncate t1");
    }
    
    @After
    public void tearDown() throws SQLException {
        conn.close();
    }
    
    @Test
    public void testBatchs() throws SQLException {
        System.out.println(String.format("test count=%d, sql=%s, wantKeys:%s, repeat=%s batch=%s", count, sql, needGenerateKey, repeatBatch, batchMode));
        if (needGenerateKey) {
            testBatchInsertAndGenerateKey(sql, count, repeatBatch);
        } else {
            testBatchInsert(sql, count, repeatBatch);
        }
        Assert.assertEquals(count * repeatBatch, queryAll().size());
    }
    
    
    public void testBatchInsert(String sql, int number, int repeatBatch) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int j = 0; j < repeatBatch; j ++) {
                for (int i = 0; i < number; i++) {
                    ps.setString(1, "aaa");
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }
    
    public void testBatchInsertAndGenerateKey(String sql, int number, int repeatBatch) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int j = 0; j < repeatBatch; j++) {
                for (int i = 0; i < number; i++) {
                    ps.setString(1, "aaa");
                    ps.addBatch();
                }
                ps.executeBatch();
                List<Integer> results = new LinkedList<>();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    while (rs.next()) {
                        results.add(rs.getInt(1));
                    }
                }
                Assert.assertEquals(number, results.size());
            }
        }
    }
    
    private List<BatchData> queryAll() throws SQLException {
        return ExecuteUtil.execute(conn, "select id, data from t1", new RsParser<BatchData>() {});
    }
    private static Connection createConnection(Properties props) throws Exception {
        return TestUtil.openDB(props);
    }
}
