/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc42;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.postgresql.PGProperty;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Title: the DeepInsertBatchAndBatchModeTest class.
 * <p>
 * Description: this to test use reWriteBatchInserts and batchMode props.
 *
 * @author justbk
 * @version [2.1.0, 2022/1/18]
 * @since 2022/1/18
 */
@RunWith(Parameterized.class)
public class DeepInsertBatchAndBatchModeTest extends BaseTest4 {
    
    private static final String TABLE_NAME = "batch_test";
    
    @Parameterized.Parameters(name = "binary = {0},batchMode = {1}, reWriteBatchedInserts = {2}")
    public static Iterable<Object[]> data() {
        Collection<Object[]> ids = new ArrayList<Object[]>();
        for (BinaryMode binaryMode : BinaryMode.values()) {
            for (String batchMode: new String[]{"ON", "OFF"}) {
                for (boolean reWriteBatched: new boolean[] {true, false}) {
                    ids.add(new Object[]{binaryMode, batchMode, reWriteBatched});
                }
            }
        }
        return ids;
    }
    
    private String batchMode;
    private boolean reWriteBatch;
    
    public DeepInsertBatchAndBatchModeTest(BinaryMode mode, String batchMode, boolean reWriteBatch) {
        this.batchMode = batchMode;
        this.reWriteBatch = reWriteBatch;
        setBinaryMode(mode);
    }
    
    private void createTable() throws SQLException  {
        TestUtil.createTable(con, TABLE_NAME, "id int primary key, data varchar(100)");
    }
    
    private void dropTable() throws SQLException {
        TestUtil.dropTable(con, TABLE_NAME);
    }
    
    @Override
    protected void updateProperties(Properties props) {
        PGProperty.REWRITE_BATCHED_INSERTS.set(props, reWriteBatch);
        props.setProperty("batchMode", this.batchMode);
    }
    
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        createTable();
    }
    
    @After
    @Override
    public void tearDown() throws SQLException {
        dropTable();
        super.tearDown();
    }
    
    @Test
    public void test() throws SQLException {
        String sql = String.format("insert into batch_test values (?, ?)");
        int batchLen = 129;
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            for (int i = 0; i < batchLen; i++) {
                ps.setInt(1, i);
                ps.setString(2, "aaa");
                ps.addBatch();
            }
            boolean bothSet = batchMode.equals("ON") && reWriteBatch;
            try {
                int[] result = ps.executeBatch();
                if (bothSet) {
                    fail("both set batchMode and reWriteBatchedInserts can not run here!");
                }
                assertEquals(batchLen, result.length);
            } catch (SQLException sqlExp) {
                if (!bothSet) {
                    throw sqlExp;
                }
            }
        }
    }
}
