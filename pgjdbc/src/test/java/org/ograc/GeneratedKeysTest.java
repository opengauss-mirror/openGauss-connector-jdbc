/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.ograc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * test generated keys
 *
 * @author zhangting
 * @since  2026-09-24
 */
public class GeneratedKeysTest {
    private static final String TABLE = "ograc_genkeys_t";

    private Connection conn;

    @Before
    public void setUp() throws Exception {
        conn = TestUtil.openDB();
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE
                    + "(id binary_integer auto_increment primary key, name varchar(64))");
        }
    }

    @After
    public void tearDown() {
        try {
            if (conn != null && !conn.isClosed()) {
                TestUtil.dropTable(conn, TABLE);
            }
        } catch (SQLException ignore) {
            // ignore
        }
        TestUtil.closeQuietly(conn);
    }

    @Test
    public void testStatementExecuteUpdateReturnGeneratedKeys() throws SQLException {
        try (Statement st = conn.createStatement()) {
            int count = st.executeUpdate("insert into " + TABLE + "(name) values('row-a')",
                    Statement.RETURN_GENERATED_KEYS);
            assertEquals(1, count);
            List<Long> keys = readGeneratedKeys(st.getGeneratedKeys());
            assertEquals(1, keys.size());
            assertEquals(1L, (long) keys.get(0));
        }
    }

    @Test
    public void testStatementExecuteReturnGeneratedKeys() throws SQLException {
        try (Statement st = conn.createStatement()) {
            boolean hasResultSet = st.execute("insert into " + TABLE + "(name) values('row-b')",
                    Statement.RETURN_GENERATED_KEYS);
            assertFalse(hasResultSet);
            List<Long> keys = readGeneratedKeys(st.getGeneratedKeys());
            assertEquals(1, keys.size());
            assertEquals(1L, (long) keys.get(0));
        }
    }

    @Test
    public void testStatementMultiRowInsertGeneratedKeys() throws SQLException {
        String sql = "insert into " + TABLE + "(name) values('row-1'), ('row-2'), ('row-3')";
        try (Statement st = conn.createStatement()) {
            int count = st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            assertEquals(3, count);
            List<Long> keys = readGeneratedKeys(st.getGeneratedKeys());
            assertEquals(Arrays.asList(1L, 2L, 3L), keys);
        }
    }

    @Test
    public void testStatementNoGeneratedKeys() throws SQLException {
        try (Statement st = conn.createStatement()) {
            int count = st.executeUpdate("insert into " + TABLE + "(name) values('no-keys')",
                    Statement.NO_GENERATED_KEYS);
            assertEquals(1, count);
            assertGetGeneratedKeysThrows(st);
        }
    }

    @Test
    public void testGetGeneratedKeysWithoutRequestThrows() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(name) values('plain')");
            assertGetGeneratedKeysThrows(st);
        }
    }

    @Test
    public void testPreparedStatementReturnGeneratedKeys() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(name) values(?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "ps-row");
            int count = ps.executeUpdate();
            assertEquals(1, count);
            List<Long> keys = readGeneratedKeys(ps.getGeneratedKeys());
            assertEquals(1, keys.size());
            assertEquals(1L, (long) keys.get(0));
        }
    }

    @Test
    public void testPreparedStatementExecuteWithGeneratedKeys() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(name) values(?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "ps-execute");
            assertFalse(ps.execute());
            List<Long> keys = readGeneratedKeys(ps.getGeneratedKeys());
            assertEquals(1, keys.size());
            assertEquals(1L, (long) keys.get(0));
        }
    }

    @Test
    public void testPreparedStatementWithColumnNames() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(name) values(?)",
                new String[] {"id"})) {
            ps.setString(1, "by-column-name");
            ps.executeUpdate();
            List<Long> keys = readGeneratedKeys(ps.getGeneratedKeys());
            assertEquals(1, keys.size());
            assertEquals(1L, (long) keys.get(0));
        }
    }

    @Test
    public void testPreparedStatementBatchGeneratedKeys() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(name) values(?)",
                Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 1; i <= 3; i++) {
                ps.setString(1, "batch-" + i);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(3, counts.length);
            for (int count : counts) {
                assertTrue("unexpected update count " + count,
                        count == Statement.SUCCESS_NO_INFO || count >= 0);
            }
            List<Long> keys = readGeneratedKeys(ps.getGeneratedKeys());
            assertEquals(Arrays.asList(1L, 2L, 3L), keys);
        }
    }

    @Test
    public void testGeneratedKeysMetaData() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(name) values('meta')", Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = st.getGeneratedKeys()) {
                ResultSetMetaData md = keys.getMetaData();
                assertNotNull(md);
                assertEquals(1, md.getColumnCount());
                assertTrue(keys.next());
                assertEquals(1L, keys.getLong(1));
                assertFalse(keys.next());
            }
        }
    }

    @Test
    public void testSequentialInsertsIncrementKeys() throws SQLException {
        List<Long> allKeys = new ArrayList<>();
        try (Statement st = conn.createStatement()) {
            for (int i = 1; i <= 3; i++) {
                st.executeUpdate("insert into " + TABLE + "(name) values('seq-" + i + "')",
                        Statement.RETURN_GENERATED_KEYS);
                allKeys.addAll(readGeneratedKeys(st.getGeneratedKeys()));
            }
        }
        assertEquals(Arrays.asList(1L, 2L, 3L), allKeys);
    }

    @Test
    public void testAutoIncrementAssignsSequentialIdsInTable() throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (int i = 1; i <= 3; i++) {
                insertWithGeneratedKeys(st, "ai-seq-" + i);
            }
            assertEquals(Arrays.asList(1L, 2L, 3L), queryIds(st));
        }
    }

    @Test
    public void testGeneratedKeysMatchStoredAutoIncrementValues() throws SQLException {
        try (Statement st = conn.createStatement()) {
            long key = insertWithGeneratedKeys(st, "ai-match");
            assertEquals(key, queryIdByName(st, "ai-match"));
        }
    }

    @Test
    public void testAutoIncrementContinuesAfterExistingRows() throws SQLException {
        try (Statement st = conn.createStatement()) {
            insertWithGeneratedKeys(st, "ai-first");
            insertWithGeneratedKeys(st, "ai-second");
            long thirdKey = insertWithGeneratedKeys(st, "ai-third");
            assertEquals(3L, thirdKey);
            assertEquals(3L, (long) queryIds(st).get(2));
        }
    }

    @Test
    public void testMultiRowInsertAutoIncrementValuesInTable() throws SQLException {
        String sql = "insert into " + TABLE + "(name) values('ai-m1'), ('ai-m2'), ('ai-m3')";
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            List<Long> keys = readGeneratedKeys(st.getGeneratedKeys());
            List<Long> ids = queryIds(st);
            assertEquals(Arrays.asList(1L, 2L, 3L), keys);
            assertEquals(keys, ids);
        }
    }

    @Test
    public void testPreparedStatementBatchAutoIncrementMatchesTable() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("insert into " + TABLE + "(name) values(?)",
                Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 1; i <= 3; i++) {
                ps.setString(1, "ai-batch-" + i);
                ps.addBatch();
            }
            ps.executeBatch();
            List<Long> keys = readGeneratedKeys(ps.getGeneratedKeys());
            try (Statement st = conn.createStatement()) {
                assertEquals(keys, queryIds(st));
            }
        }
    }

    @Test
    public void testAutoIncrementColumnMetadata() throws SQLException {
        try (Statement st = conn.createStatement()) {
            insertWithGeneratedKeys(st, "ai-meta");
            try (ResultSet rs = st.executeQuery("select id, name from " + TABLE + " order by id")) {
                ResultSetMetaData md = rs.getMetaData();
                assertTrue(md.isAutoIncrement(1));
                assertFalse(md.isAutoIncrement(2));
            }
        }
    }

    @Test
    public void testGeneratedKeysMetaDataMatchesAutoIncrementColumn() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(name) values('ai-keys-meta')",
                    Statement.RETURN_GENERATED_KEYS);
            try (ResultSet keys = st.getGeneratedKeys();
                 ResultSet rs = st.executeQuery("select id from " + TABLE + " where name = 'ai-keys-meta'")) {
                ResultSetMetaData keysMd = keys.getMetaData();
                assertEquals(1, keysMd.getColumnCount());
                assertTrue(keys.next());
                assertTrue(rs.next());
                assertEquals(rs.getLong(1), keys.getLong(1));
            }
        }
    }

    @Test
    public void testOmitAutoIncrementColumnOnInsert() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("insert into " + TABLE + "(name) values('ai-omit-id')");
            try (ResultSet rs = st.executeQuery("select id, name from " + TABLE + " where name = 'ai-omit-id'")) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong(1));
                assertEquals("ai-omit-id", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    private long insertWithGeneratedKeys(Statement st, String name) throws SQLException {
        st.executeUpdate("insert into " + TABLE + "(name) values('" + name + "')", Statement.RETURN_GENERATED_KEYS);
        List<Long> keys = readGeneratedKeys(st.getGeneratedKeys());
        assertEquals(1, keys.size());
        return keys.get(0);
    }

    private static List<Long> queryIds(Statement st) throws SQLException {
        List<Long> ids = new ArrayList<>();
        try (ResultSet rs = st.executeQuery("select id from " + TABLE + " order by id")) {
            while (rs.next()) {
                ids.add(rs.getLong(1));
            }
        }
        return ids;
    }

    private static long queryIdByName(Statement st, String name) throws SQLException {
        try (ResultSet rs = st.executeQuery("select id from " + TABLE + " where name = '" + name + "'")) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static List<Long> readGeneratedKeys(ResultSet keys) throws SQLException {
        List<Long> result = new ArrayList<>();
        while (keys.next()) {
            result.add(keys.getLong(1));
        }
        return result;
    }

    private static void assertGetGeneratedKeysThrows(Statement st) {
        try {
            st.getGeneratedKeys();
            fail("expected SQLException from getGeneratedKeys()");
        } catch (SQLException expected) {
        }
    }
}
