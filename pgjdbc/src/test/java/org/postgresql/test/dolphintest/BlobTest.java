/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package org.postgresql.test.dolphintest;

import org.junit.Test;
import org.postgresql.core.types.PGBlob;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BlobTest extends BaseTest4B {

    protected void updateProperties(Properties props) {
        super.updateProperties(props);
        props.put("blobMode", "ON");
        props.put("binaryTransfer", "true");
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeMiniOgVersion("opengauss 6.0.0",6,0,0);
        TestUtil.createTable(con, "test_blob_b", "id int, data1 tinyblob, data2 blob, data3 mediumblob, data4 longblob");
    }

    @Override
    public void tearDown() throws SQLException {
        TestUtil.dropTable(con, "test_blob_b");
        super.tearDown();
    }

    public static void executeSql(Connection connection, String sql) throws SQLException {
        try (PreparedStatement st = connection.prepareStatement(sql)) {
            st.execute();
        }
    }

    @Test
    public void test1() throws Exception {
        String sqlQuery = "select * from test_blob_b";
        String sqlInsert = "insert into test_blob_b values (?, ?, ?, ?, ?);";

        con.unwrap(PgConnection.class).setDolphinCmpt(true);
        try (PreparedStatement ps = con.prepareStatement(sqlInsert)) {
            ps.setInt(1, 1);
            PGBlob blob = new PGBlob();
            blob.setBytes(1, "abcdefgh\0ijklmn".getBytes(StandardCharsets.UTF_8));
            ps.setBlob(2, blob);
            ps.setBlob(3, blob);
            ps.setBlob(4, blob);
            ps.setBlob(5, blob);
            ps.execute();
        }
        Statement statement = con.createStatement();
        ResultSet set = statement.executeQuery(sqlQuery);
        while (set.next()) {
            assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(2).getBytes(1, 15), StandardCharsets.UTF_8));
            assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(3).getBytes(1, 15), StandardCharsets.UTF_8));
            assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(4).getBytes(1, 15), StandardCharsets.UTF_8));
            assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(5).getBytes(1, 15), StandardCharsets.UTF_8));
        }

    }

    @Test
    public void test2() throws Exception {
        String sqlQuery = "select * from test_blob_b";
        ResultSet set1 = null;
        ResultSet set2 = null;
        try {
            con.unwrap(PgConnection.class).setDolphinCmpt(true);
            Statement statement = con.createStatement();
            set1 = statement.executeQuery(sqlQuery);
            while (set1.next()) {
                assertEquals("abcdefgh\0ijklmn", set1.getString(2));
                assertEquals("abcdefgh\0ijklmn", set1.getString(3));
                assertEquals("abcdefgh\0ijklmn", set1.getString(4));
                assertEquals("abcdefgh\0ijklmn", set1.getString(5));
            }
            con.unwrap(PgConnection.class).setDolphinCmpt(false);
            set2 = statement.executeQuery(sqlQuery);
            while (set2.next()) {
                assertEquals("abcdefgh\0ijklmn", set2.getString(2));
                assertEquals("616263646566676800696A6B6C6D6E", set2.getString(3));
                assertEquals("abcdefgh\0ijklmn", set2.getString(4));
                assertEquals("abcdefgh\0ijklmn", set2.getString(5));
            }
        } finally {
            if (set1 != null) {
                set1.close();
            }
            if (set2 != null) {
                set2.close();
            }
        }
    }

    @Test
    public void testStringToBlob() throws SQLException {
        String sql = "INSERT INTO test_blob_b VALUES (2,'1234'::tinyblob,"
                + "'1234'::blob,'1234'::mediumblob,'1234'::longblob)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.execute();
        }

        try (Statement statement = con.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM test_blob_b")) {
            while (rs.next()) {
                assertEquals("1234", new String(rs.getBlob(2).getBytes(1, 4),
                        StandardCharsets.UTF_8));
                assertEquals("1234", new String(rs.getBlob(3).getBytes(1, 4),
                        StandardCharsets.UTF_8));
                assertEquals("1234", new String(rs.getBlob(4).getBytes(1, 4),
                        StandardCharsets.UTF_8));
                assertEquals("1234", new String(rs.getBlob(5).getBytes(1, 4),
                        StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * test blob by getBytes
     *
     * @throws SQLException  sql exception
     */
    @Test
    public void testBlobToBytes() throws SQLException {
        String sql = "INSERT INTO test_blob_b VALUES (1,'abcd'::tinyblob,"
                + "'abcd'::blob,'abcd'::mediumblob,'abcd'::longblob)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.execute();
        }

        try (Statement statement = con.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM test_blob_b")) {
            assertTrue(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(Types.LONGVARBINARY, rsmd.getColumnType(3));
            assertEquals("abcd", new String(rs.getBytes(2),
                    StandardCharsets.UTF_8));
            assertEquals("abcd", new String(rs.getBytes(3),
                    StandardCharsets.UTF_8));
            assertEquals("abcd", new String(rs.getBytes(4),
                    StandardCharsets.UTF_8));
            assertEquals("abcd", new String(rs.getBytes(5),
                    StandardCharsets.UTF_8));
        }
    }
}
