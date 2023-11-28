/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package org.postgresql.test.dolphintest;

import org.junit.Test;
import org.postgresql.core.types.PGBlob;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class BlobTest extends BaseTest4 {
    public static void executeSql(Connection connection, String sql) throws SQLException {
        try (PreparedStatement st = connection.prepareStatement(sql)) {
            st.execute();
        }
    }

    @Test
    public void test1() throws Exception {
        String sqlCreate = "create table if not exists t1"
                            + "(id int, data1 tinyblob, data2 blob, data3 mediumblob, data4 longblob);";
        String sqlDrop = "drop table if exists t1;";
        String sqlDropUser = "drop user test_user cascade;";
        String sqlQuery = "select * from t1";
        String sqlInsert = "insert into t1 values (?, ?, ?, ?, ?);";
        String sqlCreateUser = "CREATE USER test_user with password 'openGauss@123'";
        String sqlGrantUser = "GRANT ALL PRIVILEGES TO test_user";
        Properties props = new Properties();
        props.put("blobMode", "ON");
        props.put("binaryTransfer", "true");

        /* test about not b_comp */
        try (Connection con1 = TestUtil.openDB(props)) {
            /* cannot create the table */
            executeSql(con1, sqlCreate);
            executeSql(con1, sqlDropUser);
            executeSql(con1, sqlCreateUser);
            executeSql(con1, sqlGrantUser);
        }

        /* test about b_comp but don't have dolphin plugin */
        props.put("username", "test_user");
        props.put("password", "openGauss@123");
        try (Connection con1 = TestUtil.openDB(props)) {
            /* cannot create the table */
            executeSql(con1, sqlCreate);
        }

        Properties props1 = new Properties();
        props1.put("blobMode", "ON");
        props1.put("binaryTransfer", "true");
        props1.put("database", "test_db");
        try (Connection con1 = TestUtil.openDB(props1)) {
            con1.unwrap(PgConnection.class).setDolphinCmpt(true);
            executeSql(con1, sqlDrop);
            executeSql(con1, sqlCreate);
            try (PreparedStatement ps = con1.prepareStatement(sqlInsert)) {
                ps.setInt(1, 1);
                PGBlob blob = new PGBlob();
                blob.setBytes(1, "abcdefgh\0ijklmn".getBytes(StandardCharsets.UTF_8));
                ps.setBlob(2, blob);
                ps.setBlob(3, blob);
                ps.setBlob(4, blob);
                ps.setBlob(5, blob);
                ps.execute();
            }
            Statement statement = con1.createStatement();
            ResultSet set = statement.executeQuery(sqlQuery);
            while (set.next()) {
                assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(2).getBytes(1, 15), StandardCharsets.UTF_8));
                assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(3).getBytes(1, 15), StandardCharsets.UTF_8));
                assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(4).getBytes(1, 15), StandardCharsets.UTF_8));
                assertEquals("abcdefgh\0ijklmn", new String(set.getBlob(5).getBytes(1, 15), StandardCharsets.UTF_8));
            }
        }
    }
    @Test
    public void test2() throws Exception {
        Properties props1 = new Properties();
        props1.put("blobMode", "ON");
        props1.put("binaryTransfer", "true");
        props1.put("database", "test_db");
        String sqlQuery = "select * from t1";
        ResultSet set1 = null;
        ResultSet set2 = null;
        try (Connection con1 = TestUtil.openDB(props1)) {
            con1.unwrap(PgConnection.class).setDolphinCmpt(true);
            Statement statement = con1.createStatement();
            set1 = statement.executeQuery(sqlQuery);
            while (set1.next()) {
                assertEquals("abcdefgh\0ijklmn", set1.getString(2));
                assertEquals("abcdefgh\0ijklmn", set1.getString(3));
                assertEquals("abcdefgh\0ijklmn", set1.getString(4));
                assertEquals("abcdefgh\0ijklmn", set1.getString(5));
            }
            con1.unwrap(PgConnection.class).setDolphinCmpt(false);
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
}
