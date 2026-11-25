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

package org.postgresql.test.jdbc4;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Test case for cleanupSavepoints property
 *
 * @author zhangting
 * @since  2024-09-10
 */
public class CleanUpSavePointsPropertyTest extends BaseTest4 {
    @Before
    public void setUp() throws Exception {
        Properties props = new Properties();
        props.put("autosave", "always");
        props.put("cleanupSavepoints", "true");
        con = TestUtil.openDB(props);
        TestUtil.createTable(con, "savepoint_table", "id int primary key, name varchar(16)");
        con.setAutoCommit(false);
    }

    @After
    public void tearDown() throws SQLException {
        con.setAutoCommit(true);
        TestUtil.dropTable(con, "savepoint_table");
        super.tearDown();
    }

    @Test
    public void test() throws SQLException {
        // add record
        IntStream.range(1, 6).forEach(i -> {
            try {
                addRecord(i, "xw" + i);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        });
        // update record
        IntStream.range(1, 3).forEach(i -> {
            try {
                updateRecord(i, "name" + i + "-" + i);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        // delete record
        IntStream.range(4, 6).forEach(i -> {
            try {
                deleteRecord(i);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        // count record
        assertEquals(3, countRecord());
    }

    private void addRecord(int id, String name) throws SQLException {
        String sql = "INSERT INTO savepoint_table(id, name) VALUES (?, ?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            pstmt.setString(2, name);
            pstmt.executeUpdate();
        }
    }

    private void updateRecord(int id, String name) throws SQLException {
        String sql = "update savepoint_table set name= ? where id= ?";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, name);
            pstmt.setInt(2, id);
            pstmt.executeUpdate();
        }
    }

    private void deleteRecord(int id) throws SQLException {
        String sql = "delete from savepoint_table where id= ?";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            pstmt.executeUpdate();
        }
    }

    private int countRecord() throws SQLException {
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM savepoint_table")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
