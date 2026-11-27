/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
