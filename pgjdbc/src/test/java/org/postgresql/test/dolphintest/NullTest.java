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

package org.postgresql.test.dolphintest;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * test null
 *
 * @author zhangting
 * @since  2024-08-05
 */
public class NullTest extends BaseTest4B {
    @Test
    public void testStringNull() throws Exception {
        TestUtil.createTable(con, "test_null", "id varchar, id2 varchar");
        try (Statement statement = con.createStatement()) {
            statement.execute("INSERT INTO test_null VALUES ('acc','uu'),('ptv','bb'),('mtt','gf')");
        }

        String sql = "select id,id2 from test_null where id in (?, ?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setObject(1, "ptv");
            pstmt.setNull(2, -3);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                String s1 = rs.getString(1);
                assertNotNull(s1);
                assertEquals("ptv", s1);

                String s2 = rs.getString(2);
                assertNotNull(s2);
                assertEquals("bb", s2);
            }
        }
        TestUtil.dropTable(con, "test_null");
    }

    @Test
    public void testIntNull() throws Exception {
        TestUtil.createTable(con, "test_null", "id int, id2 int");
        try (Statement statement = con.createStatement()) {
            statement.execute("INSERT INTO test_null VALUES (51,34),(92,44),(67,88)");
        }

        String sql = "select id,id2 from test_null where id in (?, ?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setInt(1, 92);
            pstmt.setNull(2, 1111);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                int r1 = rs.getInt(1);
                assertNotNull(r1);
                assertEquals(92, r1);

                int r2 = rs.getInt(2);
                assertNotNull(r2);
                assertEquals(44, r2);
            }
        }
        TestUtil.dropTable(con, "test_null");
    }

    @Test
    public void testDateNull() throws Exception {
        TestUtil.createTable(con, "test_null", "id date, id2 varchar");
        try (Statement statement = con.createStatement()) {
            statement.execute("INSERT INTO test_null VALUES ('2024-05-02','n1')");
            statement.execute("INSERT INTO test_null VALUES ('2024-03-15','n2')");
            statement.execute("INSERT INTO test_null VALUES ('2024-06-07','n3')");
        }

        String sql = "select id,id2 from test_null where id in (?, ?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setObject(1, "2024-03-15");
            pstmt.setNull(2, -2);
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next());
                Date r1 = rs.getDate(1);
                assertNotNull(r1);
                Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("2024-03-15");
                assertEquals(d1, r1);

                String r2 = rs.getString(2);
                assertNotNull(r2);
                assertEquals("n2", r2);
            }
        }
        TestUtil.dropTable(con, "test_null");
    }
}
