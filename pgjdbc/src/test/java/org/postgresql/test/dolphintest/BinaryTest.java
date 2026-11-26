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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test binary
 *
 * @author zhangting
 * @since  2024-08-20
 */
public class BinaryTest extends BaseTest4B {
    @Test
    public void testBinary1() throws SQLException {
        TestUtil.createTable(con, "test_binary_b", "id int, c1 binary(5)");
        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_binary_b VALUES (?,?)")) {
            pstmt.setInt(1, 1);
            try (InputStream data = new ByteArrayInputStream("abcde".getBytes(StandardCharsets.UTF_8))) {
                pstmt.setBinaryStream(2, data);
                pstmt.execute();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String sql = "INSERT INTO test_binary_b VALUES (2,'abcde'::binary)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.execute();
        }

        try (Statement statement = con.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM test_binary_b")) {
            while (rs.next()) {
                assertEquals("\\x6162636465", new String(rs.getBytes(2),
                        StandardCharsets.UTF_8));
            }
        }
        TestUtil.dropTable(con, "test_binary_b");
    }

    @Test
    public void testBinary2() throws SQLException {
        try (Statement stat = con.createStatement()) {
            try (ResultSet rs = stat.executeQuery("SELECT '10101'::binary(5)")) {
                while (rs.next()) {
                    assertEquals("\\x3130313031", new String(rs.getBytes(1),
                            StandardCharsets.UTF_8));
                }
            }
            try (ResultSet rs = stat.executeQuery("SELECT cast('abc' as binary)")) {
                while (rs.next()) {
                    assertEquals("\\x616263", rs.getString(1));
                }
            }

            stat.execute("set bytea_output=escape;");
            try (ResultSet rs = stat.executeQuery("SELECT '10101'::binary(5)")) {
                while (rs.next()) {
                    assertEquals("10101", new String(rs.getBytes(1),
                            StandardCharsets.UTF_8));
                }
            }
            try (ResultSet rs = stat.executeQuery("SELECT cast('abc' as binary)")) {
                while (rs.next()) {
                    assertEquals("abc", rs.getString(1));
                }
            }
        }
    }

    /*
     * test binary type
     */
    @Test
    public void testBinaryType() throws Exception {
        TestUtil.createTable(con, "test_binary", "c1 binary,c2 binary(5),"
                + "c3 varbinary,c4 varbinary(5)");

        try (Statement stat = con.createStatement()) {
            stat.execute("set bytea_output=escape;");
            stat.executeUpdate("INSERT INTO test_binary VALUES ('a','abcde','a','abcde')");
        }
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_binary")) {
            assertTrue(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(4, rsmd.getColumnCount());
            assertEquals(Types.BINARY, rsmd.getColumnType(1));
            assertEquals(Types.BINARY, rsmd.getColumnType(2));
            assertEquals(Types.VARBINARY, rsmd.getColumnType(3));
            assertEquals(Types.VARBINARY, rsmd.getColumnType(4));

            assertEquals("a", rs.getString(1));
            assertEquals("abcde", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertEquals("abcde", rs.getString(4));
        }
        TestUtil.dropTable(con, "test_binary");
    }
}
