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
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test default type
 *
 * @author zhangting
 * @since  2024-09-04
 */
public class DefaultTypeTest extends BaseTest4B {
    /*
     * test Date type
     */
    @Test
    public void testDefaultType() throws Exception {
        TestUtil.createTable(con, "test_default_type",
                "c1 set('abc','ttp','mytest'),c2 enum('2012','2013','2014')");

        String sql = "INSERT INTO test_default_type VALUES ('abc','2014')";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_default_type")) {
            assertTrue(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(2, rsmd.getColumnCount());
            assertEquals(Types.CHAR, rsmd.getColumnType(1));
            assertEquals(Types.CHAR, rsmd.getColumnType(2));
            assertEquals("abc", rs.getString(1));
            assertEquals("2014", rs.getString(2));
        }
        TestUtil.dropTable(con, "test_default_type");
    }
}
