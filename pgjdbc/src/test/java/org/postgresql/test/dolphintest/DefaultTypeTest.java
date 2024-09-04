/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
