/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test date
 *
 * @author zhangting
 * @since  2024-08-30
 */
public class DateTest extends BaseTest4B {
    /*
     * test Date type
     */
    @Test
    public void testDate() throws Exception {
        TestUtil.createTable(con, "test_date", "id date");

        String sql = "INSERT INTO test_date VALUES (?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, "0000-01-01");
            pstmt.executeUpdate();
            pstmt.setString(1, "epoch");
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM test_date")) {
            assertTrue(rs.next());
            assertEquals("0001-01-01", rs.getObject(1).toString());
            assertTrue(rs.next());
            assertEquals("1970-01-01", rs.getObject(1).toString());
        }
        TestUtil.dropTable(con, "test_date");
    }
}
