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

package org.postgresql.test.jdbc4;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test numeric
 *
 * @author zhangting
 * @since  2024-10-30
 */
public class NumericTest extends BaseTest4 {
    /*
     * test NaN
     */
    @Test
    public void testNaN() throws Exception {
        TestUtil.createTable(con, "test_numeric", "c1 numeric,c2 numeric(8,4),c3 decimal, c4 decimal(8,4)");
        String sql = "INSERT INTO test_numeric VALUES (?,?,?,?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int i = 1; i <= 4; i++) {
                pstmt.setString(i, "NAN");
            }
            pstmt.executeUpdate();
            for (int i = 1; i <= 4; i++) {
                pstmt.setDouble(i, 5.6);
            }
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_numeric")) {
            assertTrue(rs.next());
            assertEquals(Double.class, rs.getObject(1).getClass());
            assertEquals(Double.class, rs.getObject(2).getClass());
            assertEquals(Double.class, rs.getObject(3).getClass());
            assertEquals(Double.class, rs.getObject(4).getClass());
            assertEquals(Double.NaN, rs.getObject(1));
            assertEquals(Double.NaN, rs.getObject(2));
            assertEquals(Double.NaN, rs.getObject(3));
            assertEquals(Double.NaN, rs.getObject(4));

            assertTrue(rs.next());
            assertEquals(BigDecimal.class, rs.getObject(1).getClass());
            assertEquals(BigDecimal.class, rs.getObject(2).getClass());
            assertEquals(BigDecimal.class, rs.getObject(3).getClass());
            assertEquals(BigDecimal.class, rs.getObject(4).getClass());
        }
        TestUtil.dropTable(con, "test_numeric");
    }
}
