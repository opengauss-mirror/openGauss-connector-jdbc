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

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * test numeric
 *
 * @author zhangting
 * @since  2024-08-20
 */
public class NumericTest extends BaseTest4B {
    /*
     * test numeric type
     */
    @Test
    public void testNumeric() throws Exception {
        TestUtil.createTable(con, "test_numeric", "c1 numeric,c2 numeric(8,4),c3 float,"
                + "c4 float(8,4),c5 double,c6 double(8,4),c7 real,c8 real(8,4),c9 decimal(8,4)");

        String sql = "INSERT INTO test_numeric VALUES (?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int i = 1; i <= 9; i++) {
                pstmt.setDouble(i, 92.456739023);
            }
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_numeric")) {
            assertTrue(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(9, rsmd.getColumnCount());
            assertEquals(Types.NUMERIC, rsmd.getColumnType(1));
            assertEquals(Types.NUMERIC, rsmd.getColumnType(2));
            assertEquals(Types.REAL, rsmd.getColumnType(3));
            assertEquals(Types.NUMERIC, rsmd.getColumnType(4));
            assertEquals(Types.DOUBLE, rsmd.getColumnType(5));
            assertEquals(Types.NUMERIC, rsmd.getColumnType(6));
            assertEquals(Types.REAL, rsmd.getColumnType(7));
            assertEquals(Types.NUMERIC, rsmd.getColumnType(8));
            assertEquals(Types.NUMERIC, rsmd.getColumnType(9));

            assertEquals(new BigDecimal(92), rs.getObject(1));
            assertEquals(new String("92.4567"), rs.getString(2));
            assertEquals(new String("92.4567"), rs.getString(4));
            assertEquals(new String("92.4567"), rs.getString(6));
            assertEquals(new String("92.4567"), rs.getString(8));
            assertEquals(new String("92.4567"), rs.getString(9));
        }
        TestUtil.dropTable(con, "test_numeric");
    }
}
