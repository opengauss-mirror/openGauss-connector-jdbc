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
