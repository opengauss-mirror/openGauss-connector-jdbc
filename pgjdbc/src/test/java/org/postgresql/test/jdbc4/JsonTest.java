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

package org.postgresql.test.jdbc4;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test json
 *
 * @author zhangting
 * @since  2024-08-20
 */
public class JsonTest extends BaseTest4 {
    /*
     * test json to string
     */
    @Test
    public void testJsonToString() throws Exception {
        TestUtil.createTable(con, "test_json", "id json");
        String sql = "INSERT INTO test_json VALUES ('{\"k1\":\"v1\",\"k2\":\"v2\"}')";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_json")) {
            assertTrue(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(1, rsmd.getColumnCount());
            assertEquals(Types.VARCHAR, rsmd.getColumnType(1));

            assertEquals("{\"k1\":\"v1\",\"k2\":\"v2\"}", rs.getString(1));
        }
        TestUtil.dropTable(con, "test_json");
    }
}
