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
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * test raw
 *
 * @author zhangting
 * @since  2024-09-10
 */
public class RawTest extends BaseTest4B {
    /*
     * test raw type
     */
    @Test
    public void testRaw() throws Exception {
        TestUtil.createTable(con, "test_raw", "c1 blob,c2 raw,c3 bytea");
        try (Statement statement = con.createStatement()) {
            statement.execute("set bytea_output=escape;");
            statement.execute("INSERT INTO test_raw VALUES (empty_blob(),hextoraw('deadbeef'),e'\\\\xdeadbeef')");
            statement.execute("INSERT INTO test_raw VALUES ('null','null','null')");
        }

        String sql = "select c1,c2,c3 from test_raw";
        try (PreparedStatement pstmt = con.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals("", rs.getString(1));
            assertEquals("deadbeef", rs.getString(2));
            assertEquals("\\336\\255\\276\\357", rs.getString(3));

            assertTrue(rs.next());
            assertEquals("null", rs.getString(1));
            assertEquals("null", rs.getString(2));
            assertEquals("null", rs.getString(3));
        }
        TestUtil.dropTable(con, "test_raw");
    }
}
