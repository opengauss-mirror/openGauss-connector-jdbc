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
