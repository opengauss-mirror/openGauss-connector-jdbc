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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class BitTest extends BaseTest4 {
    /*
     * Tests bit type
     */
    @Test
    public void testBit() throws Exception {
        TestUtil.createTable(con, "test_bit", "c1 bit(1),c2 bit(10),c3 bit(6)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_bit VALUES (0::bit(1), 1234::bit(10), 88::bit(6))");
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT c1,c2,c3 FROM test_bit");

        assertTrue(rs.next());
        Object o1 = rs.getObject(1);
        assertNotNull(o1);
        assertEquals(false, o1);

        Object o2 = rs.getObject(2);
        assertNotNull(o2);
        assertEquals(true, o2);

        Object o3 = rs.getObject(3);
        assertNotNull(o3);
        assertEquals(true, o3);
        TestUtil.dropTable(con, "test_bit");
    }

    /*
     * Tests bit by getBytes()
     */
    @Test
    public void testBitToBytes() throws Exception {
        TestUtil.createTable(con, "test_bitToBytes", "c1 bit(10),c2 bit(18)");
        String sql = "INSERT INTO test_bitToBytes VALUES (123::bit(10), 18437::bit(18))";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }

        try (Statement ps = con.createStatement();
             ResultSet rs = ps.executeQuery("SELECT c1,c2 FROM test_bitToBytes")) {
            assertTrue(rs.next());
            String r1 = rs.getString(1);
            assertNotNull(r1);
            assertEquals("0001111011", r1);

            String r2 = rs.getString(2);
            assertNotNull(r2);
            assertEquals("000100100000000101", r2);
        } finally {
            TestUtil.dropTable(con, "test_bitToBytes");
        }
    }
}
