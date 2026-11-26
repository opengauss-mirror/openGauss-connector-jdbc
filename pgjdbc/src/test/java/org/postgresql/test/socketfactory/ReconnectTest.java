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

package org.postgresql.test.socketfactory;

import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test reconnect
 *
 * @author zhangting
 * @since  2024-11-12
 */
public class ReconnectTest extends BaseTest4 {
    @Override
    protected void openDB(Properties props) throws Exception {
        props.put("autoReconnect", "true");
        props.put("maxReconnects", "2");
        super.openDB(props);
    }

    /*
     * test autoReconnect
     */
    @Test
    public void testReconnect() throws Exception {
        TestUtil.createTable(con, "t_reconnect", "id varchar");
        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO t_reconnect VALUES (?)")) {
            pstmt.setString(1, "abc");
            pstmt.executeUpdate();
            pstmt.setString(1, "dfg");
            pstmt.executeUpdate();
        }
        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM t_reconnect")) {
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
        }

        TestUtil.dropTable(con, "t_reconnect");
    }
}
