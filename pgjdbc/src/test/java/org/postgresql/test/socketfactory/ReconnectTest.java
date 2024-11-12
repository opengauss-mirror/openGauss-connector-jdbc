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
