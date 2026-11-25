/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
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
