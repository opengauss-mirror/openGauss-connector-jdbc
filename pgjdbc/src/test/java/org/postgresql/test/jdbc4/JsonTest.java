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
