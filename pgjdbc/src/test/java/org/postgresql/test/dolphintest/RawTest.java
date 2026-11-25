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
