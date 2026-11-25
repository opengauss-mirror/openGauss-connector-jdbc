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
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test default type
 *
 * @author zhangting
 * @since  2024-09-04
 */
public class DefaultTypeTest extends BaseTest4B {
    /*
     * test Date type
     */
    @Test
    public void testDefaultType() throws Exception {
        TestUtil.createTable(con, "test_default_type",
                "c1 set('abc','ttp','mytest'),c2 enum('2012','2013','2014')");

        String sql = "INSERT INTO test_default_type VALUES ('abc','2014')";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.executeUpdate();
        }

        try (Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_default_type")) {
            assertTrue(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(2, rsmd.getColumnCount());
            assertEquals(Types.CHAR, rsmd.getColumnType(1));
            assertEquals(Types.CHAR, rsmd.getColumnType(2));
            assertEquals("abc", rs.getString(1));
            assertEquals("2014", rs.getString(2));
        }
        TestUtil.dropTable(con, "test_default_type");
    }
}
