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

package org.postgresql.jdbc;

import org.junit.Test;
import org.postgresql.test.jdbc2.BaseTest4;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParameterInjectionTest extends BaseTest4 {
    private interface ParameterBinder {
        void bind(PreparedStatement stmt) throws SQLException;
    }

    public void testParamInjection(ParameterBinder bindPositiveOne, ParameterBinder bindNegativeOne) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("SELECT -?");
        bindPositiveOne.bind(stmt);
        try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(-1, rs.getInt(1));
        }
        bindNegativeOne.bind(stmt);
        try (ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }

        PreparedStatement stmt2 = con.prepareStatement("SELECT -?, ?");
        bindPositiveOne.bind(stmt2);
        stmt2.setString(2, "\nWHERE 0 > 1");
        try (ResultSet rs = stmt2.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(-1, rs.getInt(1));
        }

        bindNegativeOne.bind(stmt2);
        stmt2.setString(2, "\nWHERE 0 > 1");
        try (ResultSet rs = stmt2.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void handleInt2() throws SQLException {
        testParamInjection(
                stmt -> {
                    stmt.setShort(1, (short) 1);
                },
                stmt -> {
                    stmt.setShort(1, (short) -1);
                }
        );
    }

    @Test
    public void handleInt4() throws SQLException {
        testParamInjection(
                stmt -> {
                    stmt.setInt(1, 1);
                },
                stmt -> {
                    stmt.setInt(1, -1);
                }
        );
    }

    @Test
    public void handleBigInt() throws SQLException {
        testParamInjection(
                stmt -> {
                    stmt.setLong(1, (long) 1);
                },
                stmt -> {
                    stmt.setLong(1, (long) -1);
                }
        );
    }

    @Test
    public void handleNumeric() throws SQLException {
        testParamInjection(
                stmt -> {
                    stmt.setBigDecimal(1, new BigDecimal("1"));
                },
                stmt -> {
                    stmt.setBigDecimal(1, new BigDecimal("-1"));
                }
        );
    }

    @Test
    public void handleFloat() throws SQLException {
        testParamInjection(
                stmt -> {
                    stmt.setFloat(1, 1);
                },
                stmt -> {
                    stmt.setFloat(1, -1);
                }
        );
    }

    @Test
    public void handleDouble() throws SQLException {
        testParamInjection(
                stmt -> {
                    stmt.setDouble(1, 1);
                },
                stmt -> {
                    stmt.setDouble(1, -1);
                }
        );
    }
}
