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
