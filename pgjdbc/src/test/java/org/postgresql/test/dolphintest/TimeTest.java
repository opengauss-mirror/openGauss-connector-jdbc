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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4B;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import  java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test-case is only for JDBC4 time methods. Take a look at
 * {@link org.postgresql.test.jdbc2.TimeTest} for base tests concerning blobs
 */
public class TimeTest extends BaseTest4B {
    @Test
    public void testIntToTime() throws SQLException {
        TestUtil.createTable(con, "test_time", "id int");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_time VALUES (?)");
        pstmt.setObject(1, 11, Types.INTEGER);
        pstmt.executeUpdate();

        pstmt.setObject(1, -11, Types.INTEGER);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cast(id as time) FROM test_time");

        assertTrue(rs.next());
        String str = rs.getString(1);
        assertNotNull(str);

        assertTrue(rs.next());
        String str2 = rs.getString(1);
        assertNotNull(str2);
        assertEquals(str, str2);

        TestUtil.dropTable(con, "test_time");
    }

    @Test
    public void testYearToDate() throws SQLException, ParseException {
        TestUtil.createTable(con, "test_year", "c date,c2 year default '2024'");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_year(c) VALUES (?);");
        pstmt.setObject(1, "2023-05-20", Types.DATE);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT c2 FROM test_year");

        assertTrue(rs.next());
        Object c21 = rs.getObject(1);
        assertNotNull(c21);
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("2024-01-01");
        assertEquals(date, c21);

        Date c22 = rs.getDate(1);
        assertNotNull(c22);
        assertEquals(date, c22);
        TestUtil.dropTable(con, "test_year");
    }

    @Test
    public void testAsTime() throws SQLException {
        TestUtil.createTable(con, "test_as_time", "id int");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO test_as_time VALUES (?);");
        pstmt.setObject(1, -11);
        pstmt.executeUpdate();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT cast(id as time) from test_as_time");

        assertTrue(rs.next());
        Object r1 = rs.getObject(1);
        assertNotNull(r1);
        assertEquals("00:00:11", r1.toString());

        String r2 = rs.getString(1);
        assertNotNull(r2);
        assertEquals("00:00:11", r2);

        Time r3 = rs.getTime(1);
        assertNotNull(r3);
        assertEquals("00:00:11", r3.toString());
        TestUtil.dropTable(con, "test_as_time");
    }
}
