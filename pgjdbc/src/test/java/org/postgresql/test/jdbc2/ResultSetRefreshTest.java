/*
 * Copyright (c) 2022, PostgreSQL Global Development Group
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


package org.postgresql.test.jdbc2;

import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

public class ResultSetRefreshTest extends BaseTest4 {
    
    @Test
    public void testWithDataColumnThatRequiresEscaping() throws Exception {
        TestUtil.dropTable(con, "refresh_row_bad_ident");
        TestUtil.execute("CREATE TABLE refresh_row_bad_ident (id int PRIMARY KEY, \"1 FROM refresh_row_bad_ident; SELECT 2; SELECT *\" int)", con);
        TestUtil.execute("INSERT INTO refresh_row_bad_ident (id) VALUES (1), (2), (3)", con);
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM refresh_row_bad_ident");
        assertTrue(rs.next());
        try {
            rs.refreshRow();
        } catch (SQLException ex) {
            throw new RuntimeException("ResultSet.refreshRow() did not handle escaping data column identifiers", ex);
        }
        rs.close();
        stmt.close();
    }
    
    @Test
    public void testWithKeyColumnThatRequiresEscaping() throws Exception {
        TestUtil.dropTable(con, "refresh_row_bad_ident");
        TestUtil.execute("CREATE TABLE refresh_row_bad_ident (\"my key\" int PRIMARY KEY)", con);
        TestUtil.execute("INSERT INTO refresh_row_bad_ident VALUES (1), (2), (3)", con);
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM refresh_row_bad_ident");
        assertTrue(rs.next());
        try {
            rs.refreshRow();
        } catch (SQLException ex) {
            throw new RuntimeException("ResultSet.refreshRow() did not handle escaping key column identifiers", ex);
        }
        rs.close();
        stmt.close();
    }
}