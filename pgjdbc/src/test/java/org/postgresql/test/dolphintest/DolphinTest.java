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

import java.sql.*;

import static org.junit.Assert.*;

public class DolphinTest extends BaseTest4B {

    /**
     * test db type and dolphin mode
     * @throws Exception
     */
    @Test
    public void testUint1() throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rsDBType = stmt.executeQuery("show sql_compatibility;");

        assertTrue(rsDBType.next());
        String r1 = rsDBType.getString(1);
        assertNotNull(r1);
        assertEquals("B", r1);

        ResultSet rsMode = stmt.executeQuery("show dolphin.b_compatibility_mode;");

        assertTrue(rsMode.next());
        String r2 = rsMode.getString(1);
        assertNotNull(r2);
        assertEquals("on", r2);
    }
}
