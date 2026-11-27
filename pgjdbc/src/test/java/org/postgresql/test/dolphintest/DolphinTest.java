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
