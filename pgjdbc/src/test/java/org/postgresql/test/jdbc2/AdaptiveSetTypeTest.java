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

package org.postgresql.test.jdbc2;

import org.junit.After;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.fail;

/**
 * Some simple tests url adaptiveSetSQLType
 *
 * @author bin.liu
 * @version 1.0
 */
public class AdaptiveSetTypeTest extends BaseTest4 {
    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestUtil.createTable(con, "test_numeric", "f_member_id character(6) NOT NULL,f_register_capital numeric(18,0)");
    }

    @After
    public void tearDown() throws SQLException {
        TestUtil.dropTable(con, "test_numeric");
    }

    @Override
    protected void updateProperties(Properties props) {
        props.setProperty("adaptiveSetSQLType", "true");
    }

    @Test
    public void AdaptiveSetTypeTrue() throws SQLException {
        PreparedStatement ps = null;
        Long a = new Long("2180000000");
        try {
            ps = con.prepareStatement("INSERT INTO test_numeric (F_MEMBER_ID,F_REGISTER_CAPITAL) VALUES (   ?,  ?)");
            ps.setString(1, "2097  ");
            ps.setNull(2, Types.INTEGER);
            ps.addBatch();
            ps.setString(1, "3020  ");
            ps.setLong(2, a);
            ps.addBatch();
            ps.executeBatch();
        } catch (SQLException e) {
            fail(e.getMessage());
        } finally {
            TestUtil.closeQuietly(ps);
        }
    }
}
