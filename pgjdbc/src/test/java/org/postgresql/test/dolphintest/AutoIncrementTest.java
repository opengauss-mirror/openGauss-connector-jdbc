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
import org.postgresql.test.jdbc2.BaseTest4B;
import org.postgresql.util.ExecuteUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class AutoIncrementTest extends BaseTest4B {
    @Test
    public void testTriggerQuery() throws Exception {
        assumeMiniOgVersion("opengauss 6.0.0",6,0,0);
        String dropTable="drop table if exists t_tinyint0006";
        String dropTrigger="drop trigger trigger_tinyint0006";
        String sqlTable = "create table t_tinyint0006 (" + "id int primary key auto_increment,"
                + "my_data tinyint" + ");";
        String sqlTrigger = "create trigger trigger_tinyint0006 before insert on t_tinyint0006" + " for each row "
                + "begin" + " update t_tinyint0006 set my_data=1;" + "end;";
        ExecuteUtil.execute(con, dropTable);
        ExecuteUtil.execute(con, sqlTable);
        ExecuteUtil.execute(con, sqlTrigger);
        ExecuteUtil.execute(con, dropTable);
        ExecuteUtil.execute(con, dropTable);
    }
    @Test
    public void testReturningQuery() throws Exception {
        String dropTable="drop table if exists CIMMIT";
        String sqlTable = "create table CIMMIT (id int primary key auto_increment,DATA_ENABLE bigint)";
        String returnString = "INSERT INTO CIMMIT (DATA_ENABLE) VALUES (1)";
        ExecuteUtil.execute(con, dropTable);
        ExecuteUtil.execute(con, sqlTable);
        PreparedStatement st = con.prepareStatement(returnString, new String[] {"ID"});
        st.execute();
        st.close();
        ExecuteUtil.execute(con, dropTable);
    }
}
