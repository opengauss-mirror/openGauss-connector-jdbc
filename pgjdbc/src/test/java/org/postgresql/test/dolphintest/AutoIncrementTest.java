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
