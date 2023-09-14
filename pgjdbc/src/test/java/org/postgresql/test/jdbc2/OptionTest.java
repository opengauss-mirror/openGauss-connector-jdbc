/*
 * Copyright (c) openGauss 2023. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.test.jdbc2;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Some simple tests url options
 *
 * @author bin.liu
 * @version 1.0
 */
public class OptionTest extends BaseTest4 {

    private final String behaviorCompatOptionsName = "behavior_compat_options";

    @Before
    public void setUp() throws Exception {
        return;
    }

    private Connection conDB(Properties props) throws Exception {
        return TestUtil.openDB(props);
    }

    private String setOptionsAndGet(String name, String value) throws Exception {
        Properties props = new Properties();
        if (value != null && !value.equals("")) {
            props.setProperty("options", "-c " + name + "=" + value);
        }
        con = conDB(props);
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery("show " + name);
        while (rs.next()) {
            return rs.getString(1);
        }
        return "";
    }

    @Test
    public void optionsBehaviorCompatOptions() throws Exception {
        // options applied successfully
        String s = setOptionsAndGet(behaviorCompatOptionsName, "");
        assertEquals("", s);
        // behavior_compat_options_name more option
        String value = "hide_tailing_zero,display_leading_zero";
        s = setOptionsAndGet(behaviorCompatOptionsName, value);
        assertEquals(value, s);
        // behavior_compat_options_name duplicate option
        s = setOptionsAndGet(behaviorCompatOptionsName, "hide_tailing_zero,hide_tailing_zero");
        assertEquals("hide_tailing_zero,hide_tailing_zero", s);
        // options applied failed
        optionsBehaviorCompatOptionsFailed(behaviorCompatOptionsName, "''");
        optionsBehaviorCompatOptionsFailed(behaviorCompatOptionsName, "hide_tailing_zero,,");
    }

    private void optionsBehaviorCompatOptionsFailed(String name, String value) throws Exception {
        Properties props = new Properties();
        String s = name + "=" + value;
        props.setProperty("options", "-c " + s);
        Connection con1 = null;
        try {
            con1 = conDB(props);
            Assert.fail("set options " + s);
        } catch (SQLException e) {
            // Ignored.
            System.out.println(e.getMessage());
        } finally {
            if (con1 != null) {
                con1.close();
            }
        }
    }
}
