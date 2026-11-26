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
import static org.junit.Assert.assertNotEquals;

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
        if (s != null && !s.equals("")) {
            assertNotEquals("", s);
        }
        // behavior_compat_options_name more option
        String value = "hide_tailing_zero,display_leading_zero";
        s = setOptionsAndGet(behaviorCompatOptionsName, value);
        assertEquals(value, s);
        // behavior_compat_options_name duplicate option
        s = setOptionsAndGet(behaviorCompatOptionsName, "hide_tailing_zero,hide_tailing_zero");
        assertEquals("hide_tailing_zero", s);
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
