/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package org.ograc;

import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * test multiHost
 *
 * @author zhangting
 * @since  2026-09-23
 */
public class MultiHostTest {
    private static String server;
    private static int port;
    private static String[] configuredHosts;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestUtil.init();
        server = TestUtil.getServer();
        port = TestUtil.getPort();
        configuredHosts = TestUtil.getConfiguredHostPorts();
    }

    @Test
    public void testAcceptsOgracUrl() throws Exception {
        Driver driver = new Driver();
        String url = TestUtil.getUrl();
        String urlWithQuery = TestUtil.getUrl("loginTimeout=10");
        assertTrue(driver.acceptsURL(url));
        assertTrue(driver.acceptsURL(urlWithQuery));
        assertTrue(DriverManager.getDriver(url) != null);
    }

    @Test
    public void testParseSingleHost() throws Exception {
        Properties props = Driver.parseURL(TestUtil.getUrl(), new Properties());
        assertNotNull(props);
        assertEquals(server, props.getProperty("PGHOST"));
        assertEquals(String.valueOf(port), props.getProperty("PGPORT"));
        assertEquals(server, props.getProperty("PGHOSTURL"));
        assertEquals(String.valueOf(port), props.getProperty("PGPORTURL"));
    }

    @Test
    public void testParseMultiHost() throws Exception {
        assumeConfiguredMultiHost();
        Properties props = Driver.parseURL(TestUtil.getConfiguredMultiHostUrl(null), new Properties());
        assertNotNull(props);
        assertEquals(expectedHosts(), props.getProperty("PGHOST"));
        assertEquals(expectedPorts(), props.getProperty("PGPORT"));
    }

    @Test
    public void testParseThreeHosts() throws Exception {
        assumeTrue("configure at least 3 ograc nodes in build.properties for this test",
                configuredHosts.length >= 3);
        Properties props = Driver.parseURL(TestUtil.getConfiguredMultiHostUrl(null), new Properties());
        assertNotNull(props);
        assertEquals(expectedHosts(), props.getProperty("PGHOST"));
        assertEquals(expectedPorts(), props.getProperty("PGPORT"));
        assertTrue(props.getProperty("PGHOST").split(",").length >= 3);
        assertTrue(props.getProperty("PGPORT").split(",").length >= 3);
    }

    @Test
    public void testParseQueryParameters() throws Exception {
        Properties props = Driver.parseURL(TestUtil.getUrl("loginTimeout=10000&autoBalance=priority"),
                new Properties());
        assertNotNull(props);
        assertEquals("10000", props.getProperty("loginTimeout"));
        assertEquals("priority", props.getProperty("autoBalance"));
    }

    @Test
    public void testParseMultiHostWithAutoBalance() throws Exception {
        assumeConfiguredMultiHost();
        Properties props = Driver.parseURL(
                TestUtil.getConfiguredMultiHostUrl("autoBalance=leastconn&loginTimeout=10000"),
                new Properties());
        assertNotNull(props);
        assertEquals("leastconn", props.getProperty("autoBalance"));
        assertEquals("10000", props.getProperty("loginTimeout"));
        assertEquals(expectedHosts(), props.getProperty("PGHOST"));
        assertEquals(expectedPorts(), props.getProperty("PGPORT"));
    }

    @Test
    public void testParseInvalidPortReturnsNull() throws Exception {
        assertNull(Driver.parseURL(TestUtil.URL_PREFIX + server + ":99999", new Properties()));
        assertNull(Driver.parseURL(TestUtil.URL_PREFIX + server + ":abc", new Properties()));
    }

    @Test
    public void testParseHostWithoutPortUsesDefault() throws Exception {
        Properties props = Driver.parseURL(TestUtil.URL_PREFIX + server, new Properties());
        assertNotNull(props);
        assertEquals(server, props.getProperty("PGHOST"));
        assertNotNull(props.getProperty("PGPORT"));
    }

    @Test
    public void testOgracUrlDoesNotRequireDatabasePath() throws Exception {
        Properties props = Driver.parseURL(TestUtil.getUrl(), new Properties());
        assertNotNull(props);
        assertNull(props.getProperty("PGDBNAME"));
    }

    @Test
    public void testPropertyInfoNotEmpty() throws Exception {
        Driver driver = new Driver();
        DriverPropertyInfo[] infos = driver.getPropertyInfo(TestUtil.getUrl(), null);
        assertNotNull(infos);
        assertTrue(infos.length > 0);
    }

    @Test
    public void testUrlPrefixConstant() {
        assertTrue(TestUtil.getUrl(server, port, null).startsWith(TestUtil.URL_PREFIX));
        assertEquals(TestUtil.URL_PREFIX + server + ":" + port + "?loginTimeout=5",
                TestUtil.getUrl(server, port, "loginTimeout=5"));
    }

    @Test
    public void testMultiHostHelperBuildsUrl() {
        assumeConfiguredMultiHost();
        String url = TestUtil.getConfiguredMultiHostUrl("loginTimeout=10000");
        assertTrue(url.startsWith(TestUtil.URL_PREFIX));
        assertTrue(url.contains(","));
        assertTrue(url.contains("loginTimeout=10000"));
    }

    @Test
    public void testConnectSingleHostFromConfiguredServer() throws Exception {
        String url = TestUtil.getMultiHostUrl(server + ":" + port, "loginTimeout=10000");
        try (Connection conn = DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword())) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));
        }
    }

    private static void assumeConfiguredMultiHost() {
        assumeTrue("configure ograc.port2(+)/ograc.server2(+) in build.properties for multi-node tests",
                TestUtil.hasMultiHost());
    }

    private static String expectedHosts() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < configuredHosts.length; i++) {
            int colon = configuredHosts[i].lastIndexOf(':');
            if (i > 0) {
                sb.append(',');
            }
            sb.append(configuredHosts[i].substring(0, colon));
        }
        return sb.toString();
    }

    private static String expectedPorts() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < configuredHosts.length; i++) {
            int colon = configuredHosts[i].lastIndexOf(':');
            if (i > 0) {
                sb.append(',');
            }
            sb.append(configuredHosts[i].substring(colon + 1));
        }
        return sb.toString();
    }
}
