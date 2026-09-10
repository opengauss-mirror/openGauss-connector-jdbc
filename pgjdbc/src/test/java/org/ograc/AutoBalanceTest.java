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
import org.postgresql.hostchooser.MultiHostChooser;
import org.postgresql.jdbc.ORConnection;
import org.postgresql.util.HostSpec;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * test auto balance
 *
 * @author zhangting
 * @since  2026-08-22
 */
public class AutoBalanceTest {
    private static final String TABLE = "ograc_autobalance_t";
    private static final int SAMPLE_CONNECTIONS = 8;

    /**
     * OR {@code loginTimeout} is applied as milliseconds to {@link java.net.Socket#connect}.
     * {@code socketTimeout} is seconds (read timeout) and must be set so dead/half-open
     * peers fail fast during login instead of hanging the suite.
     */
    private static final String CONNECT_TIMEOUTS = "loginTimeout=5000&socketTimeout=3";

    private static String[] configuredHosts;
    private static String[] reachableHosts;

    @BeforeClass
    public static void setUpClass() throws Exception {
        TestUtil.init();
        configuredHosts = TestUtil.getConfiguredHostPorts();
        reachableHosts = probeReachableHosts(configuredHosts);
    }

    @Test
    public void testConfiguredMultiHostFromBuildProperties() {
        assertTrue("primary host must be configured", configuredHosts.length >= 1);
        assertEquals(TestUtil.getServer() + ":" + TestUtil.getPort(), configuredHosts[0]);
        assumeTrue(
                "configure ograc.port2(+)/ograc.server2(+) in build.properties for multi-node tests",
                TestUtil.hasMultiHost());
        assertTrue(TestUtil.getMultiHostPorts().contains(","));
        assertEquals(configuredHosts.length,
                TestUtil.getMultiHostPorts().split(",").length);
    }

    @Test
    public void testParseAutoBalanceRoundRobin() throws Exception {
        assertAutoBalanceParsed("roundrobin");
    }

    @Test
    public void testParseAutoBalancePriority() throws Exception {
        assertAutoBalanceParsed("priority1");
    }

    @Test
    public void testParseAutoBalanceLeastConn() throws Exception {
        assertAutoBalanceParsed("leastconn");
    }

    @Test
    public void testParseAutoBalanceShuffle() throws Exception {
        assertAutoBalanceParsed("shuffle");
    }

    @Test
    public void testIsUsingAutoLoadBalanceForEachMode() {
        assertTrue(MultiHostChooser.isUsingAutoLoadBalance(propsWith("roundrobin")));
        assertTrue(MultiHostChooser.isUsingAutoLoadBalance(propsWith("priority1")));
        assertTrue(MultiHostChooser.isUsingAutoLoadBalance(propsWith("leastconn")));
        assertTrue(MultiHostChooser.isUsingAutoLoadBalance(propsWith("shuffle")));
        assertFalse(MultiHostChooser.isUsingAutoLoadBalance(propsWith("false")));
    }

    @Test
    public void testConnectWithRoundRobin() throws Exception {
        assumeConfiguredMultiHost();
        assumeReachableHost();
        connectAndSmokeQuery("roundrobin");
    }

    @Test
    public void testConnectWithPriority() throws Exception {
        assumeConfiguredMultiHost();
        assumeReachableHost();
        connectAndSmokeQuery("priority1");
    }

    @Test
    public void testConnectWithLeastConn() throws Exception {
        assumeConfiguredMultiHost();
        assumeReachableHost();
        connectAndSmokeQuery("leastconn");
    }

    @Test
    public void testConnectWithShuffle() throws Exception {
        assumeConfiguredMultiHost();
        assumeReachableHost();
        connectAndSmokeQuery("shuffle");
    }

    @Test
    public void testRoundRobinDistributesAcrossNodes() throws Exception {
        assumeReachableMultiHost();
        Set<String> connected = collectConnectedHosts("roundrobin", SAMPLE_CONNECTIONS);
        assertTrue(
                "roundrobin should eventually hit more than one node, got: " + connected
                        + " reachable=" + Arrays.toString(reachableHosts),
                connected.size() >= 2);
        assertHostsSubsetOf(connected, reachableHosts);
    }

    @Test
    public void testLeastConnDistributesAcrossNodes() throws Exception {
        assumeReachableMultiHost();
        Set<String> connected = collectConnectedHosts("leastconn", SAMPLE_CONNECTIONS);
        assertTrue(
                "leastconn should eventually hit more than one node, got: " + connected
                        + " reachable=" + Arrays.toString(reachableHosts),
                connected.size() >= 2);
        assertHostsSubsetOf(connected, reachableHosts);
    }

    @Test
    public void testShuffleConnectsToConfiguredNodes() throws Exception {
        assumeReachableMultiHost();
        Set<String> connected = collectConnectedHosts("shuffle", SAMPLE_CONNECTIONS);
        assertFalse(connected.isEmpty());
        assertHostsSubsetOf(connected, reachableHosts);
    }

    @Test
    public void testPriorityPrefersFirstReachablePreferredHost() throws Exception {
        assumeReachableMultiHost();
        String preferred = configuredHosts[0];
        assumeTrue(
                "priority1 preferred host must be reachable: " + preferred,
                Arrays.asList(reachableHosts).contains(preferred));

        String url = multiHostUrl("autoBalance=priority1&" + CONNECT_TIMEOUTS);
        Set<String> connected = new HashSet<String>();
        Connection[] conns = new Connection[SAMPLE_CONNECTIONS];
        try {
            for (int i = 0; i < SAMPLE_CONNECTIONS; i++) {
                conns[i] = open(url);
                assertConnected(conns[i]);
                assertTrue(conns[i] instanceof ORConnection);
                HostSpec hs = ((ORConnection) conns[i]).getHostSpec();
                assertNotNull(hs);
                connected.add(hs.toString());
            }
        } finally {
            for (Connection c : conns) {
                TestUtil.closeQuietly(c);
            }
        }
        assertEquals(
                "priority1 should prefer the first configured host " + preferred + ", got: " + connected,
                1,
                connected.size());
        assertTrue(connected.contains(preferred));
    }

    @Test
    public void testMultipleConnectionsRoundRobin() throws Exception {
        assumeReachableMultiHost();
        // Three connections can still land on one node; sample like distribution tests.
        Set<String> hosts = collectConnectedHosts("roundrobin", SAMPLE_CONNECTIONS);
        assertTrue("roundrobin multi-connect should use >1 node, got: " + hosts, hosts.size() >= 2);
    }

    @Test
    public void testMultipleConnectionsLeastConn() throws Exception {
        assumeReachableMultiHost();
        // Keep connections open while opening more so leastconn can see load difference.
        String url = TestUtil.getMultiHostUrl(
                joinHosts(reachableHosts), "autoBalance=leastconn&" + CONNECT_TIMEOUTS);
        Connection[] conns = new Connection[SAMPLE_CONNECTIONS];
        Set<String> hosts = new HashSet<String>();
        try {
            for (int i = 0; i < SAMPLE_CONNECTIONS; i++) {
                conns[i] = open(url);
                assertConnected(conns[i]);
                assertTrue(conns[i] instanceof ORConnection);
                HostSpec hs = ((ORConnection) conns[i]).getHostSpec();
                assertNotNull(hs);
                hosts.add(hs.toString());
            }
            runSimpleDml(conns[0]);
        } finally {
            for (Connection c : conns) {
                TestUtil.closeQuietly(c);
            }
        }
        assertTrue(
                "leastconn should eventually hit more than one node across "
                        + SAMPLE_CONNECTIONS + " connections, got: " + hosts
                        + " reachable=" + Arrays.toString(reachableHosts),
                hosts.size() >= 2);
        assertHostsSubsetOf(hosts, reachableHosts);
    }

    @Test
    public void testMultipleConnectionsShuffle() throws Exception {
        assumeConfiguredMultiHost();
        assumeReachableHost();
        String url = multiHostUrl("autoBalance=shuffle&" + CONNECT_TIMEOUTS);
        try (Connection c1 = open(url);
             Connection c2 = open(url)) {
            assertConnected(c1);
            assertConnected(c2);
            assertHostsSubsetOf(hostSet(c1, c2), configuredHosts);
        }
    }

    @Test
    public void testMultipleConnectionsPriority() throws Exception {
        assumeConfiguredMultiHost();
        assumeReachableHost();
        String preferred = configuredHosts[0];
        assumeTrue(
                "priority1 preferred host must be reachable: " + preferred,
                Arrays.asList(reachableHosts).contains(preferred));
        String url = multiHostUrl("autoBalance=priority1&" + CONNECT_TIMEOUTS);
        try (Connection c1 = open(url);
             Connection c2 = open(url)) {
            assertConnected(c1);
            assertConnected(c2);
            assertTrue(c1 instanceof ORConnection);
            assertTrue(c2 instanceof ORConnection);
            assertEquals(preferred, ((ORConnection) c1).getHostSpec().toString());
            assertEquals(preferred, ((ORConnection) c2).getHostSpec().toString());
        }
    }

    private static void assumeConfiguredMultiHost() {
        assumeTrue(
                "configure ograc.port2(+)/ograc.server2(+) in build.properties for multi-node tests",
                TestUtil.hasMultiHost());
    }

    private static void assumeReachableHost() {
        assumeTrue(
                "no reachable ograc node among " + Arrays.toString(configuredHosts),
                reachableHosts.length >= 1);
    }

    private static void assumeReachableMultiHost() {
        assumeConfiguredMultiHost();
        assumeTrue(
                "need >=2 reachable ograc nodes for distribution checks; reachable="
                        + Arrays.toString(reachableHosts)
                        + " configured=" + Arrays.toString(configuredHosts),
                reachableHosts.length >= 2);
    }

    private static void assertAutoBalanceParsed(String mode) throws Exception {
        assumeConfiguredMultiHost();
        String[] hostPorts = TestUtil.getConfiguredHostPorts();
        String url = TestUtil.getConfiguredMultiHostUrl("autoBalance=" + mode);
        Properties props = Driver.parseURL(url, new Properties());
        assertNotNull(props);
        assertEquals(mode, props.getProperty("autoBalance"));

        StringBuilder hosts = new StringBuilder();
        StringBuilder ports = new StringBuilder();
        for (int i = 0; i < hostPorts.length; i++) {
            int colon = hostPorts[i].lastIndexOf(':');
            assertTrue("invalid host:port " + hostPorts[i], colon > 0);
            if (i > 0) {
                hosts.append(',');
                ports.append(',');
            }
            hosts.append(hostPorts[i].substring(0, colon));
            ports.append(hostPorts[i].substring(colon + 1));
        }
        assertEquals(hosts.toString(), props.getProperty("PGHOST"));
        assertEquals(ports.toString(), props.getProperty("PGPORT"));
    }

    private static Properties propsWith(String autoBalance) {
        Properties p = new Properties();
        p.setProperty("autoBalance", autoBalance);
        return p;
    }

    private static void connectAndSmokeQuery(String mode) throws Exception {
        String url = multiHostUrl("autoBalance=" + mode + "&" + CONNECT_TIMEOUTS);
        try (Connection conn = open(url)) {
            assertConnected(conn);
            assertTrue(conn instanceof ORConnection);
            HostSpec hs = ((ORConnection) conn).getHostSpec();
            assertNotNull(hs);
            assertHostsSubsetOf(hostSet(conn), configuredHosts);
            runSimpleDml(conn);
        }
    }

    private static Set<String> collectConnectedHosts(String mode, int count) throws Exception {
        // Use only currently reachable hosts so strategy checks are not skewed by dead listeners.
        String url = TestUtil.getMultiHostUrl(
                joinHosts(reachableHosts), "autoBalance=" + mode + "&" + CONNECT_TIMEOUTS);
        Set<String> connected = new HashSet<String>();
        Connection[] conns = new Connection[count];
        try {
            for (int i = 0; i < count; i++) {
                conns[i] = open(url);
                assertConnected(conns[i]);
                assertTrue(conns[i] instanceof ORConnection);
                HostSpec hs = ((ORConnection) conns[i]).getHostSpec();
                assertNotNull(hs);
                connected.add(hs.toString());
            }
        } finally {
            for (Connection c : conns) {
                TestUtil.closeQuietly(c);
            }
        }
        return connected;
    }

    private static String[] probeReachableHosts(String[] hosts) {
        List<String> ok = new ArrayList<String>();
        for (String hostPort : hosts) {
            String url = TestUtil.getMultiHostUrl(hostPort, CONNECT_TIMEOUTS);
            try (Connection c = open(url);
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("select 1")) {
                if (rs.next()) {
                    ok.add(hostPort);
                }
            } catch (SQLException ignore) {
                // node down or auth/protocol mismatch
            }
        }
        return ok.toArray(new String[0]);
    }

    private static void assertHostsSubsetOf(Set<String> connected, String[] allowed) {
        Set<String> allow = new HashSet<String>(Arrays.asList(allowed));
        for (String h : connected) {
            assertTrue("unexpected host " + h + ", allowed=" + allow, allow.contains(h));
        }
    }

    private static Set<String> hostSet(Connection... conns) {
        Set<String> hosts = new HashSet<String>();
        for (Connection c : conns) {
            assertTrue(c instanceof ORConnection);
            hosts.add(((ORConnection) c).getHostSpec().toString());
        }
        return hosts;
    }

    private static String joinHosts(String[] hosts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hosts.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(hosts[i]);
        }
        return sb.toString();
    }

    private static void assertConnected(Connection conn) throws SQLException {
        assertNotNull(conn);
        assertFalse(conn.isClosed());
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    private static void runSimpleDml(Connection conn) throws SQLException {
        TestUtil.dropTable(conn, TABLE);
        try (Statement st = conn.createStatement()) {
            st.execute("create table " + TABLE + "(c1 int)");
            assertEquals(1, st.executeUpdate("insert into " + TABLE + " values(1)"));
            try (ResultSet rs = st.executeQuery("select c1 from " + TABLE)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        } finally {
            TestUtil.dropTable(conn, TABLE);
        }
    }

    private static Connection open(String url) throws SQLException {
        return java.sql.DriverManager.getConnection(url, TestUtil.getUser(), TestUtil.getPassword());
    }

    private static String multiHostUrl(String query) {
        if (reachableHosts != null && reachableHosts.length >= 1) {
            return TestUtil.getMultiHostUrl(joinHosts(reachableHosts), query);
        }
        return TestUtil.getConfiguredMultiHostUrl(query);
    }
}
