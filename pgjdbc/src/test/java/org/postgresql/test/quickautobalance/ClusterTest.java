/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

package org.postgresql.test.quickautobalance;

import org.junit.Test;
import org.postgresql.QueryCNListUtils;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.StatementCancelState;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.quickautobalance.Cluster;
import org.postgresql.quickautobalance.ConnectionInfo;
import org.postgresql.quickautobalance.ConnectionManager;
import org.postgresql.quickautobalance.DataNode;
import org.postgresql.quickautobalance.ReflectUtil;
import org.postgresql.test.TestUtil;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Cluster test
 */
public class ClusterTest {
    private static Log LOGGER = Logger.getLogger(ClusterTest.class.getName());

    private static final String FAKE_HOST = "127.127.217.217";

    private static final String FAKE_PORT = "1";

    private static final String FAKE_USER = "fakeuser";

    private static final String FAKE_PASSWORD = "fakepassword";

    private static final int DN_NUM = 3;

    private PgConnection getConnection(String url, Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties).unwrap(PgConnection.class);
    }

    private HostSpec[] initHostSpecs() {
        HostSpec[] hostSpecs = new HostSpec[DN_NUM];
        hostSpecs[0] = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        hostSpecs[1] = new HostSpec(TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort());
        hostSpecs[2] = new HostSpec(TestUtil.getSecondaryServer2(), TestUtil.getSecondaryServerPort2());
        return hostSpecs;
    }

    private HostSpec[] initHostSpecsWithInvalidNode() {
        HostSpec[] hostSpecs = new HostSpec[DN_NUM];
        hostSpecs[0] = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        hostSpecs[1] = new HostSpec(TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort());
        hostSpecs[2] = new HostSpec(FAKE_HOST, Integer.parseInt(FAKE_PORT));
        return hostSpecs;
    }

    private boolean checkHostSpecs(HostSpec[] hostSpecs) {
        if (hostSpecs.length != DN_NUM) {
            return false;
        }
        for (int i = 0; i < DN_NUM; i++) {
            if ("localhost".equals(hostSpecs[i].getHost())) {
                return false;
            }
        }
        return true;
    }

    private Properties initPriority(HostSpec[] hostSpecs) {
        String host = hostSpecs[0].getHost() + "," + hostSpecs[1].getHost() + "," + hostSpecs[2].getHost();
        String port = hostSpecs[0].getPort() + "," + hostSpecs[1].getPort() + "," + hostSpecs[2].getPort();
        Properties properties = new Properties();
        properties.setProperty("PGDBNAME", TestUtil.getDatabase());
        properties.setProperty("PGHOSTURL", host);
        properties.setProperty("PGPORT", port);
        properties.setProperty("PGPORTURL", port);
        properties.setProperty("PGHOST", host);
        properties.setProperty("user", TestUtil.getUser());
        properties.setProperty("password", TestUtil.getPassword());
        return properties;
    }

    private String initURL(HostSpec[] hostSpecs) {
        String host1 = hostSpecs[0].getHost() + ":" + hostSpecs[0].getPort();
        String host2 = hostSpecs[1].getHost() + ":" + hostSpecs[1].getPort();
        String host3 = hostSpecs[2].getHost() + ":" + hostSpecs[2].getPort();
        return "jdbc:postgresql://" + host1 + "," + host2 + "," + host3 + "/" + TestUtil.getDatabase();
    }

    @Test
    public void checkConnectionsValidityTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs) + "?autoBalance=leastconn";
        int total = 20;
        int remove = 5;
        List<PgConnection> pgConnectionList = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(url, properties);
                pgConnectionList.add(pgConnection);
                cluster.setConnection(pgConnection, properties);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        List<Integer> result = cluster.checkConnectionsValidity();
        int sum = result.stream().reduce(Integer::sum).orElse(0);
        assertEquals(0, sum);

        for (int i = 0; i < remove; i++) {
            try {
                pgConnectionList.get(i).close();
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        sum = 0;
        result = cluster.checkConnectionsValidity();
        sum = result.stream().reduce(Integer::sum).orElse(0);
        assertEquals(remove, sum);
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void setMinReservedConPerClusterDefaultParamsTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        assertFalse(cluster.isEnableMinReservedConPerCluster());
        assertFalse(cluster.isEnableMinReservedConPerDatanode());
        assertEquals(cluster.getMinReservedConPerCluster(), 0);
        assertEquals(cluster.getMinReservedConPerDatanode(), 0);
    }

    @Test (expected = SQLException.class)
    public void setMinReservedConPerClusterParsedFailedTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);

        String url1 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerCluster", "asafaas");
        try (PgConnection pgConnection = getConnection(url1, properties)) {
            cluster.setConnection(pgConnection, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_TYPE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test (expected = SQLException.class)
    public void setMinReservedConPerClusterParsedTooSmallTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);

        String url1 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerCluster", "-1");
        try (PgConnection pgConnection = getConnection(url1, properties)) {
            cluster.setConnection(pgConnection, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_VALUE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test (expected = SQLException.class)
    public void setMinReservedConPerClusterParsedTooBigTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);

        String url1 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerCluster", "200");
        try (PgConnection pgConnection = getConnection(url1, properties)) {
            cluster.setConnection(pgConnection, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_VALUE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test (expected = SQLException.class)
    public void setMinReservedConPerDatanodeParsedFailedTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);

        String url1 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerDatanode", "asafaas");
        try (PgConnection pgConnection = getConnection(url1, properties)) {
            cluster.setConnection(pgConnection, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_TYPE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test (expected = SQLException.class)
    public void setMinReservedConPerDatanodeParsedTooSmallTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);

        String url1 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerDatanode", "-1");
        try (PgConnection pgConnection = getConnection(url1, properties)) {
            cluster.setConnection(pgConnection, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_VALUE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test (expected = SQLException.class)
    public void setMinReservedConPerDatanodeParsedTooBigTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String paramsOutOfRange = "2000000000";
        String url1 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerDatanode", paramsOutOfRange);
        try (PgConnection pgConnection = getConnection(url1, properties)) {
            cluster.setConnection(pgConnection, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_VALUE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test
    public void updateParamsFailedTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        assertFalse(cluster.isEnableMinReservedConPerCluster());
        assertFalse(cluster.isEnableMinReservedConPerDatanode());
        assertEquals(cluster.getMinReservedConPerCluster(), 0);
        assertEquals(cluster.getMinReservedConPerDatanode(), 0);

        String url2 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerCluster", "40");
        properties.setProperty("minReservedConPerDatanode", "50");
        PgConnection pgConnection1 = getConnection(url2, properties);
        cluster.setConnection(pgConnection1, properties);
        assertTrue(cluster.isEnableMinReservedConPerCluster());
        assertTrue(cluster.isEnableMinReservedConPerDatanode());
        assertEquals(cluster.getMinReservedConPerCluster(), 40);
        assertEquals(cluster.getMinReservedConPerDatanode(), 50);
        pgConnection1.close();

        String url3 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerCluster", "60");
        properties.setProperty("minReservedConPerDatanode", "70");
        PgConnection pgConnection2 = getConnection(url3, properties);
        cluster.setConnection(pgConnection2, properties);
        assertTrue(cluster.isEnableMinReservedConPerCluster());
        assertTrue(cluster.isEnableMinReservedConPerDatanode());
        assertEquals(cluster.getMinReservedConPerCluster(), 40);
        assertEquals(cluster.getMinReservedConPerDatanode(), 50);
        pgConnection2.close();
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void updateParamsSuccessTest() throws SQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        assertFalse(cluster.isEnableMinReservedConPerCluster());
        assertFalse(cluster.isEnableMinReservedConPerDatanode());
        assertEquals(cluster.getMinReservedConPerCluster(), 0);
        assertEquals(cluster.getMinReservedConPerDatanode(), 0);

        String url2 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerCluster", "40");
        properties.setProperty("minReservedConPerDatanode", "50");
        PgConnection pgConnection1 = getConnection(url2, properties);
        cluster.setConnection(pgConnection1, properties);
        assertTrue(cluster.isEnableMinReservedConPerCluster());
        assertTrue(cluster.isEnableMinReservedConPerDatanode());
        assertEquals(cluster.getMinReservedConPerCluster(), 40);
        assertEquals(cluster.getMinReservedConPerDatanode(), 50);
        pgConnection1.close();

        String url3 = initURL(hostSpecs) + "?autoBalance=leastconn";
        properties.setProperty("minReservedConPerCluster", "20");
        properties.setProperty("minReservedConPerDatanode", "30");
        PgConnection pgConnection2 = getConnection(url3, properties);
        cluster.setConnection(pgConnection2, properties);
        assertTrue(cluster.isEnableMinReservedConPerCluster());
        assertTrue(cluster.isEnableMinReservedConPerDatanode());
        assertEquals(cluster.getMinReservedConPerCluster(), 20);
        assertEquals(cluster.getMinReservedConPerDatanode(), 30);
        pgConnection2.close();
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void setConnectionTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs) + "?autoBalance=leastconn";
        PgConnection pgConnection;
        try {
            pgConnection = getConnection(url, properties);
            cluster.setConnection(pgConnection, properties);
            ConnectionInfo connectionInfo = cluster.getConnectionInfo(pgConnection);
            assertEquals(pgConnection, connectionInfo.getPgConnection());
            cluster.setConnection(null, properties);
            cluster.setConnection(pgConnection, null);
            ConnectionManager.getInstance().clear();
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    private void setConnection(Cluster cluster, String ip, int port, Properties properties) {
        String url = "jdbc:postgresql://" + ip + ":" + port + "/" + TestUtil.getDatabase();
        try {
            final PgConnection pgConnection = getConnection(url, properties);
            cluster.incrementCachedCreatingConnectionSize(new HostSpec(ip, port));
            cluster.setConnection(pgConnection, properties);
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void sortDnsByLeastConnTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);

        int num1 = 4;
        int num2 = 6;
        int num3 = 5;
        for (int i = 0; i < num1; i++) {
            setConnection(cluster, TestUtil.getServer(), TestUtil.getPort(), properties);
        }
        for (int i = 0; i < num2; i++) {
            setConnection(cluster, TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort(), properties);
        }
        for (int i = 0; i < num3; i++) {
            setConnection(cluster, TestUtil.getSecondaryServer2(), TestUtil.getSecondaryServerPort2(), properties);
        }
        List<HostSpec> result = cluster.sortDnsByLeastConn(Arrays.asList(hostSpecs));
        LOGGER.info(GT.tr("after sort: {0}", result.toString()));
        assertEquals(TestUtil.getServer(), result.get(0).getHost());
        assertEquals(TestUtil.getPort(), result.get(0).getPort());
        assertEquals(TestUtil.getSecondaryServer(), result.get(2).getHost());
        assertEquals(TestUtil.getSecondaryPort(), result.get(2).getPort());
        assertEquals(TestUtil.getSecondaryServer2(), result.get(1).getHost());
        assertEquals(TestUtil.getSecondaryServerPort2(), result.get(1).getPort());
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void sortDnsByLeastConnWithOneNodeFailedTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);

        int num1 = 4;
        int num2 = 6;
        int num3 = 5;
        for (int i = 0; i < num1; i++) {
            setConnection(cluster, TestUtil.getServer(), TestUtil.getPort(), properties);
        }
        for (int i = 0; i < num2; i++) {
            setConnection(cluster, TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort(), properties);
        }
        for (int i = 0; i < num3; i++) {
            setConnection(cluster, TestUtil.getSecondaryServer2(), TestUtil.getSecondaryServerPort2(), properties);
        }
        Map<HostSpec, DataNode> cachedDnList = ReflectUtil.getField(Cluster.class, cluster, Map.class, "cachedDnList");
        DataNode dataNode = cachedDnList.getOrDefault(hostSpecs[0], null);
        ReflectUtil.setField(DataNode.class, dataNode, "dataNodeState", false);
        List<HostSpec> result = cluster.sortDnsByLeastConn(Arrays.asList(hostSpecs));
        LOGGER.info(GT.tr("after sort: {0}", result.toString()));
        assertEquals(TestUtil.getServer(), result.get(2).getHost());
        assertEquals(TestUtil.getPort(), result.get(2).getPort());
        assertEquals(TestUtil.getSecondaryServer(), result.get(1).getHost());
        assertEquals(TestUtil.getSecondaryPort(), result.get(1).getPort());
        assertEquals(TestUtil.getSecondaryServer2(), result.get(0).getHost());
        assertEquals(TestUtil.getSecondaryServerPort2(), result.get(0).getPort());
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkDnStateSpecSuccessTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Arrays.sort(hostSpecs);
        String URLIdentifier = String.valueOf(hostSpecs);
        String pgHostUrl = hostSpecs[0].getHost() + "," + hostSpecs[1].getHost() + "," + hostSpecs[2].getHost();
        String pgPortUrl = hostSpecs[0].getPort() + "," + hostSpecs[1].getPort() + "," + hostSpecs[2].getPort();
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty("user", TestUtil.getUser());
        clusterProperties.setProperty("password", TestUtil.getPassword());
        clusterProperties.setProperty("PGDBNAME", TestUtil.getDatabase());
        clusterProperties.setProperty("PGHOSTURL", pgHostUrl);
        clusterProperties.setProperty("PGPORT", pgPortUrl);
        clusterProperties.setProperty("PGPORTURL", pgPortUrl);
        clusterProperties.setProperty("PGHOST", pgHostUrl);
        Cluster cluster = new Cluster(URLIdentifier, clusterProperties);
        for (int i = 0; i < DN_NUM; i++) {
            assertTrue(cluster.checkDnState(hostSpecs[i]));
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkDnStateUserOrPasswordErrorTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Arrays.sort(hostSpecs);
        String URLIdentifier = String.valueOf(hostSpecs);
        String pgHostUrl = hostSpecs[0].getHost() + "," + hostSpecs[1].getHost() + "," + hostSpecs[2].getHost();
        String pgPortUrl = hostSpecs[0].getPort() + "," + hostSpecs[1].getPort() + "," + hostSpecs[2].getPort();
        Properties invalidProperties = new Properties();
        invalidProperties.setProperty("user", FAKE_USER);
        invalidProperties.setProperty("password", FAKE_PASSWORD);
        invalidProperties.setProperty("PGDBNAME", TestUtil.getDatabase());
        invalidProperties.setProperty("PGHOSTURL", pgHostUrl);
        invalidProperties.setProperty("PGPORT", pgPortUrl);
        invalidProperties.setProperty("PGPORTURL", pgPortUrl);
        invalidProperties.setProperty("PGHOST", pgHostUrl);
        Cluster cluster = new Cluster(URLIdentifier, invalidProperties);
        for (int i = 0; i < DN_NUM; i++) {
            assertFalse(cluster.checkDnState(hostSpecs[i]));
        }

        Properties validProperties = new Properties();
        validProperties.setProperty("user", TestUtil.getUser());
        validProperties.setProperty("password", TestUtil.getPassword());
        validProperties.setProperty("PGDBNAME", TestUtil.getDatabase());
        validProperties.setProperty("PGHOSTURL", pgHostUrl);
        validProperties.setProperty("PGPORT", pgPortUrl);
        validProperties.setProperty("PGPORTURL", pgPortUrl);
        validProperties.setProperty("PGHOST", pgHostUrl);
        cluster.setProperties(invalidProperties);
        cluster.setProperties(validProperties);
        for (int i = 0; i < DN_NUM; i++) {
            assertTrue(cluster.checkDnState(hostSpecs[i]));
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkDnStateConnectFailedTest() throws PSQLException {
        HostSpec[] hostSpecs = new HostSpec[DN_NUM];
        for (int i = 0; i < DN_NUM; i++) {
            hostSpecs[i] = new HostSpec(FAKE_HOST, Integer.parseInt(FAKE_PORT));
        }
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Arrays.sort(hostSpecs);
        String URLIdentifier = Arrays.toString(hostSpecs);
        String pgHostUrl = hostSpecs[0].getHost() + "," + hostSpecs[1].getHost() + "," + hostSpecs[2].getHost();
        String pgPortUrl = hostSpecs[0].getPort() + "," + hostSpecs[1].getPort() + "," + hostSpecs[2].getPort();
        Properties invalidProperties = new Properties();
        invalidProperties.setProperty("user", TestUtil.getUser());
        invalidProperties.setProperty("password", TestUtil.getPassword());
        invalidProperties.setProperty("PGDBNAME", TestUtil.getDatabase());
        invalidProperties.setProperty("PGHOSTURL", pgHostUrl);
        invalidProperties.setProperty("PGPORT", pgPortUrl);
        invalidProperties.setProperty("PGPORTURL", pgPortUrl);
        invalidProperties.setProperty("PGHOST", pgHostUrl);
        Cluster cluster = new Cluster(URLIdentifier, invalidProperties);
        for (int i = 0; i < DN_NUM; i++) {
            assertFalse(cluster.checkDnState(hostSpecs[i]));
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkClusterStateSuccessTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs) + "?autoBalance=leastconn";
        int total = 10;
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(url, properties);
                cluster.setConnection(pgConnection, properties);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        int invalidDn = cluster.checkClusterState();
        assertEquals(0, invalidDn);
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkClusterStateOneNodeFailedTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecsWithInvalidNode();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs) + "?autoBalance=leastconn";
        int total = 10;
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(url, properties);
                cluster.setConnection(pgConnection, properties);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        int invalidDn = cluster.checkClusterState();
        assertEquals(1, invalidDn);
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkClusterStateAndQuickLoadBalanceTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        properties.setProperty("enableQuickAutoBalance", "true");
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs) + "?autoBalance=leastconn";
        int total = 9;
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(url, properties);
                cluster.setConnection(pgConnection, properties);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        try {
            Thread.sleep(1100);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        // set dnState='false' of dn1

        Map<HostSpec, DataNode> cachedDnList = ReflectUtil.getField(Cluster.class, cluster,
            Map.class, "cachedDnList");
        DataNode dataNode = cachedDnList.get(hostSpecs[0]);
        dataNode.setDataNodeState(false);
        int invalidDn = cluster.checkClusterState();
        assertEquals(0, invalidDn);
        // check cluster state: jdbc will find dnState change to true from false, and execute quickAutoBalance.
        Queue<ConnectionInfo> abandonedConnectionList = ReflectUtil.getField(Cluster.class, cluster,
            Queue.class, "abandonedConnectionList");
        assertEquals(6, abandonedConnectionList.size());
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkClusterStateAndQuickLoadBalanceWithParamsTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        // init properties with quickLoadBalance.
        Properties properties = initPriority(hostSpecs);
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("minReservedConPerDatanode", "0");
        properties.setProperty("minReservedConPerCluster", "75");
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs) + "?autoBalance=leastconn";
        int total = 12;
        // set connection
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(url, properties);
                cluster.setConnection(pgConnection, properties);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        try {
            Thread.sleep(1100);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        // set dnState='false' of dn1
        Map<HostSpec, DataNode> cachedDnList = ReflectUtil.getField(Cluster.class, cluster, Map.class,
            "cachedDnList");
        DataNode dataNode = cachedDnList.get(hostSpecs[0]);
        dataNode.setDataNodeState(false);
        // check cluster state: jdbc will find dnState change to true from false, and execute quickAutoBalance.
        int invalidDn = cluster.checkClusterState();
        assertEquals(0, invalidDn);
        Queue<ConnectionInfo> abandonedConnectionList = ReflectUtil.getField(Cluster.class, cluster,
            Queue.class, "abandonedConnectionList");
        assertEquals((int) (total / 3 * 2 * (100 - 75) / 100), abandonedConnectionList.size());
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void setConnectionStateTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs) + "?autoBalance=leastconn";
        PgConnection pgConnection;
        try {
            pgConnection = getConnection(url, properties);
            cluster.setConnection(pgConnection, properties);
            assertEquals(StatementCancelState.IDLE, cluster.getConnectionInfo(pgConnection).getConnectionState());
            cluster.setConnectionState(pgConnection, StatementCancelState.IN_QUERY);
            assertEquals(StatementCancelState.IN_QUERY, cluster.getConnectionInfo(pgConnection).getConnectionState());
            cluster.setConnectionState(pgConnection, StatementCancelState.CANCELLED);
            assertEquals(StatementCancelState.CANCELLED, cluster.getConnectionInfo(pgConnection).getConnectionState());
            cluster.setConnectionState(pgConnection, StatementCancelState.CANCELING);
            assertEquals(StatementCancelState.CANCELING, cluster.getConnectionInfo(pgConnection).getConnectionState());
            ConnectionManager.getInstance().clear();
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void incrementCachedCreatingConnectionSizeTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        // init properties with quickLoadBalance.
        Properties properties = initPriority(hostSpecs);
        properties.setProperty("autoBalance", "leastconn");
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        Map<HostSpec, DataNode> cachedDnList = ReflectUtil.getField(Cluster.class, cluster, Map.class,
            "cachedDnList");
        cluster.incrementCachedCreatingConnectionSize(hostSpecs[0]);
        assertEquals(1, cachedDnList.get(hostSpecs[0]).getCachedCreatingConnectionSize());
        cluster.incrementCachedCreatingConnectionSize(hostSpecs[0]);
        assertEquals(2, cachedDnList.get(hostSpecs[0]).getCachedCreatingConnectionSize());
        cluster.incrementCachedCreatingConnectionSize(hostSpecs[1]);
        assertEquals(1, cachedDnList.get(hostSpecs[1]).getCachedCreatingConnectionSize());
        assertEquals(0, cluster.incrementCachedCreatingConnectionSize(new HostSpec(FAKE_HOST,
            Integer.parseInt(FAKE_PORT))));
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void decrementCachedCreatingConnectionSizeTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        // init properties with quickLoadBalance.
        Properties properties = initPriority(hostSpecs);
        properties.setProperty("autoBalance", "leastconn");
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        Map<HostSpec, DataNode> cachedDnList = ReflectUtil.getField(Cluster.class, cluster,
            Map.class, "cachedDnList");
        cluster.incrementCachedCreatingConnectionSize(hostSpecs[0]);
        cluster.incrementCachedCreatingConnectionSize(hostSpecs[0]);
        assertEquals(2, cachedDnList.get(hostSpecs[0]).getCachedCreatingConnectionSize());
        cluster.decrementCachedCreatingConnectionSize(hostSpecs[0]);
        assertEquals(1, cachedDnList.get(hostSpecs[0]).getCachedCreatingConnectionSize());
        cluster.decrementCachedCreatingConnectionSize(hostSpecs[0]);
        assertEquals(0, cachedDnList.get(hostSpecs[0]).getCachedCreatingConnectionSize());
        cluster.decrementCachedCreatingConnectionSize(hostSpecs[0]);
        assertEquals(0, cachedDnList.get(hostSpecs[0]).getCachedCreatingConnectionSize());
        assertEquals(0, cluster.incrementCachedCreatingConnectionSize(new HostSpec(FAKE_HOST,
            Integer.parseInt(FAKE_PORT))));
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void setPropertiesTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties1 = initPriority(hostSpecs);
        properties1.setProperty("autoBalance", "leastconn");
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties1);
        Cluster cluster = new Cluster(URLIdentifier, properties1);
        List<Properties> cachedPropertiesList = ReflectUtil.getField(Cluster.class, cluster, List.class,
            "cachedPropertiesList");
        assertEquals(1, cachedPropertiesList.size());
        assertEquals(TestUtil.getUser(), cachedPropertiesList.get(0).getProperty("user", ""));
        assertEquals(TestUtil.getPassword(), cachedPropertiesList.get(0).getProperty("password", ""));
        Properties properties2 = initPriority(hostSpecs);
        properties2.setProperty("user", "fakeuser");
        properties2.setProperty("password", "fakepassword");
        cluster.setProperties(properties2);
        assertEquals(2, cachedPropertiesList.size());
        assertEquals("fakeuser", cachedPropertiesList.get(1).getProperty("user", ""));
        assertEquals("fakepassword", cachedPropertiesList.get(1).getProperty("password", ""));
        Properties properties3 = initPriority(hostSpecs);
        properties3.setProperty("password", "fakepassword2");
        cluster.setProperties(properties3);
        assertEquals(2, cachedPropertiesList.size());
        assertEquals(TestUtil.getUser(), cachedPropertiesList.get(0).getProperty("user", ""));
        assertEquals("fakepassword2", cachedPropertiesList.get(0).getProperty("password", ""));
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void closeConnectionTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs);
        int total = 50;
        List<PgConnection> pgConnectionList = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(url, properties);
                cluster.setConnection(pgConnection, properties);
                pgConnectionList.add(pgConnection);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        try {
            Thread.sleep(1100);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        Queue<ConnectionInfo> abandonedConnectionList = ReflectUtil.getField(Cluster.class, cluster,
            Queue.class, "abandonedConnectionList");
        ReflectUtil.setField(Cluster.class, cluster, "totalAbandonedConnectionSize", total);
        ReflectUtil.setField(Cluster.class, cluster, "quickAutoBalanceStartTime", Long.MAX_VALUE);
        for (PgConnection pgConnection : pgConnectionList) {
            ConnectionInfo connectionInfo = cluster.getConnectionInfo(pgConnection);
            abandonedConnectionList.add(connectionInfo);
        }
        assertEquals((int) (Math.ceil((double) total * 0.2)), cluster.closeConnections());
        assertEquals((int) (Math.ceil((double) total * 0.2)), cluster.closeConnections());
        assertEquals((int) (Math.ceil((double) total * 0.2)), cluster.closeConnections());
        assertEquals((int) (Math.ceil((double) total * 0.2)), cluster.closeConnections());
        assertEquals(total - 4 * (int) (Math.ceil((double) total * 0.2)), cluster.closeConnections());
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void getCachedConnectionSizeTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Properties properties = initPriority(hostSpecs);
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("enableQuickAutoBalance", "true");
        Arrays.sort(hostSpecs);
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        Cluster cluster = new Cluster(URLIdentifier, properties);
        String url = initURL(hostSpecs);
        int total = 50;
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(url, properties);
                cluster.setConnection(pgConnection, properties);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        assertEquals(total, cluster.getCachedConnectionSize());
    }
}

