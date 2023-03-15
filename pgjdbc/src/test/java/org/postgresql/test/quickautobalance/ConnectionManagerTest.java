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
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.StatementCancelState;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.quickautobalance.Cluster;
import org.postgresql.quickautobalance.ConnectionInfo;
import org.postgresql.quickautobalance.ConnectionManager;
import org.postgresql.test.TestUtil;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class ConnectionManagerTest {
    private static Log LOGGER = Logger.getLogger(ConnectionManagerTest.class.getName());

    private static final String USER = TestUtil.getUser();

    private static final String PASSWORD = TestUtil.getPassword();

    private static final String MASTER_1 = TestUtil.getServer() + ":" + TestUtil.getPort();

    private static final String SECONDARY_1 = TestUtil.getSecondaryServer() + ":" + TestUtil.getSecondaryPort();

    private static final String SECONDARY_2 = TestUtil.getSecondaryServer2() + ":" + TestUtil.getSecondaryServerPort2();

    private static final String FAKE_HOST = "127.127.217.217";

    private static final int FAKE_PORT = 1;

    private static final String DATABASE = TestUtil.getDatabase();

    private static final int DN_NUM = 3;

    private String initURLWithLeastConn(HostSpec[] hostSpecs) {
        return "jdbc:postgresql://" + hostSpecs[0].toString() + "," + hostSpecs[1].toString()
            + "," + hostSpecs[2].toString() + "/" + DATABASE + "?autoBalance=leastconn";
    }

    private PgConnection getConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password).unwrap(PgConnection.class);
    }

    private HostSpec[] initHostSpec() {
        HostSpec[] hostSpecs = new HostSpec[DN_NUM];
        hostSpecs[0] = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        hostSpecs[1] = new HostSpec(TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort());
        hostSpecs[2] = new HostSpec(TestUtil.getSecondaryServer2(), TestUtil.getSecondaryServerPort2());
        return hostSpecs;
    }

    private HostSpec[] initHostSpecWithOneDnFailed() {
        HostSpec[] hostSpecs = new HostSpec[DN_NUM];
        hostSpecs[0] = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        hostSpecs[1] = new HostSpec(TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort());
        hostSpecs[2] = new HostSpec(FAKE_HOST, FAKE_PORT);
        return hostSpecs;
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

    private Properties initPriorityWithLeastConn(HostSpec[] hostSpecs) {
        Properties properties = initPriority(hostSpecs);
        properties.setProperty("autoBalance", "leastconn");
        return properties;
    }

    @Test
    public void setConnectionTest() throws ClassNotFoundException, SQLException {
        if (String.valueOf(TestUtil.getSecondaryPort()).equals(System.getProperty("def_pgport"))
            || String.valueOf(TestUtil.getSecondaryServerPort2()).equals(System.getProperty("def_pgport"))) {
            return;
        }
        Class.forName("org.postgresql.Driver");
        String url = initURLWithLeastConn(initHostSpec());
        PgConnection connection1 = getConnection(url, USER, PASSWORD);
        String urlIdentifier = ConnectionManager.getURLIdentifierFromUrl(url);
        Cluster cluster = ConnectionManager.getInstance().getCluster(urlIdentifier);
        assertNotEquals(cluster, null);
        ConnectionInfo connectionInfo = cluster.getConnectionInfo(connection1);
        assertNotEquals(connectionInfo, null);
        assertEquals(connectionInfo.getPgConnection(), connection1);

        PgConnection connection2 = getConnection(url, USER, PASSWORD);
        urlIdentifier = ConnectionManager.getURLIdentifierFromUrl(url);
        cluster = ConnectionManager.getInstance().getCluster(urlIdentifier);
        assertNotEquals(cluster, null);
        connectionInfo = cluster.getConnectionInfo(connection2);
        assertNotEquals(connectionInfo, null);
        assertEquals(connectionInfo.getPgConnection(), connection2);
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void setConnectionStateTest() throws ClassNotFoundException, SQLException {
        if (String.valueOf(TestUtil.getSecondaryPort()).equals(System.getProperty("def_pgport"))
            || String.valueOf(TestUtil.getSecondaryServerPort2()).equals(System.getProperty("def_pgport"))) {
            return;
        }
        Class.forName("org.postgresql.Driver");
        String url = initURLWithLeastConn(initHostSpec());
        // set connection state which exists.
        PgConnection connection = getConnection(url, USER, PASSWORD);
        ConnectionManager.getInstance().setConnectionState(connection, StatementCancelState.IDLE);
        String urlIdentifier = ConnectionManager.getURLIdentifierFromUrl(url);
        ConnectionInfo connectionInfo = ConnectionManager.getInstance()
            .getCluster(urlIdentifier).getConnectionInfo(connection);
        assertEquals(connectionInfo.getConnectionState(), StatementCancelState.IDLE);

        ConnectionManager.getInstance().setConnectionState(connection, StatementCancelState.IN_QUERY);
        urlIdentifier = ConnectionManager.getURLIdentifierFromUrl(url);
        connectionInfo = ConnectionManager.getInstance().getCluster(urlIdentifier).getConnectionInfo(connection);
        assertEquals(connectionInfo.getConnectionState(), StatementCancelState.IN_QUERY);

        ConnectionManager.getInstance().setConnectionState(connection, StatementCancelState.CANCELING);
        urlIdentifier = ConnectionManager.getURLIdentifierFromUrl(url);
        connectionInfo = ConnectionManager.getInstance().getCluster(urlIdentifier).getConnectionInfo(connection);
        assertEquals(connectionInfo.getConnectionState(), StatementCancelState.CANCELING);

        ConnectionManager.getInstance().setConnectionState(connection, StatementCancelState.CANCELLED);
        urlIdentifier = ConnectionManager.getURLIdentifierFromUrl(url);
        connectionInfo = ConnectionManager.getInstance().getCluster(urlIdentifier).getConnectionInfo(connection);
        assertEquals(connectionInfo.getConnectionState(), StatementCancelState.CANCELLED);
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void cachedCreatingConnectionSizeTest() throws ClassNotFoundException, SQLException {
        if (String.valueOf(TestUtil.getSecondaryPort()).equals(System.getProperty("def_pgport"))
            || String.valueOf(TestUtil.getSecondaryServerPort2()).equals(System.getProperty("def_pgport"))) {
            return;
        }
        Class.forName("org.postgresql.Driver");
        HostSpec[] hostSpecs = initHostSpec();
        Properties properties = initPriorityWithLeastConn(hostSpecs);
        String url = initURLWithLeastConn(hostSpecs);
        int num = 10;
        for (int i = 0; i < num; i++) {
            PgConnection connection = getConnection(url, USER, PASSWORD);
            assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0],
                properties));
            assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0],
                properties));
            assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[1],
                properties));
            assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[1],
                properties));
            assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[2],
                properties));
            assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[2],
                properties));
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void cachedCreatingConnectionSizeOneFailTest() throws ClassNotFoundException, SQLException {
        if (String.valueOf(TestUtil.getSecondaryPort()).equals(System.getProperty("def_pgport"))
            || String.valueOf(TestUtil.getSecondaryServerPort2()).equals(System.getProperty("def_pgport"))) {
            return;
        }
        Class.forName("org.postgresql.Driver");
        HostSpec[] hostSpecs = initHostSpecWithOneDnFailed();
        Properties properties = initPriorityWithLeastConn(hostSpecs);
        String url = initURLWithLeastConn(hostSpecs);
        int num = 10;
        for (int i = 0; i < num; i++) {
            PgConnection connection = getConnection(url, USER, PASSWORD);
            assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0],
                properties));
            assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0],
                properties));
            assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[1],
                properties));
            assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[1],
                properties));
            assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[2],
                properties));
            assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[2],
                properties));
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void leastConnTest() throws SQLException, ClassNotFoundException {
        if (String.valueOf(TestUtil.getSecondaryPort()).equals(System.getProperty("def_pgport"))
            || String.valueOf(TestUtil.getSecondaryServerPort2()).equals(System.getProperty("def_pgport"))) {
            return;
        }
        Class.forName("org.postgresql.Driver");
        String url = initURLWithLeastConn(initHostSpec());
        int num = 100;
        HashMap<String, Integer> dns = new HashMap<>();
        for (int i = 0; i < num; i++) {
            PgConnection connection = getConnection(url, USER, PASSWORD);
            String socketAddress = connection.getSocketAddress().split("/")[1];
            dns.put(socketAddress, dns.getOrDefault(socketAddress, 0) + 1);
        }
        List<Integer> connectionCounts = new ArrayList<>();
        for (Entry<String, Integer> entry : dns.entrySet()) {
            connectionCounts.add(entry.getValue());
        }
        for (Integer connectionCount : connectionCounts) {
            LOGGER.info(GT.tr("{0}", connectionCount));
        }
        for (int i = 0; i < connectionCounts.size() - 1; i++) {
            assertTrue(Math.abs(connectionCounts.get(i) - connectionCounts.get(i + 1)) <= 1);
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void leastConnIntoOneDnTest() throws SQLException, ClassNotFoundException {
        if (String.valueOf(TestUtil.getSecondaryPort()).equals(System.getProperty("def_pgport"))
            || String.valueOf(TestUtil.getSecondaryServerPort2()).equals(System.getProperty("def_pgport"))) {
            return;
        }
        Class.forName("org.postgresql.Driver");
        String url1 = initURLWithLeastConn(initHostSpec());
        url1 += "&targetServerType=slave";
        int num1 = 100;
        HashMap<String, Integer> dns = new HashMap<>();
        for (int i = 0; i < num1; i++) {
            PgConnection connection = getConnection(url1, USER, PASSWORD);
            String socketAddress = connection.getSocketAddress().split("/")[1];
            dns.put(socketAddress, dns.getOrDefault(socketAddress, 0) + 1);
        }
        LOGGER.info(GT.tr("create 100 connections to slave nodes."));
        for (Entry<String, Integer> entry : dns.entrySet()) {
            int num = entry.getValue();
            LOGGER.info(GT.tr("{0} : {1}", entry.getKey(), entry.getValue()));
            assertEquals(50, num);
        }

        int num2 = 20;
        String url2 = initURLWithLeastConn(initHostSpec());
        String lastAddress = "";
        for (int i = 0; i < num2; i++) {
            PgConnection connection = getConnection(url2, TestUtil.getUser(), TestUtil.getPassword());
            String socketAddress = connection.getSocketAddress().split("/")[1];
            dns.put(socketAddress, dns.getOrDefault(socketAddress, 0) + 1);
            if (i > 1) {
                assertEquals(lastAddress, socketAddress);
            }
            lastAddress = socketAddress;
        }
        LOGGER.info(GT.tr("Create 20 connections to all nodes."));
        for (Entry<String, Integer> entry : dns.entrySet()) {
            LOGGER.info(GT.tr("{0} : {1}", entry.getKey(), entry.getValue()));
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void checkConnectionStateTest() {
        // If this testcase fail, maybe heartBeatingThread remove invalid connections.
        try {
            if (String.valueOf(TestUtil.getSecondaryPort()).equals(System.getProperty("def_pgport"))
                || String.valueOf(TestUtil.getSecondaryServerPort2()).equals(System.getProperty("def_pgport"))) {
                return;
            }
            Class.forName("org.postgresql.Driver");
            String url = initURLWithLeastConn(initHostSpec());
            List<PgConnection> pgConnections = new ArrayList<>();
            int total = 100;
            int remove = 10;
            for (int i = 0; i < total; i++) {
                PgConnection connection = getConnection(url, USER, PASSWORD);
                pgConnections.add(connection);
            }
            for (int i = 0; i < remove; i++) {
                pgConnections.get(i).close();
            }
            assertEquals(Integer.valueOf(remove), ConnectionManager.getInstance().checkConnectionsValidity().get(0));
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            fail();
        }
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void setClusterTest() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpec();
        Properties properties = initPriority(hostSpecs);
        assertFalse(ConnectionManager.getInstance().setCluster(properties));
        assertFalse(ConnectionManager.getInstance().setCluster(null));

        properties = new Properties();
        properties.setProperty("PGDBNAME", TestUtil.getDatabase());
        properties.setProperty("PGHOSTURL", TestUtil.getServer());
        properties.setProperty("PGPORT", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGPORTURL", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGHOST", TestUtil.getServer());
        properties.setProperty("user", TestUtil.getUser());
        properties.setProperty("password", TestUtil.getPassword());
        assertFalse(ConnectionManager.getInstance().setCluster(properties));

        properties = initPriorityWithLeastConn(hostSpecs);
        assertTrue(ConnectionManager.getInstance().setCluster(properties));
        assertFalse(ConnectionManager.getInstance().setCluster(properties));
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void incrementCachedCreatingConnectionSize() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpec();
        Properties properties = initPriorityWithLeastConn(hostSpecs);
        ConnectionManager.getInstance().setCluster(properties);
        assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(2, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(1, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], null));
        assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(2, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        properties.setProperty("autoBalance", "");
        assertEquals(0, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        properties.setProperty("autoBalance", "leastconn");
        assertEquals(0, ConnectionManager.getInstance()
            .incrementCachedCreatingConnectionSize(new HostSpec("127.127.127.127", 93589), properties));
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void decrementCachedCreatingConnectionSize() throws PSQLException {
        HostSpec[] hostSpecs = initHostSpec();
        Properties properties = initPriorityWithLeastConn(hostSpecs);
        ConnectionManager.getInstance().setCluster(properties);
        assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(2, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(1, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], null));
        assertEquals(1, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        assertEquals(2, ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        properties.setProperty("autoBalance", "");
        assertEquals(0, ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpecs[0], properties));
        properties.setProperty("autoBalance", "leastconn");
        assertEquals(0, ConnectionManager.getInstance()
            .decrementCachedCreatingConnectionSize(new HostSpec("127.127.127.127", 93589), properties));
        ConnectionManager.getInstance().clear();
    }

    @Test
    public void getCachedConnectionSizeTest() throws SQLException {
        String url = initURLWithLeastConn(initHostSpec());
        int total = 10;
        for (int i = 0; i < total; i++) {
            PgConnection pgConnection = getConnection(url, TestUtil.getUser(), TestUtil.getPassword());
        }
        assertEquals(total, ConnectionManager.getInstance().getCachedConnectionSize());
    }
}
