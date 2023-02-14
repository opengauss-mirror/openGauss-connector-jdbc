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
import org.postgresql.quickautobalance.ConnectionInfo;
import org.postgresql.quickautobalance.DataNode;
import org.postgresql.quickautobalance.DataNode.CheckDnStateResult;
import org.postgresql.test.TestUtil;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class DataNodeTest {
    private static final int DN_NUM = 3;

    private static final String FAKE_HOST = "127.127.217.217";

    private static final String FAKE_PORT = "1";

    private static final String FAKE_USER = "fakeuser";

    private static final String FAKE_DATABASE = "fakedatabase";

    private static final String FAKE_PASSWORD = "fakepassword";

    private HostSpec initHost() {
        return new HostSpec(TestUtil.getServer(), TestUtil.getPort());
    }

    private PgConnection getConnection(String url, Properties properties) throws SQLException {
        Connection connection = DriverManager.getConnection(url, properties);
        assertTrue(connection instanceof PgConnection);
        return (PgConnection) connection;
    }

    private Properties initProperties() {
        Properties properties = new Properties();
        properties.setProperty("PGDBNAME", TestUtil.getDatabase());
        properties.setProperty("PGHOSTURL", TestUtil.getServer());
        properties.setProperty("PGPORT", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGPORTURL", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGHOST", TestUtil.getServer());
        properties.setProperty("user", TestUtil.getUser());
        properties.setProperty("password", TestUtil.getPassword());
        properties.setProperty("autoBalance", "leastconn");
        return properties;
    }

    private HostSpec[] initHostSpecs() {
        HostSpec[] hostSpecs = new HostSpec[DN_NUM];
        hostSpecs[0] = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        hostSpecs[1] = new HostSpec(TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort());
        hostSpecs[2] = new HostSpec(TestUtil.getSecondaryServer2(), TestUtil.getSecondaryServerPort2());
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

    @Test
    public void setConnectionStateTest() throws SQLException {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        Properties properties = initProperties();
        PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
        dataNode.setConnection(pgConnection, properties, hostSpec);

        dataNode.setConnectionState(pgConnection, StatementCancelState.IDLE);
        assertEquals(StatementCancelState.IDLE, dataNode.getConnectionInfo(pgConnection).getConnectionState());

        dataNode.setConnectionState(pgConnection, StatementCancelState.IN_QUERY);
        assertEquals(StatementCancelState.IN_QUERY, dataNode.getConnectionInfo(pgConnection).getConnectionState());

        dataNode.setConnectionState(pgConnection, StatementCancelState.CANCELLED);
        assertEquals(StatementCancelState.CANCELLED, dataNode.getConnectionInfo(pgConnection).getConnectionState());

        dataNode.setConnectionState(pgConnection, StatementCancelState.CANCELING);
        assertEquals(StatementCancelState.CANCELING, dataNode.getConnectionInfo(pgConnection).getConnectionState());
    }

    @Test
    public void setConnectionTest() throws SQLException {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        Properties properties = initProperties();
        PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
        dataNode.setConnection(pgConnection, properties, hostSpec);
        ConnectionInfo connectionInfo = dataNode.getConnectionInfo(pgConnection);
        assertNotNull(connectionInfo);
        assertEquals(pgConnection, connectionInfo.getPgConnection());

        dataNode.setConnection(null, properties, hostSpec);
        dataNode.setConnection(pgConnection, null, hostSpec);
        dataNode.setConnection(pgConnection, properties, null);
    }

    @Test
    public void getConnectionTest() throws SQLException {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        Properties properties = initProperties();
        PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
        dataNode.setConnection(pgConnection, properties, hostSpec);
        ConnectionInfo connectionInfo = dataNode.getConnectionInfo(pgConnection);
        assertNotNull(connectionInfo);
        assertEquals(pgConnection, connectionInfo.getPgConnection());

        connectionInfo = dataNode.getConnectionInfo(null);
        assertNull(connectionInfo);
    }

    @Test
    public void getCachedConnectionListSizeTest() throws SQLException {
        int num = 10;
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        Properties properties = initProperties();
        assertEquals(0, dataNode.getCachedConnectionListSize());
        for (int i = 0; i < num; i++) {
            PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
            dataNode.setConnection(pgConnection, properties, hostSpec);
        }
        int result = dataNode.getCachedConnectionListSize();
        assertEquals(num, result);
    }

    @Test
    public void checkDnStateWithPropertiesSuccessTest() {
        HostSpec hostSpec = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        Properties properties = new Properties();
        properties.setProperty("PGDBNAME", TestUtil.getDatabase());
        properties.setProperty("PGHOSTURL", TestUtil.getServer());
        properties.setProperty("PGPORT", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGPORTURL", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGHOST", TestUtil.getServer());
        properties.setProperty("user", TestUtil.getUser());
        properties.setProperty("password", TestUtil.getPassword());
        DataNode dataNode = new DataNode(hostSpec);
        try {
            assertTrue(dataNode.checkDnState(properties));
        } catch (InvocationTargetException | PSQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void checkDnStateWithPropertiesUsernameOrPasswordErrorTest() {
        HostSpec hostSpec = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        Properties properties = new Properties();
        properties.setProperty("PGDBNAME", TestUtil.getDatabase());
        properties.setProperty("PGHOSTURL", TestUtil.getServer());
        properties.setProperty("PGPORT", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGPORTURL", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGHOST", TestUtil.getServer());
        properties.setProperty("user", FAKE_USER);
        properties.setProperty("password", FAKE_PASSWORD);
        DataNode dataNode = new DataNode(hostSpec);
        try {
            dataNode.checkDnState(properties);
        } catch (InvocationTargetException e) {
            assertTrue(e.getCause() instanceof PSQLException);
            if (e.getCause() instanceof PSQLException) {
                PSQLException psqlException = (PSQLException) (e.getCause());
                String sqlState = psqlException.getSQLState();
                assertEquals("28P01", sqlState);
            } else {
                e.printStackTrace();
                fail();
            }
        } catch (PSQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void checkDnStateWithPropertiesDatabaseErrorTest() {
        // Invalid parameter "PGDBNAME" doesn't affect to tryConnect().
        HostSpec hostSpec = new HostSpec(TestUtil.getServer(), TestUtil.getPort());
        Properties properties = new Properties();
        properties.setProperty("PGDBNAME", FAKE_DATABASE);
        properties.setProperty("PGHOSTURL", TestUtil.getServer());
        properties.setProperty("PGPORT", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGPORTURL", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGHOST", TestUtil.getServer());
        properties.setProperty("user", TestUtil.getUser());
        properties.setProperty("password", TestUtil.getPassword());
        DataNode dataNode = new DataNode(hostSpec);
        try {
            assertTrue(dataNode.checkDnState(properties));
        } catch (InvocationTargetException | PSQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void checkDnStateWithPropertiesConnectionFailedTest() {
        HostSpec hostSpec = new HostSpec(FAKE_HOST, Integer.parseInt(FAKE_PORT));
        Properties properties = new Properties();
        properties.setProperty("PGDBNAME", TestUtil.getDatabase());
        properties.setProperty("PGHOSTURL", FAKE_HOST);
        properties.setProperty("PGPORT", FAKE_PORT);
        properties.setProperty("PGPORTURL", FAKE_PORT);
        properties.setProperty("PGHOST", FAKE_HOST);
        properties.setProperty("user", TestUtil.getUser());
        properties.setProperty("password", TestUtil.getPassword());
        DataNode dataNode = new DataNode(hostSpec);
        try {
            dataNode.checkDnState(properties);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof PSQLException) {
                PSQLException psqlException = (PSQLException) (e.getCause());
                String SQLState = psqlException.getSQLState();
                assertNotEquals("28P01", SQLState);
            } else {
                assertTrue(true);
            }
        } catch (PSQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void checkDnStateWithHostSpecSuccessTest() {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Arrays.sort(hostSpecs);
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
        DataNode dataNode = new DataNode(new HostSpec(TestUtil.getServer(), TestUtil.getPort()));
        CheckDnStateResult result = dataNode.checkDnStateAndProperties(clusterProperties);
        assertEquals(CheckDnStateResult.DN_VALID, result);
    }

    @Test
    public void checkDnStateWithHostSpecUserOrPasswordExpiredTest() {
        HostSpec[] hostSpecs = initHostSpecs();
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Arrays.sort(hostSpecs);
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
        DataNode dataNode = new DataNode(new HostSpec(TestUtil.getServer(), TestUtil.getPort()));
        CheckDnStateResult result = dataNode.checkDnStateAndProperties(invalidProperties);
        assertEquals(CheckDnStateResult.PROPERTIES_INVALID, result);
    }

    @Test
    public void checkDnStateWithHostSpecConnectFailedTest() {
        HostSpec[] hostSpecs = initHostSpecs();
        HostSpec hostSpec = new HostSpec(FAKE_HOST, Integer.parseInt(FAKE_PORT));
        if (!checkHostSpecs(hostSpecs)) {
            return;
        }
        Arrays.sort(hostSpecs);
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty("user", TestUtil.getUser());
        clusterProperties.setProperty("password", TestUtil.getPassword());
        clusterProperties.setProperty("PGDBNAME", TestUtil.getDatabase());
        clusterProperties.setProperty("PGHOSTURL", FAKE_HOST);
        clusterProperties.setProperty("PGPORT", FAKE_PORT);
        clusterProperties.setProperty("PGPORTURL", FAKE_PORT);
        clusterProperties.setProperty("PGHOST", FAKE_HOST);
        DataNode dataNode = new DataNode(hostSpec);
        CheckDnStateResult result = dataNode.checkDnStateAndProperties(clusterProperties);
        assertEquals(CheckDnStateResult.DN_INVALID, result);
    }

    @Test
    public void checkConnectionValidityTest() {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        int total = 10;
        int remove = 2;
        List<PgConnection> pgConnections = new ArrayList<>();
        Properties properties = initProperties();
        try {
            for (int i = 0; i < total; i++) {
                PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
                dataNode.setConnection(pgConnection, properties, hostSpec);
                pgConnections.add(pgConnection);
            }
            assertEquals(0, dataNode.checkConnectionsValidity());
            assertEquals(10, dataNode.getCachedConnectionListSize());
            for (int i = 0; i < remove; i++) {
                pgConnections.get(i).close();
            }
            assertEquals(2, dataNode.checkConnectionsValidity());
            assertEquals(10 - 2, dataNode.getCachedConnectionListSize());
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void filterIdleConnectionsTest() {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        int idleSize = 10;
        int querySize = 20;
        Properties properties = initProperties();
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        for (int i = 0; i < idleSize; i++) {
            try {
                PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
                dataNode.setConnection(pgConnection, properties, hostSpec);
                ConnectionInfo connectionInfo = dataNode.getConnectionInfo(pgConnection);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        for (int i = 0; i < querySize; i++) {
            try {
                PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
                dataNode.setConnection(pgConnection, properties, hostSpec);
                ConnectionInfo connectionInfo = dataNode.getConnectionInfo(pgConnection);
                connectionInfo.setConnectionState(StatementCancelState.IN_QUERY);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        List<ConnectionInfo> connectionInfos = dataNode.filterIdleConnections(System.currentTimeMillis());
        assertEquals(idleSize, connectionInfos.size());
        for (ConnectionInfo connectionInfo : connectionInfos) {
            assertEquals(StatementCancelState.IDLE, connectionInfo.getConnectionState());
        }
    }

    @Test
    public void getAndSetDNStateTest() {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        assertTrue(dataNode.getDataNodeState());
        dataNode.setDataNodeState(false);
        assertFalse(dataNode.getDataNodeState());
    }

    @Test
    public void clearCachedConnectionsTest() {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        int idleSize = 10;
        Properties properties = initProperties();
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        for (int i = 0; i < idleSize; i++) {
            try {
                PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
                dataNode.setConnection(pgConnection, properties, hostSpec);
                ConnectionInfo connectionInfo = dataNode.getConnectionInfo(pgConnection);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        assertEquals(idleSize, dataNode.getCachedConnectionListSize());
        dataNode.clearCachedConnections();
        assertEquals(0, dataNode.getCachedConnectionListSize());
    }

    @Test
    public void closeConnectionsTest() {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        int total = 10;
        int closed = 5;
        Properties properties = initProperties();
        List<PgConnection> connectionList = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            try {
                PgConnection pgConnection = getConnection(TestUtil.getURL(), properties);
                dataNode.setConnection(pgConnection, properties, hostSpec);
                connectionList.add(pgConnection);
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        assertEquals(total, dataNode.getCachedConnectionListSize());
        for (int i = 0; i < total; i++) {
            try {
                assertTrue(connectionList.get(i).isValid(4));
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
        for (int i = 0; i < closed; i++) {
            assertTrue(dataNode.closeConnection(connectionList.get(i)));
        }
        assertEquals(total - closed, dataNode.getCachedConnectionListSize());
        for (int i = 0; i < closed; i++) {
            try {
                assertFalse(connectionList.get(i).isValid(4));
            } catch (SQLException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    @Test
    public void getAndSetCachedCreatingConnectionSizeTest() {
        HostSpec hostSpec = initHost();
        DataNode dataNode = new DataNode(hostSpec);
        assertEquals(0, dataNode.getCachedCreatingConnectionSize());
        assertEquals(1, dataNode.incrementCachedCreatingConnectionSize());
        assertEquals(1, dataNode.getCachedCreatingConnectionSize());
        assertEquals(0, dataNode.decrementCachedCreatingConnectionSize());
        assertEquals(0, dataNode.getCachedCreatingConnectionSize());
        assertEquals(0, dataNode.decrementCachedCreatingConnectionSize());
    }
}
