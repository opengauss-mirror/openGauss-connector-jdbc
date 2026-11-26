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

package org.postgresql.test.quickautobalance;

import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.StatementCancelState;
import org.postgresql.quickautobalance.ConnectionInfo;
import org.postgresql.test.TestUtil;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ConnectionInfo Test
 */
@Ignore
public class ConnectionInfoTest {
    private HostSpec initHost() {
        return new HostSpec(TestUtil.getServer(), TestUtil.getPort());
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
        return properties;
    }

    @Test
    public void createConnectionInfoWithDefaultParamsTest() throws SQLException {
        // ConnectionInfo without quickAutoBalance
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        assertEquals(connectionInfo.getPgConnection(), pgConnection);
        assertEquals(connectionInfo.getHostSpec(), hostSpec);
        assertEquals(connectionInfo.getConnectionState(), StatementCancelState.IDLE);
        assertEquals(connectionInfo.getMaxIdleTimeBeforeTerminal(), 30);
        assertEquals(connectionInfo.getAutoBalance(), "");
        assertFalse(connectionInfo.isEnableQuickAutoBalance());
        pgConnection.close();
    }

    @Test(expected = SQLException.class)
    public void createConnectionInfoEnableQuickAutoBalanceParsedFailedTest() throws SQLException {
        Properties properties = initProperties();
        properties.setProperty("autoBalance", "luanma");
        properties.setProperty("enableQuickAutoBalance", "luanma");
        HostSpec hostSpec = initHost();
        try (PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class)) {
            ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        } catch (PSQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_VALUE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test
    public void createConnectionInfoEnableQuickAutoBalanceFalseTest() throws SQLException {
        Properties properties = initProperties();
        properties.setProperty("autoBalance", "luanma");
        properties.setProperty("enableQuickAutoBalance", "false");
        HostSpec hostSpec = initHost();
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        assertEquals("luanma", connectionInfo.getAutoBalance());
        assertFalse(connectionInfo.isEnableQuickAutoBalance());
    }

    @Test
    public void createConnectionInfoSuccessTest() throws SQLException {
        String requestMaxIdleTimeBeforeTerminal = "9223372036854774";
        long exceptMaxIdleTimeBeforeTerminal = 9223372036854774L;
        Properties properties = initProperties();
        HostSpec hostSpec = initHost();
        properties.setProperty("maxIdleTimeBeforeTerminal", requestMaxIdleTimeBeforeTerminal);
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("enableQuickAutoBalance", "true");
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        assertEquals(connectionInfo.getMaxIdleTimeBeforeTerminal(), exceptMaxIdleTimeBeforeTerminal);
        assertTrue(connectionInfo.isEnableQuickAutoBalance());
        assertEquals("leastconn", connectionInfo.getAutoBalance());
        pgConnection.close();
    }

    @Test
    public void createConnectionInfoEqualTo0Test() throws SQLException {
        Properties properties = initProperties();
        HostSpec hostSpec = initHost();
        properties.setProperty("maxIdleTimeBeforeTerminal", "0");
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("enableQuickAutoBalance", "true");
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        assertEquals(connectionInfo.getMaxIdleTimeBeforeTerminal(), 0);
        assertTrue(connectionInfo.isEnableQuickAutoBalance());
        assertEquals("leastconn", connectionInfo.getAutoBalance());
        pgConnection.close();
    }

    @Test(expected = PSQLException.class)
    public void createConnectionInfoMaxIdleTimeBeforeTerminalParsedFailedTest() throws SQLException {
        Properties properties = initProperties();
        properties.setProperty("maxIdleTimeBeforeTerminal", "111111@@");
        HostSpec hostSpec = initHost();
        try {
            Connection connection = DriverManager.getConnection(TestUtil.getURL(), properties);
            PgConnection pgConnection = connection.unwrap(PgConnection.class);
            ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_TYPE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test(expected = SQLException.class)
    public void createConnectionInfoMaxIdleTimeBeforeTerminalTooBigTest() throws SQLException {
        String maxIdleTimeBeforeTerminalMaxThreshold = "9223372036854775";
        Properties properties = initProperties();
        HostSpec hostSpec = initHost();
        properties.setProperty("maxIdleTimeBeforeTerminal", maxIdleTimeBeforeTerminalMaxThreshold);
        properties.setProperty("enableQuickAutoBalance", "false");
        try (PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class)) {
            ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_VALUE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test(expected = SQLException.class)
    public void createConnectionInfoMaxIdleTimeBeforeTerminalTooSmallTest() throws SQLException {
        Properties properties = initProperties();
        HostSpec hostSpec = initHost();
        properties.setProperty("maxIdleTimeBeforeTerminal", String.valueOf(-1));
        properties.setProperty("enableQuickAutoBalance", "false");
        try (PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class)) {
            new ConnectionInfo(pgConnection, properties, hostSpec);
        } catch (SQLException e) {
            e.printStackTrace();
            assertEquals(PSQLState.INVALID_PARAMETER_VALUE.getState(), e.getSQLState());
            throw e;
        }
    }

    @Test
    public void checkConnectionCanBeClosedTestPgConnectionNullTest() throws InterruptedException, PSQLException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        PgConnection pgConnection;
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        Thread.sleep(1100);
        ConnectionInfo connectionInfo = new ConnectionInfo(null, properties, hostSpec);
        assertFalse(connectionInfo.checkConnectionCanBeClosed(System.currentTimeMillis()));
    }

    @Test
    public void checkConnectionCanBeClosedTestPgConnectionUnableQuickAutoBalance() throws SQLException,
        InterruptedException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        properties.setProperty("enableQuickAutoBalance", "false");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        Thread.sleep(1100);
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        assertFalse(connectionInfo.checkConnectionCanBeClosed(System.currentTimeMillis()));
        pgConnection.close();
    }

    @Test
    public void checkConnectionCanBeClosedTestStartTimeSmallerThanCreateTimeStamp() throws SQLException,
        InterruptedException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        Thread.sleep(1100);
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties).unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        assertFalse(connectionInfo.checkConnectionCanBeClosed(System.currentTimeMillis() - 1000 * 10));
        pgConnection.close();
    }

    @Test
    public void checkConnectionCanBeClosedTestConnectionStateNoEqualsToIDLE() throws SQLException, InterruptedException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        connectionInfo.setConnectionState(StatementCancelState.IN_QUERY);
        Thread.sleep(1100);
        assertFalse(connectionInfo.checkConnectionCanBeClosed(System.currentTimeMillis()));
        pgConnection.close();
    }

    @Test
    public void checkConnectionCanBeCloseTestIDLETimeToShort() throws SQLException, InterruptedException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "10");
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties).unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        Thread.sleep(1100);
        assertFalse(connectionInfo.checkConnectionCanBeClosed(System.currentTimeMillis()));
        pgConnection.close();
    }

    @Test
    public void checkConnectionCanBeCloseTestSuccessTest() throws SQLException, InterruptedException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        properties.setProperty("enableQuickAutoBalance", "true");
        properties.setProperty("maxIdleTimeBeforeTerminal", "1");
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties).unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        Thread.sleep(1100);
        assertTrue(connectionInfo.checkConnectionCanBeClosed(System.currentTimeMillis()));
        pgConnection.close();
    }

    @Test
    public void checkConnectionIsValidTest() throws SQLException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        assertTrue(connectionInfo.checkConnectionIsValid());
    }

    @Test
    public void checkConnectionIsValidFailedTest() throws SQLException {
        HostSpec hostSpec = initHost();
        Properties properties = initProperties();
        PgConnection pgConnection = DriverManager.getConnection(TestUtil.getURL(), properties)
            .unwrap(PgConnection.class);
        ConnectionInfo connectionInfo = new ConnectionInfo(pgConnection, properties, hostSpec);
        pgConnection.getQueryExecutor().setAvailability(false);
        pgConnection.close();
        long before = System.currentTimeMillis();
        assertFalse(connectionInfo.checkConnectionIsValid());
        long after = System.currentTimeMillis();
    }
}
