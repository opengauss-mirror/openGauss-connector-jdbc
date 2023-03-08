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
import org.postgresql.quickautobalance.Cluster;
import org.postgresql.quickautobalance.ConnectionManager;
import org.postgresql.quickautobalance.LoadBalanceHeartBeating;
import org.postgresql.quickautobalance.ReflectUtil;
import org.postgresql.test.TestUtil;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * LoadBalanceHeartBeatingTest
 */
public class LoadBalanceHeartBeatingTest {
    private static final String USER = TestUtil.getUser();

    private static final String PASSWORD = TestUtil.getPassword();

    private static final String MASTER_1 = TestUtil.getServer() + ":" + TestUtil.getPort();

    private static final String SECONDARY_1 = TestUtil.getSecondaryServer() + ":" + TestUtil.getSecondaryPort();

    private static final String SECONDARY_2 = TestUtil.getSecondaryServer2() + ":" + TestUtil.getSecondaryServerPort2();

    private static final String DATABASE = TestUtil.getDatabase();

    private Properties initProperties() {
        Properties properties = new Properties();
        properties.setProperty("PGPORTURL", TestUtil.getPort() + ","
            + TestUtil.getSecondaryPort() + "," + TestUtil.getSecondaryServerPort2());
        properties.setProperty("PGHOSTURL", TestUtil.getServer() + ","
            + TestUtil.getSecondaryServer() + "," + TestUtil.getSecondaryServer2());
        return properties;
    }

    private String initURLWithLeastConn() {
        return "jdbc:postgresql://" + MASTER_1 + "," + SECONDARY_1
            + "," + SECONDARY_2 + "/" + DATABASE + "?autoBalance=leastconn&loggerLevel=OFF";
    }

    @Test
    public void startCheckConnectionScheduledExecutorServiceSuccessTest() {
        Properties properties = initProperties();
        properties.setProperty("autoBalance", "leastconn");
        assertFalse(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertFalse(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        LoadBalanceHeartBeating.stopHeartBeatingThread();
    }

    @Test
    public void startCloseConnectionExecutorServiceSuccessTest() {
        Properties properties = initProperties();
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("enableQuickAutoBalance", "true");
        assertFalse(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertTrue(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertTrue(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertTrue(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        LoadBalanceHeartBeating.stopHeartBeatingThread();
    }

    @Test
    public void startCloseConnectionExecutorServiceFailedTest() {
        Properties properties = initProperties();
        properties.setProperty("autoBalance", "leastconn");
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        properties.setProperty("enableQuickAutoBalance", "fsfsfs");
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        properties.setProperty("enableQuickAutoBalance", "false");
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertFalse(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        LoadBalanceHeartBeating.stopHeartBeatingThread();
    }

    @Test
    public void startExecutorServiceWithSingleHostTest() {
        Properties properties = new Properties();
        properties.setProperty("PGPORTURL", String.valueOf(TestUtil.getPort()));
        properties.setProperty("PGHOSTURL", TestUtil.getServer());
        properties.setProperty("autoBalance", "leastconn");
        properties.setProperty("enableQuickAutoBalance", "true");
        assertFalse(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertFalse(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        LoadBalanceHeartBeating.startScheduledExecutorService(properties);
        assertFalse(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertFalse(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        LoadBalanceHeartBeating.stopHeartBeatingThread();
    }

    @Test
    public void setConnectionWithLeastConnTest() throws SQLException {
        String url = initURLWithLeastConn();
        DriverManager.getConnection(url, USER, PASSWORD).unwrap(PgConnection.class);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        LoadBalanceHeartBeating.stopHeartBeatingThread();
    }

    @Test
    public void setConnectionWithQuickAutoBalanceTest() throws SQLException {
        String url = initURLWithLeastConn() + "&enableQuickAutoBalance=true";
        DriverManager.getConnection(url, USER, PASSWORD).unwrap(PgConnection.class);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertTrue(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertTrue(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        LoadBalanceHeartBeating.stopHeartBeatingThread();
    }

    @Test
    public void stopHeartBeatingThreadTest() throws SQLException {
        String url = initURLWithLeastConn() + "&enableQuickAutoBalance=true";
        Map<String, Cluster> cachedClusters = ReflectUtil.getField(ConnectionManager.class, ConnectionManager.getInstance(),
            Map.class,
            "cachedClusters");
        // Start heartBeating thread.
        DriverManager.getConnection(url, USER, PASSWORD).unwrap(PgConnection.class);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertTrue(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertTrue(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        assertEquals(1, cachedClusters.size());
        // Stop heartBeating thread.
        LoadBalanceHeartBeating.stopHeartBeatingThread();
        assertFalse(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertFalse(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        assertEquals(0, cachedClusters.size());
        // Restart heartBeating thread.
        DriverManager.getConnection(url, USER, PASSWORD).unwrap(PgConnection.class);
        assertTrue(LoadBalanceHeartBeating.isLeastConnStarted());
        assertTrue(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertTrue(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        assertEquals(1, cachedClusters.size());
        // Stop heartBeating thread.
        LoadBalanceHeartBeating.stopHeartBeatingThread();
        assertFalse(LoadBalanceHeartBeating.isLeastConnStarted());
        assertFalse(LoadBalanceHeartBeating.isQuickAutoBalanceStarted());
        assertFalse(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        assertEquals(0, cachedClusters.size());
    }

    @Test
    public void checkHeartBeatingThreadShouldStopTest() throws SQLException, InterruptedException {
        String url = initURLWithLeastConn() + "&enableQuickAutoBalance=true";
        Map<String, Cluster> cachedClusters = ReflectUtil.getField(ConnectionManager.class, ConnectionManager.getInstance(),
            Map.class,
            "cachedClusters");
        // Start heartBeating thread.
        PgConnection connection = DriverManager.getConnection(url, USER, PASSWORD).unwrap(PgConnection.class);
        assertTrue(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        connection.close();
        Thread.sleep(2 * 1000);
        assertTrue(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        assertEquals(1, cachedClusters.size());
        Thread.sleep(20 * 1000);
        assertFalse(LoadBalanceHeartBeating.isLoadBalanceHeartBeatingStarted());
        assertEquals(0, cachedClusters.size());
    }
}
