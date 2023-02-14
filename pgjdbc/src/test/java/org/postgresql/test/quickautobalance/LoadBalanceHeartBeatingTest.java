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
import org.postgresql.quickautobalance.LoadBalanceHeartBeating;
import org.postgresql.test.TestUtil;

import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * LoadBalanceHeartBeatingTest
 */
public class LoadBalanceHeartBeatingTest {
    private Properties initProperties() {
        Properties properties = new Properties();
        properties.setProperty("PGPORTURL", TestUtil.getPort() + ","
            + TestUtil.getSecondaryPort() + "," + TestUtil.getSecondaryServerPort2());
        properties.setProperty("PGHOSTURL", TestUtil.getServer() + ","
            + TestUtil.getSecondaryServer() + "," + TestUtil.getSecondaryServer2());
        return properties;
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
        LoadBalanceHeartBeating.stop();
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
        LoadBalanceHeartBeating.stop();
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
        LoadBalanceHeartBeating.stop();
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
    }
}
