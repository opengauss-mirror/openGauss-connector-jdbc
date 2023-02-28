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

package org.postgresql.quickautobalance;

import org.postgresql.PGProperty;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Load balance heartBeating.
 */
public class LoadBalanceHeartBeating {

    private static final int INITIAL_DELAY = 1000;

    private static final int CHECK_CLUSTER_STATE_PERIOD = 1000 * 20;

    // unit: CLOSE_CONNECTION_PER_SECOND
    private static final int CLOSE_CONNECTION_PERIOD = 1000 * 5;

    // A heartBeating thread used to check clusters' state.
    // If user has configured 'autoBalance=leastconn', jdbc will start checkConnectionScheduledExecutorService.
    private static final ScheduledExecutorService checkClusterStateScheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(r -> new Thread(r, "loadBalanceHeartBeatingThread"));

    // A heartBeating thread used to close abandoned connections.
    // If user has configured 'autoBalance=leastconn&enableQuickAutoBalance=true', jdbc will start closeConnectionExecutorService.
    private static final ScheduledExecutorService closeConnectionExecutorService = Executors
        .newSingleThreadScheduledExecutor(r -> new Thread(r, "closeConnectionsHeartBeatingThread"));
    private static Log LOGGER = Logger.getLogger(LoadBalanceHeartBeating.class.getName());
    private static volatile ScheduledFuture<?> checkClusterStateScheduledFuture = null;

    private static volatile ScheduledFuture<?> closeConnectionScheduledFuture = null;

    // If singleton checkConnectionScheduledExecutorService has started.
    private static volatile boolean leastConnStarted = false;

    // If singleton closeConnectionExecutorService has started.
    private static volatile boolean quickAutoBalanceStarted = false;

    /**
     * Whether quickAutoBalance has started.
     *
     * @return whether quickAutoBalance has started
     */
    public static boolean isLoadBalanceHeartBeatingStarted() {
        return leastConnStarted && quickAutoBalanceStarted;
    }

    /**
     * Get if quickAutoBalance has started.
     *
     * @return if quickAutoBalance has started
     */
    public static boolean isQuickAutoBalanceStarted() {
        return quickAutoBalanceStarted;
    }

    /**
     * Get if leastConnStarted has started.
     *
     * @return if leastConnStarted has started
     */
    public static boolean isLeastConnStarted() {
        return leastConnStarted;
    }

    /**
     * Start scheduled executor service. There are two singleton scheduled executor service.
     * If user has configured 'autoBalance=leastconn', jdbc will start checkConnectionScheduledExecutorService.
     * If user has configured 'autoBalance=leastconn&enableQuickAutoBalance=true', jdbc will start closeConnectionExecutorService.
     *
     * @param properties properties
     */
    public static void startScheduledExecutorService(Properties properties) {
        if (!leastConnStarted) {
            if (ConnectionManager.checkEnableLeastConn(properties)) {
                synchronized (LoadBalanceHeartBeating.class) {
                    if (!leastConnStarted) {
                        leastConnStarted = true;
                        checkClusterStateScheduledFuture = checkClusterStateScheduledExecutorService
                            .scheduleAtFixedRate(LoadBalanceHeartBeating::checkClusterStateScheduleTask,
                                INITIAL_DELAY, CHECK_CLUSTER_STATE_PERIOD, TimeUnit.MILLISECONDS);
                        LOGGER.info(GT.tr("Start scheduleExecutorService, period:{0} milliseconds.",
                            CHECK_CLUSTER_STATE_PERIOD));
                    }
                }
            }
        }
        if (!quickAutoBalanceStarted) {
            if (ConnectionManager.checkEnableLeastConn(properties)
                && ConnectionInfo.ENABLE_QUICK_AUTO_BALANCE_PARAMS.equals(PGProperty.ENABLE_QUICK_AUTO_BALANCE.get(properties))) {
                synchronized (LoadBalanceHeartBeating.class) {
                    if (!quickAutoBalanceStarted) {
                        quickAutoBalanceStarted = true;
                        closeConnectionScheduledFuture = closeConnectionExecutorService
                            .scheduleAtFixedRate(LoadBalanceHeartBeating::closeAbandonedConnections,
                                INITIAL_DELAY, CLOSE_CONNECTION_PERIOD, TimeUnit.MILLISECONDS);
                        LOGGER.info(GT.tr("Start closeConnectionScheduledFuture, period:{0} milliseconds.",
                            CLOSE_CONNECTION_PERIOD));
                    }
                }
            }
        }
    }

    private static void checkClusterStateScheduleTask() {
        checkClusterState();
        checkConnectionValidity();
    }

    private static void closeAbandonedConnections() {
        List<Integer> closedConnections = ConnectionManager.getInstance().closeConnections();
        int sum = closedConnections.stream().mapToInt(Integer::intValue).sum();
        LOGGER.info(GT.tr("Scheduled task: closeAbandonedConnections(), thread id: {0}, " +
            "amount of closed connections: {1}.", Thread.currentThread().getId(), sum));
    }

    private static void checkClusterState() {
        int invalidDataNodes = ConnectionManager.getInstance().checkClusterStates();
        LOGGER.info(GT.tr("Scheduled task: checkClusterState(), thread id: {0}, " +
            "amount of invalid data nodes: {1}.", Thread.currentThread().getId(), invalidDataNodes));
    }

    private static void checkConnectionValidity() {
        List<Integer> removes = ConnectionManager.getInstance().checkConnectionsValidity();
        int sum = removes.stream().mapToInt(Integer::intValue).sum();
        LOGGER.info(GT.tr("Scheduled task: checkConnectionValidity(), thread id: {0}, " +
            "amount of removed connections: {1}.", Thread.currentThread().getId(), sum));
    }

    /**
     * Stop scheduled executor service.
     */
    public static void stop() {
        if (checkClusterStateScheduledFuture != null || closeConnectionScheduledFuture != null) {
            synchronized (LoadBalanceHeartBeating.class) {
                if (checkClusterStateScheduledFuture != null) {
                    checkClusterStateScheduledFuture.cancel(true);
                    leastConnStarted = false;
                }
                if (closeConnectionScheduledFuture != null) {
                    closeConnectionScheduledFuture.cancel(true);
                    quickAutoBalanceStarted = false;
                }
            }
        }
    }
}
