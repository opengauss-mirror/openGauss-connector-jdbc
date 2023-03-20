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

import org.postgresql.jdbc.PgConnection;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private static ScheduledExecutorService checkClusterStateScheduledExecutorService = null;


    // A heartBeating thread used to close abandoned connections.
    // If user has configured 'autoBalance=leastconn&enableQuickAutoBalance=true', jdbc will start closeConnectionExecutorService.
    private static ScheduledExecutorService closeConnectionExecutorService = null;

    private static Log LOGGER = Logger.getLogger(LoadBalanceHeartBeating.class.getName());

    // CheckClusterStateScheduledExecutorService will count the number of cached connections in ConnectionManager
    // each time it executes. If it's 0 for two consecutive times, jdbc will close heartbeat thread, and clear
    // ConnectionManager.
    private static final AtomicInteger emptyCacheTime = new AtomicInteger(0);

    private static final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();

    private static final ReentrantReadWriteLock.ReadLock readLock = reentrantLock.readLock();

    private static final ReentrantReadWriteLock.WriteLock writeLock = reentrantLock.writeLock();

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
     * Set connection into connection manager. If set, start singleton heart beating thread.
     *
     * @param pgConnection pgConnection
     * @param props properties
     * @throws PSQLException parameters parsed failed.
     */
    public static void setConnection(PgConnection pgConnection, Properties props) throws PSQLException {
        if (!ConnectionManager.checkEnableLeastConn(props)) {
            return;
        }
        try {
            readLock.lock();
            if (ConnectionManager.getInstance().setConnection(pgConnection, props)) {
                LoadBalanceHeartBeating.startScheduledExecutorService(props);
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Start scheduled executor service. There are two singleton scheduled executor service.
     * If cluster has configured 'autoBalance=leastconn', jdbc will start checkConnectionScheduledExecutorService.
     * If cluster has configured 'autoBalance=leastconn&enableQuickAutoBalance=true', jdbc will start
     * closeConnectionExecutorService.
     *
     * @param properties properties
     */
    public static void startScheduledExecutorService(Properties properties) {
        // Both two heartBeating thread has started.
        if (leastConnStarted && quickAutoBalanceStarted) {
            return;
        }
        // The connection doesn't enable leastconn.
        if (!ConnectionManager.checkEnableLeastConn(properties)) {
            return;
        }
        // CheckClusterStateHeartBeatingThread has started, and the connection doesn't enable quickAutoBalance.
        if (leastConnStarted && !ConnectionManager.checkEnableQuickAutoBalance(properties)) {
            return;
        }
        synchronized (LoadBalanceHeartBeating.class) {
            if (!leastConnStarted && ConnectionManager.checkEnableLeastConn(properties)) {
                leastConnStarted = true;
                checkClusterStateScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
                    new Thread(r, "checkClusterStateHeartBeatingThread"));
                checkClusterStateScheduledExecutorService.scheduleAtFixedRate(LoadBalanceHeartBeating::checkClusterStateScheduleTask,
                    INITIAL_DELAY, CHECK_CLUSTER_STATE_PERIOD, TimeUnit.MILLISECONDS);
                LOGGER.info(GT.tr("Start scheduleExecutorService, period:{0} milliseconds.",
                    CHECK_CLUSTER_STATE_PERIOD));
            }
            if (!quickAutoBalanceStarted && ConnectionManager.checkEnableQuickAutoBalance(properties)) {
                quickAutoBalanceStarted = true;
                closeConnectionExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
                    new Thread(r, "closeConnectionsHeartBeatingThread"));
                closeConnectionExecutorService.scheduleAtFixedRate(LoadBalanceHeartBeating::closeAbandonedConnections,
                    INITIAL_DELAY, CLOSE_CONNECTION_PERIOD, TimeUnit.MILLISECONDS);
                LOGGER.info(GT.tr("Start closeConnectionScheduledFuture, period:{0} milliseconds.",
                    CLOSE_CONNECTION_PERIOD));
            }
        }
    }

    private static void checkClusterStateScheduleTask() {
        checkClusterState();
        checkConnectionValidity();
        checkHeartBeatingThreadShouldStop();
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

    private static void checkHeartBeatingThreadShouldStop() {
        int maxCachedConnectionsEmptyTimesBeforeClear = 2;
        int cachedConnectionSize = ConnectionManager.getInstance().getCachedConnectionSize();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(GT.tr("CachedConnectionSize = {0}.", cachedConnectionSize));
        }
        if (cachedConnectionSize != 0) {
            emptyCacheTime.set(0);
            return;
        }
        emptyCacheTime.incrementAndGet();
        if (emptyCacheTime.get() >= maxCachedConnectionsEmptyTimesBeforeClear) {
            try{
                writeLock.lock();
                if (ConnectionManager.getInstance().getCachedConnectionSize() == 0) {
                    emptyCacheTime.set(0);
                    stopHeartBeatingThread();
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * Stop scheduled executor service, and clear connection manager.
     */
    public static void stopHeartBeatingThread() {
        if (!leastConnStarted && !quickAutoBalanceStarted) {
            return;
        }
        synchronized (LoadBalanceHeartBeating.class) {
            if (leastConnStarted) {
                checkClusterStateScheduledExecutorService.shutdownNow();
                checkClusterStateScheduledExecutorService = null;
                leastConnStarted = false;
                LOGGER.info(GT.tr("ScheduledExecutorService: {0} close.", "loadBalanceHeartBeatingThread"));
            }
            if (quickAutoBalanceStarted) {
                closeConnectionExecutorService.shutdownNow();
                closeConnectionExecutorService = null;
                quickAutoBalanceStarted = false;
                LOGGER.info(GT.tr("ScheduledExecutorService: {0} close.", "closeConnectionsHeartBeatingThread"));
            }
            ConnectionManager.getInstance().clear();
        }
    }
}
