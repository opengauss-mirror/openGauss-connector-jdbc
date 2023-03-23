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

package org.postgresql.clusterhealthy;

import org.postgresql.PGProperty;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 *  Cluster information processing, cache connection information, whether to start the heartbeat thread
 */
public class ClusterNodeCache {
    private static volatile boolean status;
    private static final Object STATUS_LOCK = new Object();
    private static Log LOGGER = Logger.getLogger(ClusterNodeCache.class.getName());
    private static ExecutorService executorService = null;
    private static final ClusterHeartBeat CLUSTER_HEART_BEAT = new ClusterHeartBeat();

    public static boolean isOpen() {
        return status;
    }

    /**
     * The faulty host is switched to a new host
     * @param hostSpecs the parsed/defaulted connection properties
     */
    public static void checkReplacement(HostSpec[] hostSpecs) {
        if (hostSpecs == null) {
            return;
        }
        ClusterHeartBeatFailureMaster failureMaster = ClusterHeartBeatFailureMaster.getInstance();
        Map<HostSpec, HostSpec> failureMap = failureMaster.getFailureMaster();
        for (int i = 0; i < hostSpecs.length; i++) {
            while (failureMap.containsKey(hostSpecs[i])) {
                if (hostSpecs[i] == failureMap.get(hostSpecs[i])) {
                    failureMaster.remove(hostSpecs[i]);
                    return;
                }
                hostSpecs[i] = failureMap.get(hostSpecs[i]);
            }
        }
    }

    /**
     * Verify parameters and replace the failed primary node
     * @param hostSpecs cluster node
     */
    public static void checkHostSpecs(HostSpec[] hostSpecs) throws PSQLException {
        // check the interval of heartbeat threads.
        Set<HostSpec> set = Arrays.stream(hostSpecs)
                .collect(Collectors.toSet());
        if (set.size() > 1) {
            checkReplacement(hostSpecs);
        }
    }

    /**
     *
     * @param master master node
     * @param hostSpecs cluster node
     * @param properties the parsed/defaulted connection properties
     */
    public static void pushHostSpecs(HostSpec master, HostSpec[] hostSpecs, Properties properties) {
        Set<HostSpec> set = Arrays.stream(hostSpecs)
                .collect(Collectors.toSet());
        String period = PGProperty.HEARTBEAT_PERIOD.get(properties);
        if (!isNumeric(period)) {
            LOGGER.debug("Invalid heartbeatPeriod value: " + period);
            return;
        }
        long timePeriod = Long.parseLong(period);
        if (timePeriod <= 0) {
            LOGGER.debug("Invalid heartbeatPeriod value: " + period);
            return;
        }
        if (set.size() > 1) {
            CLUSTER_HEART_BEAT.addNodeRelationship(master, hostSpecs, properties);
            start();
        }
    }

    private static void start() {
        if (status) {
            LOGGER.info("heartbeat thread ----> started");
            return;
        }
        synchronized (STATUS_LOCK) {
            if (status) {
                LOGGER.info("heartbeat thread ----> started");
                return;
            }
            status = true;
        }
        if (executorService == null) {
            executorService = Executors.newSingleThreadExecutor();
        }
        executorService.execute(CLUSTER_HEART_BEAT::masterNodeProbe);
    }

    public static void stop() {
        synchronized (STATUS_LOCK) {
            status = false;
            CLUSTER_HEART_BEAT.clear();
            CLUSTER_HEART_BEAT.initPeriodTime();
            executorService.shutdown();
            executorService = Executors.newSingleThreadExecutor();
        }
    }

    public static boolean isNumeric(final CharSequence cs) {
        if (cs.length() == 0) {
            return false;
        }
        final int size = cs.length();
        for (int i = 0; i < size; i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }


    public static void updateDetection() {
        synchronized (STATUS_LOCK) {
            CLUSTER_HEART_BEAT.updateDetection();
        }
    }
}
