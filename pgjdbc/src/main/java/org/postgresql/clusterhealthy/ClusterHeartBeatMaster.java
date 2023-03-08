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

import org.postgresql.core.QueryExecutor;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintain the primary node of the cluster. When the primary node breaks down,
 * you need to detect a new host and bind the faulty host to the new host to facilitate
 * quick switchover when the failed node is connected
 */
public class ClusterHeartBeatMaster extends ClusterHeartBeat {

    public final Map<HostSpec, Set<HostSpec>> CLUSTER_NODE_RELATIONSHIP = new ConcurrentHashMap<>();
    private volatile static ClusterHeartBeatMaster ClusterHeartBeatMaster;
    private static Log LOGGER = Logger.getLogger(ClusterHeartBeatMaster.class.getName());

    private final Object NODE_LOCK = new Object();

    private ClusterHeartBeatMaster() {

    }

    public static synchronized ClusterHeartBeatMaster getInstance() {
        if (ClusterHeartBeatMaster == null) {
            ClusterHeartBeatMaster = new ClusterHeartBeatMaster();
        }
        return ClusterHeartBeatMaster;
    }

    /**
     * the primary node is active and added to the failure set after failure
     */
    public void run() {
        Map<HostSpec, Set<HostSpec>> clusterRelationship = getClusterRelationship();
        Iterator<Map.Entry<HostSpec, Set<HostSpec>>> iterator = clusterRelationship.entrySet().iterator();
        LOGGER.debug("master nodes " + clusterRelationship);
        while (iterator.hasNext()) {
            Map.Entry<HostSpec, Set<HostSpec>> nodeMap = iterator.next();
            HostSpec master = nodeMap.getKey();
            Set<HostSpec> slaves = nodeMap.getValue();
            LOGGER.debug("Current node " + master + " Standby node " + slaves);
            QueryExecutor queryExecutor = null;
            Set<Properties> propertiesSet = getProperties(master);
            try {
                queryExecutor = super.getQueryExecutor(master, propertiesSet);
            } catch (SQLException e) {
                LOGGER.debug("acquire QueryExecutor failure");
                super.cacheProcess(master, slaves, propertiesSet);
                continue;
            }
            LOGGER.debug("Information about the current connected node " + queryExecutor.getSocketAddress());
            if (!super.nodeRoleIsMaster(queryExecutor)) {
                LOGGER.debug(master + ":The host is degraded to the standby server.");
                super.cacheProcess(master, slaves, propertiesSet);
            }
        }
    }

    public Map<HostSpec, Set<HostSpec>> getClusterRelationship() {
        return CLUSTER_NODE_RELATIONSHIP;
    }

    public void addClusterNode(HostSpec hostSpecs, HostSpec... value) {
        synchronized (NODE_LOCK) {
            Set<HostSpec> hostSpecSet = CLUSTER_NODE_RELATIONSHIP.computeIfAbsent(hostSpecs, k -> new HashSet<>());
            Arrays.stream(value)
                    .filter(host -> !host.equals(hostSpecs))
                    .forEach(hostSpecSet::add);
            CLUSTER_NODE_RELATIONSHIP.put(hostSpecs, hostSpecSet);
        }
    }

    public void removeClusterNode(HostSpec key, HostSpec newKey, Set<HostSpec> slaves) {
        synchronized (NODE_LOCK) {
            CLUSTER_NODE_RELATIONSHIP.remove(key);
            if (newKey != null) {
                Set<HostSpec> hostSpecSet = CLUSTER_NODE_RELATIONSHIP.get(newKey);
                if (hostSpecSet == null) {
                    hostSpecSet = new HashSet<>();
                }
                hostSpecSet.addAll(slaves);
                hostSpecSet.remove(newKey);
                CLUSTER_NODE_RELATIONSHIP.put(newKey, hostSpecSet);
            }
        }
    }

    public Set<HostSpec> get(HostSpec hostSpec) {
        synchronized (NODE_LOCK) {
            return CLUSTER_NODE_RELATIONSHIP.get(hostSpec);
        }
    }

    public void clear() {
        synchronized (NODE_LOCK) {
            CLUSTER_NODE_RELATIONSHIP.clear();
        }
    }


}
