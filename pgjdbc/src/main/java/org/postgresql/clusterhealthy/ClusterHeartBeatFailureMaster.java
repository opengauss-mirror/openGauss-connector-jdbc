/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.postgresql.clusterhealthy;

import org.postgresql.core.QueryExecutor;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * If only one host in the cluster is down, and a new host is selected,
 * the relationship between the faulty host and the new host needs to be maintained.
 * When the faulty host recovers, it needs to be added to the node again for maintenance
 */
public class ClusterHeartBeatFailureMaster extends ClusterHeartBeat{

    public Map<HostSpec, HostSpec> failureMap = new ConcurrentHashMap<>();
    private volatile static ClusterHeartBeatFailureMaster clusterHeartBeatFailureMaster;
    private static Log LOGGER = Logger.getLogger(ClusterHeartBeatFailureMaster.class.getName());
    private ClusterHeartBeatFailureMaster() {

    }

    public static synchronized ClusterHeartBeatFailureMaster getInstance() {
        if (clusterHeartBeatFailureMaster == null) {
            clusterHeartBeatFailureMaster = new ClusterHeartBeatFailureMaster();
        }
        return clusterHeartBeatFailureMaster;
    }

    /**
     * If the failed node is alive, join cache maintenance
     */
    public void run() {
        HashMap<HostSpec, HostSpec> failureMapClone = new HashMap<>(failureMap);
        LOGGER.debug("failure node " + failureMapClone);
        for (Map.Entry<HostSpec, HostSpec> next : failureMapClone.entrySet()) {
            HostSpec key = next.getKey();
            HostSpec value = next.getValue();
            Set<Properties> properties = getProperties(key);
            QueryExecutor queryExecutor = null;
            try {
                queryExecutor = getQueryExecutor(key, properties);
                failureMap.remove(key);
            } catch (SQLException e) {
                LOGGER.error(key.toString() + " tryConnect failure.");
                continue;
            }
            boolean isMaster = nodeRoleIsMaster(queryExecutor);
            if (isMaster) {
                HostSpec current = value;
                while (failureMap.containsKey(current)) {
                    current = failureMap.get(current);
                }
                if (getClusterRelationship().containsKey(current)) {
                    Set<Properties> prop = getProperties(key);
                    boolean currentIsMaster;
                    try {
                        QueryExecutor currentQueryExecutor = getQueryExecutor(current, prop);
                        currentIsMaster = nodeRoleIsMaster(currentQueryExecutor);
                    } catch (SQLException e) {
                        currentIsMaster = false;
                    }
                    if (!currentIsMaster) {
                        Set<HostSpec> set = getClusterSalveNode(current);
                        set.add(current);
                        addClusterNode(key, set.toArray(new HostSpec[0]));
                    }
                }

            } else {
                HostSpec current = value;
                while (failureMap.containsKey(current)) {
                    if (current == failureMap.get(current)) {
                        failureMap.remove(current);
                        break;
                    }
                    current = failureMap.get(current);
                }
                addClusterNode(current, key);
            }
        }
    }

    public void addFailureMaster(HostSpec hostSpec, HostSpec maseterNode) {
        failureMap.put(hostSpec, maseterNode);
    }

    public Map<HostSpec, HostSpec> getFailureMaster() {
        return failureMap;
    }

    public void remove(HostSpec hostSpec) {
        failureMap.remove(hostSpec);
    }

    public void clear() {
        failureMap.clear();
    }

}
