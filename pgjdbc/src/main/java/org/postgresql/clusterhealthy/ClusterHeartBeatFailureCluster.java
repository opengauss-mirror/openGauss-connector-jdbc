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

package org.postgresql.clusterhealthy;

import org.postgresql.core.QueryExecutor;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * After the primary node breaks down during heartbeat maintenance,
 * the cluster is in the no-host state. This section describes how to detect hosts in a cluster without hosts.
 */
public class ClusterHeartBeatFailureCluster extends ClusterHeartBeat{

    public List<FailureCluster> failureCluster = new ArrayList<>();
    private int thresholdValue;
    private volatile static ClusterHeartBeatFailureCluster clusterHeartBeatFailureCluster;
    private static Log LOGGER = Logger.getLogger(ClusterHeartBeatFailureCluster.class.getName());
    private ClusterHeartBeatFailureCluster() {

    }

    public static synchronized ClusterHeartBeatFailureCluster getInstance() {
        if (clusterHeartBeatFailureCluster == null) {
            clusterHeartBeatFailureCluster = new ClusterHeartBeatFailureCluster();
        }
        return clusterHeartBeatFailureCluster;
    }

    /**
     * After the active node fails, find a new active node on the standby node or check whether the active node is resurrected
     */
    public void run() {
        if (failureCluster.isEmpty()) {
            return;
        }
        List<FailureCluster> list = new ArrayList<>(failureCluster);
        failureCluster.clear();
        LOGGER.debug("cluster does not have a master node" + list);
        for (FailureCluster cluster : list) {
            QueryExecutor queryExecutor = null;
            try {
                if (cluster == null || cluster.getMaster() == null) {
                    continue;
                }
                queryExecutor = getQueryExecutor(cluster.getMaster(), cluster.getProps());
            } catch (SQLException e) {
                Set<HostSpec> salves = cluster.getSalves();
                int count = 0;
                for (HostSpec salf : salves) {
                    QueryExecutor salveQueryExcutor = null;
                    try {
                        salveQueryExcutor = getQueryExecutor(salf, cluster.getProps());
                    } catch (SQLException ex) {
                        count++;
                    } finally {
                        if (salveQueryExcutor != null) {
                            salveQueryExcutor.close();
                        }
                    }
                }
                if (count == salves.size()) {
                    continue;
                }
                int frequency = cluster.getFrequency();
                if (thresholdValue > frequency) {
                    cacheProcess(cluster.getMaster(), salves, cluster.getProps(), ++ frequency);
                }
            }
            if (queryExecutor != null) {
                boolean isMaster = nodeRoleIsMaster(queryExecutor);
                if (isMaster) {
                    addClusterNode(cluster.getMaster(), cluster.getSalves().toArray(new HostSpec[0]));
                    addProperties(cluster.getMaster(), cluster.getProps());
                } else {
                    HostSpec maseterNode = findMasterNode(cluster.getSalves(), cluster.getProps());
                    if (maseterNode != null) {
                        addProperties(maseterNode, cluster.getProps());
                        Set<HostSpec> salves = cluster.getSalves();
                        salves.add(cluster.getMaster());
                        removeClusterNode(cluster.getMaster(), maseterNode, salves);
                    }
                }
            }
        }
    }

    public void addFailureCluster(FailureCluster cluster) {
        failureCluster.add(cluster);
    }

    public List<FailureCluster> getFailureCluster() {
        return failureCluster;
    }

    public void clear() {
        failureCluster.clear();
    }

    public void setThresholdValue(int thresholdValue) {
        this.thresholdValue = thresholdValue;
    }

}
