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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * After the primary node breaks down during heartbeat maintenance,
 * the cluster is in the no-host state. This section describes how to detect hosts in a cluster without hosts.
 */
public class ClusterHeartBeatFailureCluster extends ClusterHeartBeat{

    public List<FailureCluster> failureCluster = new ArrayList<>();
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
                cacheProcess(cluster.getMaster(), salves, cluster.getProps());
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

}
