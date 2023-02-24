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
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.SocketFactoryFactory;
import org.postgresql.core.v3.ConnectionFactoryImpl;
import org.postgresql.core.v3.QueryExecutorImpl;
import org.postgresql.jdbc.SslMode;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;

import javax.net.SocketFactory;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.postgresql.GlobalConnectionTracker.closeConnectionOfCrash;
import static org.postgresql.GlobalConnectionTracker.getConnections;
import static org.postgresql.clusterhealthy.ClusterNodeCache.isOpen;
import static org.postgresql.util.PSQLState.CONNECTION_REJECTED;

/**
 * Heartbeat detection, active detection of the primary node, and maintenance of cache after failure The next
 * contest pair active detection of the failed node, detection of the cluster where the primary node has failed,
 * and maintenance of the latest information to the cache set。
 */
public class ClusterHeartBeat {

    public static final Map<HostSpec, Set<Properties>> CLUSTER_PROPERTIES = new ConcurrentHashMap<>();
    private static Log LOGGER = Logger.getLogger(ClusterHeartBeat.class.getName());
    private static final ConnectionFactoryImpl FACTORY = new ConnectionFactoryImpl();
    private static final String UPDATE_TIME = "time";
    private volatile Long periodTime = 5000L;


    /**
     * heartbeat task thread.
     */
    public void masterNodeProbe() {
        while (isOpen()) {
            LOGGER.debug("heartBeat thread start time: " + new Date(System.currentTimeMillis()));
            // failed node detection
            ClusterHeartBeatFailureMaster.getInstance().run();

            // active primary node detection
            ClusterHeartBeatMaster.getInstance().run();
            // The failed cluster seeks the primary node
            ClusterHeartBeatFailureCluster.getInstance().run();
            try {
                Thread.sleep(periodTime);
            } catch (InterruptedException e) {
                LOGGER.debug(e.getStackTrace());
            }
        }
        periodTime = 5000L;
    }

    /**
     * get parsed/defaulted connection properties
     * @param hostSpec ip and port
     * @return properties set
     */
    public Set<Properties> getProperties(HostSpec hostSpec) {
        synchronized (CLUSTER_PROPERTIES) {
            return CLUSTER_PROPERTIES.computeIfAbsent(hostSpec, k -> new HashSet<>());
        }
    }

    public Map<HostSpec, Set<HostSpec>> getClusterRelationship () {
        return ClusterHeartBeatMaster.getInstance().getClusterRelationship();
    }

    /**
     * Adding Cluster Information
     * @param key master
     * @param value secondary list
     * @param properties the parsed/defaulted connection properties
     */
    public void addNodeRelationship (HostSpec key, HostSpec[] value, Properties properties) {
        // update node host and ip
        addClusterNode(key, value);
        // update node properties
        addProperties(key,  Collections.singleton(properties));
        if (PGProperty.HEARTBEAT_PERIOD.get(properties) != null) {
            String period = PGProperty.HEARTBEAT_PERIOD.get(properties);
            long time = Long.parseLong(period);
            synchronized (UPDATE_TIME) {
                if (time > 0) {
                    periodTime = Math.min(periodTime, time);
                }
            }
        }
    }

    /**
     *
     * @param hostSpec ip and port
     * @param properties the parsed/defaulted connection properties
     */
    public void addProperties(HostSpec hostSpec, Set<Properties> properties) {
        synchronized (CLUSTER_PROPERTIES) {
            Set<Properties> propertiesSet = CLUSTER_PROPERTIES.get(hostSpec);
            if (propertiesSet == null) {
                propertiesSet = new HashSet<>();
            }
            propertiesSet.addAll(properties);
            CLUSTER_PROPERTIES.put(hostSpec, propertiesSet);
        }
    }

    /**
     * Update the cluster node relationship
     * @param key old master
     * @param newKey new master
     * @param slaves slaves set
     */
    public void removeClusterNode(HostSpec key, HostSpec newKey, Set<HostSpec> slaves) {
        ClusterHeartBeatMaster.getInstance().removeClusterNode(key, newKey, slaves);

    }

    /**
     * Perform cache maintenance on cluster nodes connected to hosts
     * @param hostSpecs the parsed/defaulted connection properties
     * @param value slaves set
     */
    public void addClusterNode(HostSpec hostSpecs, HostSpec... value) {
        ClusterHeartBeatMaster.getInstance().addClusterNode(hostSpecs, value);
    }

    /**
     * Get the cluster slave node
     * @param hostSpec the parsed/defaulted connection properties
     * @return slaves set
     */
    public Set<HostSpec> getClusterSalveNode(HostSpec hostSpec) {
        return ClusterHeartBeatMaster.getInstance().getClusterSalveNode(hostSpec);
    }

    /**
     *
     * @param hostSpec ip and port
     * @param properties the parsed/defaulted connection properties
     */
    public void removeProperties(HostSpec hostSpec, Properties properties) {
        synchronized (CLUSTER_PROPERTIES) {
            Set<Properties> propertiesSet = CLUSTER_PROPERTIES.getOrDefault(hostSpec, null);
            if (propertiesSet != null) {
                propertiesSet.remove(properties);
                CLUSTER_PROPERTIES.put(hostSpec, propertiesSet);
            }
        }
    }

    /**
     * the node probes the activity by reflecting the tryConnect() method.
     *
     * @param hostSpec ip and port.
     * @param propSet  the parsed/defaulted connection properties
     * @return QueryExecutor
     * @throws SQLException new sql exception
     */
    public QueryExecutor getQueryExecutor(HostSpec hostSpec, Set<Properties> propSet) throws SQLException {
        Properties props = null;
        try {
            for (Properties properties : propSet) {
                props = properties;
                SocketFactory socketFactory = SocketFactoryFactory.getSocketFactory(props);
                SslMode sslMode = SslMode.of(props);
                String user = props.getProperty("user", "");
                String database = props.getProperty("PGDBNAME", "");
                PGStream pgStream = FACTORY.tryConnect(user, database, props, socketFactory, hostSpec, sslMode);
                return new QueryExecutorImpl(pgStream, user, database,
                        1000, props);
            }
        } catch (SQLException e) {
            String sqlState = e.getSQLState();
            if (CONNECTION_REJECTED.getState().equals(sqlState) || "28P01".equals(sqlState)) {
                LOGGER.debug("node " + hostSpec + " is active, and connenction authentication fails.");
                LOGGER.debug("remove before propSet size :" + propSet.size());
                removeProperties(hostSpec, props);
                LOGGER.debug("remove after propSet size :" + propSet.size());
            }

            LOGGER.debug("acquire QueryExecutor failure " + e.getMessage());
        } catch (IOException e) {
            LOGGER.debug("acquire QueryExecutor failure " + e.getMessage());
            LOGGER.debug(e.getCause());
        }
        throw new SQLException();
    }

    /**
     * Check whether the node is the primary node
     *
     * @param queryExecutor queryExector
     * @return true/false
     */
    public boolean nodeRoleIsMaster(QueryExecutor queryExecutor) {
        try {
            return FACTORY.isMaster(queryExecutor);
        } catch (SQLException | IOException e) {
            LOGGER.debug("Error obtaining node role " + e.getMessage());
            LOGGER.debug(e.getStackTrace());
            return false;
        }
    }

    /**
     * Post-processing after the primary node fails
     *
     * @param hostSpec master ip and port
     * @param slaves   slaves set
     * @param props    the parsed/defaulted connection properties
     */
    public void cacheProcess(HostSpec hostSpec, Set<HostSpec> slaves, Set<Properties> props) {
        HostSpec maseterNode = findMasterNode(slaves, props);
        removeClusterNode(hostSpec, maseterNode, slaves);
        if (maseterNode != null) {
            addProperties(maseterNode, props);
            ClusterHeartBeatFailureMaster.getInstance().addFailureMaster(hostSpec, maseterNode);
        } else {
            FailureCluster cluster = new FailureCluster(hostSpec, slaves, props);
            ClusterHeartBeatFailureCluster.getInstance().addFailureCluster(cluster);
        }
        closeConnectionOfCrash(hostSpec.toString());
    }

    /**
     * Locate the host on the standby computer
     *
     * @param hostSpecSet slaves set
     * @param properties  the parsed/defaulted connection properties
     * @return new master node
     */
    public HostSpec findMasterNode(Set<HostSpec> hostSpecSet, Set<Properties> properties) {
        for (HostSpec hostSpec : hostSpecSet) {
            List<QueryExecutor> queryExecutorList = getConnections(hostSpec.toString());
            for (QueryExecutor executor : queryExecutorList) {
                if (!executor.isClosed() && nodeRoleIsMaster(executor)) {
                    return hostSpec;
                }
            }
            QueryExecutor queryExecutor = null;
            try {
                queryExecutor = getQueryExecutor(hostSpec, properties);
            } catch (SQLException e) {
                continue;
            }
            boolean isMaster = nodeRoleIsMaster(queryExecutor);
            if (isMaster) {
                return hostSpec;
            }
        }
        return null;
    }

}
