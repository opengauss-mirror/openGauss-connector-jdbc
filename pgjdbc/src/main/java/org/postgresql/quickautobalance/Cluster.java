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

import org.postgresql.Driver;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.StatementCancelState;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.quickautobalance.DataNode.CheckDnStateResult;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A Cluster, which cached the information of  the dataNodes in this cluster.
 */
public class Cluster {
    private static Log LOGGER = Logger.getLogger(Cluster.class.getName());

    private static final double CLOSE_CONNECTION_PERCENTAGE_EACH_TIME = 0.2d;

    private static final int MIN_RESERVED_CON_UNSET_PARAMS = -1;

    private final String urlIdentifier;

    private final Set<HostSpec> dns;

    private final Queue<ConnectionInfo> abandonedConnectionList;

    private final Map<HostSpec, DataNode> cachedDnList;

    private final List<Properties> cachedPropertiesList;

    // Percentage of minimum reserved connections that can be closed in a cluster, value range: [0,100].
    private volatile int minReservedConPerCluster;

    private volatile boolean enableMinReservedConPerCluster;

    // Percentage of minimum reserved connections that can be closed in a datanode, value range: [0,100].
    private volatile int minReservedConPerDatanode;

    private volatile boolean enableMinReservedConPerDatanode;

    private volatile long quickAutoBalanceStartTime;

    private int totalAbandonedConnectionSize;

    public Cluster(final String urlIdentifier, final Properties properties) throws PSQLException {
        this.urlIdentifier = urlIdentifier;
        HostSpec[] hostSpecs = Driver.getURLHostSpecs(properties);
        this.dns = new HashSet<>();
        this.dns.addAll(Arrays.asList(hostSpecs));
        this.cachedDnList = new ConcurrentHashMap<>();
        for (HostSpec hostSpec : hostSpecs) {
            DataNode dataNode = new DataNode(hostSpec);
            this.cachedDnList.put(hostSpec, dataNode);
        }
        updateParams(properties);
        this.abandonedConnectionList = new ConcurrentLinkedQueue<>();
        this.cachedPropertiesList = new Vector<>();
        this.cachedPropertiesList.add(properties);
        this.quickAutoBalanceStartTime = 0;
        this.totalAbandonedConnectionSize = 0;
    }

    /**
     * set connection state of cached connections if it exists.
     *
     * @param pgConnection pgConnection
     * @param state new state
     */
    public void setConnectionState(final PgConnection pgConnection, final StatementCancelState state) {
        String socketAddress = pgConnection.getSocketAddress();
        HostSpec hostSpec = calculateHostSpec(socketAddress);
        if (hostSpec != null && !dns.contains(hostSpec)) {
            return;
        }
        DataNode dataNode = cachedDnList.get(hostSpec);
        if (dataNode != null) {
            dataNode.setConnectionState(pgConnection, state);
        }
    }

    // calculate the hostSpec of destination
    private HostSpec calculateHostSpec(String socketAddress) {
        String urlClient = socketAddress.split("/")[1];
        String[] urlClientSplit = urlClient.split(":");
        if (urlClientSplit.length == 2) {
            String host = urlClientSplit[0];
            int port = Integer.parseInt(urlClientSplit[1]);
            return new HostSpec(host, port);
        } else {
            return null;
        }
    }

    /**
     * Add a new connection.
     *
     * @param pgConnection pgConnection
     * @param properties properties
     */
    public void setConnection(final PgConnection pgConnection, final Properties properties)
        throws PSQLException {
        if (pgConnection == null || properties == null) {
            return;
        }
        String socketAddress = pgConnection.getSocketAddress();
        HostSpec hostSpec = calculateHostSpec(socketAddress);
        if (hostSpec != null && !dns.contains(hostSpec)) {
            return;
        }
        setProperties(properties);
        synchronized (cachedDnList) {
            cachedDnList.get(hostSpec).setConnection(pgConnection, properties, hostSpec);
            decrementCachedCreatingConnectionSize(hostSpec);
            updateParams(properties);
        }
    }

    /**
     * Set new properties if the user doesn't exist in cachedPropertiesList,
     * or update properties if exists.
     *
     * @param properties properties
     */
    public void setProperties(Properties properties) {
        synchronized (cachedPropertiesList) {
            for (int i = 0; i < cachedPropertiesList.size(); i++) {
                Properties prop = cachedPropertiesList.get(i);
                if (prop.getProperty("user", "").equals(properties.getProperty("user", null))) {
                    cachedPropertiesList.set(i, properties);
                    return;
                }
            }
            cachedPropertiesList.add(properties);
        }
    }

    /**
     * CacheCreatingConnectionNum - 1;
     *
     * @param hostSpec hostSpec
     * @return cachedCreatingConnectionNum
     */
    public int decrementCachedCreatingConnectionSize(final HostSpec hostSpec) {
        if (!cachedDnList.containsKey(hostSpec)) {
            LOGGER.error(GT.tr("Can not find hostSpec: {0} in Cluster: {1}.", hostSpec.toString(), urlIdentifier));
            return 0;
        }
        DataNode dataNode = cachedDnList.get(hostSpec);
        if (dataNode != null) {
            return dataNode.decrementCachedCreatingConnectionSize();
        } else {
            LOGGER.error(GT.tr("Can not find hostSpec: {0} in Cluster: {1}.", hostSpec.toString(), urlIdentifier));
            return 0;
        }
    }

    private void updateMinReservedConPerCluster(Properties properties) throws PSQLException {
        int perCluster = parseMinReservedConPerCluster(properties);
        if (perCluster == MIN_RESERVED_CON_UNSET_PARAMS) {
            return;
        }
        if (this.enableMinReservedConPerCluster) {
            this.minReservedConPerCluster = Math.min(this.minReservedConPerCluster, perCluster);
        } else {
            this.enableMinReservedConPerCluster = true;
            this.minReservedConPerCluster = perCluster;
        }
    }

    private void updateMinReservedConPerDatanode(Properties properties) throws PSQLException {
        int perDatanode = parseMinReservedConPerDatanode(properties);
        if (perDatanode == MIN_RESERVED_CON_UNSET_PARAMS) {
            return;
        }
        if (this.enableMinReservedConPerDatanode) {
            this.minReservedConPerDatanode = Math.min(this.minReservedConPerDatanode, perDatanode);
        } else {
            this.enableMinReservedConPerDatanode = true;
            this.minReservedConPerDatanode = perDatanode;
        }
    }

    /**
     * Parse minReservedConPerCluster, value range: [0, 100].
     * return -1 if minReservedConPerCluster isn't configured.
     *
     * @param properties properties
     * @return minReservedConPerCluster
     * @throws PSQLException minReservedConPerCluster parse failed
     */
    public static int parseMinReservedConPerCluster(Properties properties) throws PSQLException {
        int perCluster;
        String param = PGProperty.MIN_RESERVED_CON_PER_CLUSTER.get(properties);
        if (param == null) {
            return MIN_RESERVED_CON_UNSET_PARAMS;
        }
        try {
            perCluster = Integer.parseInt(param);
        } catch (NumberFormatException e) {
            throw new PSQLException(GT.tr("Parameter minReservedConPerCluster={0} parsed failed, value range: int && [0, 100]."
                , PGProperty.MIN_RESERVED_CON_PER_CLUSTER.get(properties)), PSQLState.INVALID_PARAMETER_TYPE);
        }
        if (perCluster < 0 || perCluster > 100) {
            throw new PSQLException(GT.tr("Parameter minReservedConPerCluster={0} parsed failed, value range: int && [0, 100]."
                , perCluster), PSQLState.INVALID_PARAMETER_VALUE);
        } else {
            return perCluster;
        }
    }

    /**
     * Parse minReservedConPerDatanode, value range: [0, 100].
     * return -1 if minReservedConPerDatanode isn't configured.
     *
     * @param properties properties
     * @return minReservedConPerDatanode
     * @throws PSQLException minReservedConPerDatanode parse failed
     */
    public static int parseMinReservedConPerDatanode(Properties properties) throws  PSQLException {
        int perDatanode;
        String param = PGProperty.MIN_RESERVED_CON_PER_DATANODE.get(properties);
        if (param == null) {
            return MIN_RESERVED_CON_UNSET_PARAMS;
        }
        try {
            perDatanode = Integer.parseInt(param);
        } catch (NumberFormatException e) {
            throw new PSQLException(GT.tr("Parameter minReservedConPerDatanode={0} parsed failed, value range: int && [0, 100]."
                , PGProperty.MIN_RESERVED_CON_PER_DATANODE.get(properties)), PSQLState.INVALID_PARAMETER_TYPE);
        }

        if (perDatanode < 0 || perDatanode > 100) {
            throw new PSQLException(GT.tr("Parameter minReservedConPerDatanode={0} parsed failed, value range: " +
                "int && [0, 100].", perDatanode), PSQLState.INVALID_PARAMETER_VALUE);
        } else {
            return perDatanode;
        }
    }

    private void updateParams(Properties properties) throws PSQLException {
        updateMinReservedConPerCluster(properties);
        updateMinReservedConPerDatanode(properties);
    }

    /**
     * Get connection info of the connection if exists.
     *
     * @param connection connection
     * @return connection info
     */
    public ConnectionInfo getConnectionInfo(PgConnection connection) {
        String socketAddress = connection.getSocketAddress();
        HostSpec hostSpec = calculateHostSpec(socketAddress);
        DataNode dataNode = cachedDnList.get(hostSpec);
        return hostSpec != null && dataNode != null ? dataNode.getConnectionInfo(connection) : null;
    }

    /**
     * Sort hostSpec list in ascending order by amount of connections.
     * Put the hostSpec to the tail, if dataNodeState = false.
     *
     * @param hostSpecs host specs
     * @return hostSpec list
     */
    public List<HostSpec> sortDnsByLeastConn(List<HostSpec> hostSpecs) {
        Map<HostSpec, DataNodeCompareInfo> dataNodeCompareInfoMap;
        // Copy dataNodeCompareInfo from cachedDnList.
        synchronized (cachedDnList) {
            dataNodeCompareInfoMap = hostSpecs.stream()
                .collect(Collectors.toMap(Function.identity(), hostSpec -> {
                    DataNode dataNode = cachedDnList.get(hostSpec);
                    int cachedConnectionListSize = dataNode.getCachedConnectionListSize();
                    int cachedCreatingConnectionSize = dataNode.getCachedCreatingConnectionSize();
                    boolean dataNodeState = dataNode.getDataNodeState();
                    return new DataNodeCompareInfo(cachedConnectionListSize, cachedCreatingConnectionSize, dataNodeState);
                }));
        }
        hostSpecs.sort((o1, o2) -> {
            boolean o1State = dataNodeCompareInfoMap.get(o1).getDataNodeState();
            boolean o2State = dataNodeCompareInfoMap.get(o2).getDataNodeState();
            if (!o1State && o2State) {
                return 1;
            }
            if (!o2State && o1State) {
                return -1;
            }
            int o1ConnectionSize = dataNodeCompareInfoMap.get(o1).getConnectionListSize() + dataNodeCompareInfoMap.get(o1).getCachedCreatedConnectionSize();
            int o2ConnectionSize = dataNodeCompareInfoMap.get(o2).getConnectionListSize() + dataNodeCompareInfoMap.get(o2).getCachedCreatedConnectionSize();
            return o1ConnectionSize - o2ConnectionSize;
        });
        if (hostSpecs.get(0) != null) {
            this.cachedDnList.get(hostSpecs.get(0)).incrementCachedCreatingConnectionSize();
        }
        LOGGER.info(GT.tr("SortDnsByLeastConn:  {0}."
            , dataNodeCompareInfoMap));
        return hostSpecs;
    }

    /**
     * The necessary info when sorting data nodes.
     */
    class DataNodeCompareInfo {
        int connectionListSize;
        int cachedCreatedConnectionSize;
        boolean dataNodeState;

        public DataNodeCompareInfo(final int connectionListSize, final int cachedCreatedConnectionSize, final boolean dataNodeState) {
            this.connectionListSize = connectionListSize;
            this.cachedCreatedConnectionSize = cachedCreatedConnectionSize;
            this.dataNodeState = dataNodeState;
        }

        /**
         * Get connectionList size.
         *
         * @return size of connectionList
         */
        public int getConnectionListSize() {
            return connectionListSize;
        }

        /**
         * Get cached created connection size.
         *
         * @return cachedCreateConnectionSize
         */
        public int getCachedCreatedConnectionSize() {
            return cachedCreatedConnectionSize;
        }

        /**
         * Get data node state.
         *
         * @return dataNodeState
         */
        public boolean getDataNodeState() {
            return dataNodeState;
        }

        @Override
        public String toString() {
            return "{" +
                "connectionListSize=" + connectionListSize +
                ", cachedCreatedConnectionSize=" + cachedCreatedConnectionSize +
                ", dataNodeState=" + dataNodeState +
                '}';
        }
    }

    /**
     * Check each data nodes' validity of cluster,
     * use cached properties to tryConnect to each data nodes,
     * if cached properties is invalid, remove cached properties,
     * if state change to valid from invalid, quick load balancing start,
     * if state change to invalid from valid, clear cached connections of this node.
     *
     * @return the number of invalid data nodes
     */
    public int checkClusterState() {
        // Count the state of all data nodes before and after checking cluster state.
        Map<HostSpec, Boolean> oldStates = cachedDnList.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey,
                (val) -> val.getValue().getDataNodeState()));
        Map<HostSpec, Boolean> newStates = cachedDnList.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey,
                (val) -> this.checkDnState(val.getKey())));
        Map<DataNodeChangedState, List<HostSpec>> checkResult = new HashMap<>();
        for (DataNodeChangedState dataNodeChangedState : DataNodeChangedState.values()) {
            checkResult.put(dataNodeChangedState, new ArrayList<>());
        }
        for (Map.Entry<HostSpec, DataNode> entry : cachedDnList.entrySet()) {
            HostSpec hostSpec = entry.getKey();
            boolean oldState = oldStates.get(hostSpec);
            boolean newState = newStates.get(hostSpec);
            if (oldState && !newState) {
                // If data node fails, clear its cacheConnectionList.
                int removed = cachedDnList.get(hostSpec).clearCachedConnections();
                checkResult.get(DataNodeChangedState.CHANGE_TO_INVALID).add(hostSpec);
                LOGGER.info(GT.tr("A data node failed, clear cached connections, cluster: {0}, " +
                        "hostSpec: {1}, cached connections: {2}.",
                    urlIdentifier, hostSpec.toString(), removed));
            } else if (!oldState && newState) {
                checkResult.get(DataNodeChangedState.CHANGE_TO_VALID).add(hostSpec);
            } else if (oldState) {
                checkResult.get(DataNodeChangedState.KEEP_VALID).add(hostSpec);
            } else {
                checkResult.get(DataNodeChangedState.KEEP_INVALID).add(hostSpec);
            }
        }
        LOGGER.info(GT.tr("Check cluster states in cluster: {0}, result: {1}.",
            urlIdentifier, checkResult.toString()));
        // Start to quickLoadBalance.
        if (checkResult.get(DataNodeChangedState.CHANGE_TO_VALID).size() != 0
            && LoadBalanceHeartBeating.isQuickAutoBalanceStarted()) {
            quickLoadBalance(checkResult.get(DataNodeChangedState.KEEP_VALID));
        }
        return checkResult.get(DataNodeChangedState.KEEP_INVALID).size() +
            checkResult.get(DataNodeChangedState.CHANGE_TO_INVALID).size();
    }

    enum DataNodeChangedState {
        KEEP_VALID, KEEP_INVALID, CHANGE_TO_VALID, CHANGE_TO_INVALID
    }

    /**
     * Check dn state by cached properties, and remove invalid properties.
     *
     * @param hostSpec hostSpec
     * @return state of the date node
     */
    public boolean checkDnState(HostSpec hostSpec) {
        synchronized (cachedPropertiesList) {
            DataNode dataNode = cachedDnList.get(hostSpec);
            if (dataNode == null) {
                return false;
            }
            for (Iterator<Properties> iterator = cachedPropertiesList.iterator(); iterator.hasNext(); ) {
                Properties properties = iterator.next();
                CheckDnStateResult result = dataNode.checkDnStateAndProperties(properties);
                if (CheckDnStateResult.DN_VALID.equals(result)) {
                    dataNode.setDataNodeState(true);
                    return true;
                } else if (CheckDnStateResult.DN_INVALID.equals(result)) {
                    dataNode.setDataNodeState(false);
                    return false;
                } else {
                    iterator.remove();
                }
            }
            dataNode.setDataNodeState(false);
            return false;
        }
    }

    private int quickLoadBalance(List<HostSpec> validDns) {
        synchronized (abandonedConnectionList) {
            this.quickAutoBalanceStartTime = System.currentTimeMillis();
            // the connections added into abandonedConnectionList
            int removed = 0;
            // cachedConnectionList size
            int total = 0;
            // idle connection filtered from cachedConnectionList
            int idle = 0;
            int minReservedConnectionPercentage;
            if (!enableMinReservedConPerCluster) {
                minReservedConnectionPercentage = minReservedConPerDatanode;
            } else if (!enableMinReservedConPerDatanode) {
                minReservedConnectionPercentage = minReservedConPerCluster;
            } else {
                minReservedConnectionPercentage = Math.max(minReservedConPerDatanode, minReservedConPerCluster);
            }
            for (Entry<HostSpec, DataNode> entry : cachedDnList.entrySet()) {
                DataNode dataNode = entry.getValue();
                if (dataNode != null) {
                    total += dataNode.getCachedConnectionListSize();
                }
            }
            // Start to quickLoadBalance.
            HashSet<ConnectionInfo> removedConnectionList = new HashSet<>();
            for (HostSpec hostSpec : validDns) {
                DataNode dataNode = cachedDnList.get(hostSpec);
                if (dataNode != null) {
                    List<ConnectionInfo> idleConnections = dataNode.filterIdleConnections(quickAutoBalanceStartTime);
                    idle += idleConnections.size();
                    int removedConnectionsSize = (int) (idleConnections.size() * (((double) (100 - minReservedConnectionPercentage)) / 100.0));
                    for (int i = 0; i < removedConnectionsSize; i++) {
                        removedConnectionList.add(idleConnections.get(i));
                        removed++;
                    }
                }
            }
            this.abandonedConnectionList.clear();
            this.abandonedConnectionList.addAll(removedConnectionList);
            this.totalAbandonedConnectionSize = abandonedConnectionList.size();
            LOGGER.info(GT.tr("QuickLoadBalancing executes in cluster: {0}, " +
                    "put {1} idle connections into abandonedConnectionList, connections can be closed: {2}, " +
                    "total connection: {3}, minReservedConPerCluster: {4}, minReservedConPerDatanode: {5}.",
                urlIdentifier, removed, idle, total, this.minReservedConPerCluster, this.minReservedConPerDatanode));
            return removed;
        }
    }

    /**
     * Check cached connections validity, and remove invalid connections.
     *
     * @return the amount of removed connections of each dn.
     */
    public List<Integer> checkConnectionsValidity() {
        List<Integer> ans = new ArrayList<>();
        for (Entry<HostSpec, DataNode> entry : cachedDnList.entrySet()) {
            DataNode dataNode = entry.getValue();
            ans.add(dataNode.checkConnectionsValidity());
        }
        return ans;
    }

    /**
     * CachedCreatingConnection + 1
     *
     * @param hostSpec hostSpec
     * @return cachedCreatingConnection
     */
    public int incrementCachedCreatingConnectionSize(final HostSpec hostSpec) {
        if (!cachedDnList.containsKey(hostSpec)) {
            LOGGER.error(GT.tr("Can not find hostSpec: {0} in Cluster: {1}.",
                hostSpec.toString(), urlIdentifier));
            return 0;
        }
        DataNode dataNode = cachedDnList.get(hostSpec);
        if (dataNode != null) {
            return dataNode.incrementCachedCreatingConnectionSize();
        } else {
            LOGGER.error(GT.tr("Can not find hostSpec: {0} in Cluster: {1}.",
                hostSpec.toString(), urlIdentifier));
            return 0;
        }
    }

    /**
     * Get minReservedConPerCluster.
     *
     * @return minReservedConPerCluster
     */
    public int getMinReservedConPerCluster() {
        return minReservedConPerCluster;
    }

    /**
     * Get enableMinReservedConPerCluster.
     *
     * @return enableMinReservedConPerCluster
     */
    public boolean isEnableMinReservedConPerCluster() {
        return enableMinReservedConPerCluster;
    }

    /**
     * Get minReservedConPerDatanode.
     *
     * @return minReservedConPerDatanode
     */
    public int getMinReservedConPerDatanode() {
        return minReservedConPerDatanode;
    }

    /**
     * Get enableMInReservedConPerDatanode.
     *
     * @return enableMInReservedConPerDatanode
     */
    public boolean isEnableMinReservedConPerDatanode() {
        return enableMinReservedConPerDatanode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(urlIdentifier, dns, abandonedConnectionList, cachedDnList, cachedPropertiesList);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final Cluster that = (Cluster) o;
        return Objects.equals(urlIdentifier, that.urlIdentifier) && Objects.equals(dns, that.dns);
    }

    /**
     * Close connections from abandonedConnectionList.
     * Jdbc will continue to pop connections from abandonedConnectionList and determine whether each connection can be closed.
     * If it does, the connection will be close. If it does not, the connection won't be put back into abandonedConnectionList.
     * The number of connections closed at each cluster for each scheduled task at most: 20% * totalAbandonedConnectionSize.
     *
     * @return the number of connections which are closed
     */
    public int closeConnections() {
        int closed = 0;
        double ceilError = 0.001;
        int atMost = (int) (Math.ceil(CLOSE_CONNECTION_PERCENTAGE_EACH_TIME * totalAbandonedConnectionSize) + ceilError);
        synchronized (abandonedConnectionList) {
            if (abandonedConnectionList.isEmpty()) {
                return closed;
            }
            int oldSize = abandonedConnectionList.size();
            while (!abandonedConnectionList.isEmpty() && closed < atMost) {
                ConnectionInfo connectionInfo = abandonedConnectionList.poll();
                HostSpec hostSpec = connectionInfo.getHostSpec();
                // The connections shouldn't be null.
                if (hostSpec == null) {
                    continue;
                }
                // The connections should be valid.
                if (!connectionInfo.checkConnectionIsValid()) {
                    continue;
                }
                // The state of connection may change after put into abandonedConnectionList,
                // so it's necessary to recheck it which can be closed.
                if (!connectionInfo.checkConnectionCanBeClosed(quickAutoBalanceStartTime)) {
                    continue;
                }
                DataNode dataNode = cachedDnList.get(hostSpec);
                if (dataNode == null) {
                    continue;
                }
                boolean hasClosed = dataNode.closeConnection(connectionInfo.getPgConnection());
                if (hasClosed) {
                    closed++;
                }
            }
            if (abandonedConnectionList.isEmpty()) {
                this.quickAutoBalanceStartTime = 0;
                this.totalAbandonedConnectionSize = 0;
            }
            LOGGER.info(GT.tr("Close connections execute in cluster: {0}, closed connections: {1}, " +
                    "size of abandonedConnectionList before closing: {2}," +
                    " size of abandonedConnectionList after closing: {3}.",
                urlIdentifier, closed, oldSize, abandonedConnectionList.size()));
        }
        return closed;
    }

    /**
     * Get cached connection size.
     *
     * @return cachedConnectionSize
     */
    public int getCachedConnectionSize() {
        return cachedDnList.values().stream().mapToInt(DataNode::getCachedConnectionListSize).sum();
    }
}
