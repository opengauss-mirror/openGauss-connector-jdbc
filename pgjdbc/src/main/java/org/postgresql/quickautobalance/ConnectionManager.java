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
import org.postgresql.QueryCNListUtils;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.StatementCancelState;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connection manager of quick load balancing.
 */
public class ConnectionManager {
    private static final Log LOGGER = Logger.getLogger(ConnectionManager.class.getName());

    private static final String AUTO_BALANCE = "autoBalance";

    private static final String LEAST_CONN = "leastconn";

    private final Map<String, Cluster> cachedClusters;

    private ConnectionManager() {
        this.cachedClusters = new ConcurrentHashMap<>();
    }

    /**
     * Choose abandoned connections from abandonedConnections of each cluster.
     * The number of connections that each cluster closes : CLOSE_CONNECTION_PERIOD * CLOSE_CONNECTION_PER_SECOND.
     *
     * @return the number of connections closed per cluster
     */
    public List<Integer> closeConnections() {
        List<Integer> ans = new ArrayList<>();
        for (Entry<String, Cluster> entry : cachedClusters.entrySet()) {
            Cluster cluster = entry.getValue();
            int num = 0;
            num += cluster != null ? cluster.closeConnections() : 0;
            ans.add(num);
        }
        return ans;
    }

    /**
     * check whether the properties enable leastconn.
     *
     * @param properties properties
     * @return whether the properties enable leastconn.
     */
    public static boolean checkEnableLeastConn(Properties properties) {
        if (properties == null) {
            return false;
        }
        if (!LEAST_CONN.equals(properties.getProperty(AUTO_BALANCE, ""))) {
            return false;
        }
        HostSpec[] hostSpecs = Driver.getURLHostSpecs(properties);
        return hostSpecs.length > 1;
    }

    /**
     * Check a properties if enable quickAutoBalance.
     *
     * @param properties properties
     * @return if enable quickAutoBalance
     */
    public static boolean checkEnableQuickAutoBalance(Properties properties) {
        if (!checkEnableLeastConn(properties)) {
            return false;
        }
        return ConnectionInfo.ENABLE_QUICK_AUTO_BALANCE_PARAMS
            .equals(PGProperty.ENABLE_QUICK_AUTO_BALANCE.get(properties));
    }

    private static class Holder {
        private static final ConnectionManager INSTANCE = new ConnectionManager();
    }

    /**
     * Get instance.
     *
     * @return connectionManager
     */
    public static ConnectionManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Set cluster into connection manager.
     *
     * @param properties properties
     * @return set or not.
     */
    public boolean setCluster(Properties properties) throws PSQLException {
        if (!checkEnableLeastConn(properties)) {
            return false;
        }
        checkQuickAutoBalanceParams(properties);
        String urlIdentifier = QueryCNListUtils.keyFromURL(properties);
        // create a cluster if it doesn't exist in cachedClusters.
        if (!cachedClusters.containsKey(urlIdentifier)) {
            synchronized (cachedClusters) {
                if (!cachedClusters.containsKey(urlIdentifier)) {
                    Cluster cluster = new Cluster(urlIdentifier, properties);
                    cachedClusters.put(urlIdentifier, cluster);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    private void checkQuickAutoBalanceParams(Properties properties) throws PSQLException {
        ConnectionInfo.parseEnableQuickAutoBalance(properties);
        ConnectionInfo.parseMaxIdleTimeBeforeTerminal(properties);
        Cluster.parseMinReservedConPerCluster(properties);
        Cluster.parseMinReservedConPerDatanode(properties);
    }

    /**
     * Cache this connection, if it's configured with autoBalance = "leastconn".
     *
     * @param pgConnection connection
     * @param properties properties
     * @return if insert the connection into connectionManager.
     */
    public boolean setConnection(PgConnection pgConnection, Properties properties) throws PSQLException {
        if (!checkEnableLeastConn(properties)) {
            return false;
        }
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        // create a cluster if it doesn't exist in cachedClusters.
        if (!cachedClusters.containsKey(URLIdentifier)) {
            synchronized (cachedClusters) {
                if (!cachedClusters.containsKey(URLIdentifier)) {
                    Cluster cluster = new Cluster(URLIdentifier, properties);
                    cluster.setConnection(pgConnection, properties);
                    cachedClusters.put(URLIdentifier, cluster);
                }
            }
        } else {
            cachedClusters.get(URLIdentifier).setConnection(pgConnection, properties);
        }
        return true;
    }

    /**
     * Set connection state of cached connections if it exists.
     *
     * @param pgConnection pgConnection
     * @param state state
     */
    public void setConnectionState(PgConnection pgConnection, StatementCancelState state) throws PSQLException {
        String url;
        try {
            url = pgConnection.getURL();
        } catch (SQLException e) {
            LOGGER.error(GT.tr("Can't get url from pgConnection."));
            return;
        }
        String URLIdentifier = getURLIdentifierFromUrl(url);
        Cluster cluster = cachedClusters.get(URLIdentifier);
        if (cluster != null) {
            cluster.setConnectionState(pgConnection, state);
        }
    }

    public Cluster getCluster(String URLIdentifier) {
        return cachedClusters.get(URLIdentifier);
    }

    /**
     * Get URLIdentifier from url, which is a unique id of cluster.
     *
     * @param url url
     * @return URLIdentifier
     */
    public static String getURLIdentifierFromUrl(String url) throws PSQLException {
        HostSpec[] hostSpecs;
        try {
            String pgHostUrl = url.split("//")[1].split("/")[0];
            String[] pgHosts = pgHostUrl.split(",");
            hostSpecs = new HostSpec[pgHosts.length];
            for (int i = 0; i < hostSpecs.length; i++) {
                hostSpecs[i] = new HostSpec(pgHosts[i].split(":")[0],
                    Integer.parseInt(pgHosts[i].split(":")[1]));
            }
            Arrays.sort(hostSpecs);

        } catch (ArrayIndexOutOfBoundsException e) {
            throw new PSQLException(
                GT.tr("Parsed url={0} failed.", url), PSQLState.INVALID_PARAMETER_VALUE);
        }
        return Arrays.toString(hostSpecs);
    }

    /**
     * Check whether the connections are valid in each cluster, and remove invalid connections.
     *
     * @return the number of connections removed from cache.
     */
    public List<Integer> checkConnectionsValidity() {
        List<Integer> ans = new ArrayList<>();
        for (Entry<String, Cluster> entry : cachedClusters.entrySet()) {
            Cluster cluster = entry.getValue();
            int num = 0;
            if (cluster != null) {
                List<Integer> removes = cluster.checkConnectionsValidity();
                for (int remove : removes) {
                    num += remove;
                }
            }
            ans.add(num);
        }
        return ans;
    }

    /**
     * Check datanode states of each cluster.
     *
     * @return the number of invalid data nodes of all cluster
     */
    public int checkClusterStates() {
        int invalidDataNodes = 0;
        for (Entry<String, Cluster> entry : cachedClusters.entrySet()) {
            Cluster cluster = entry.getValue();
            if (cluster != null) {
                invalidDataNodes += cluster.checkClusterState();
            }
        }
        return invalidDataNodes;
    }

    /**
     * Increment cachedCreatingConnectionSize.
     * CachedCreatingConnectionSize indicates the number of connections that have been load balanced,
     * but haven't been cached into cachedConnectionList yet.
     *
     * @param hostSpec hostSpec
     * @param properties properties
     * @return cachedCreatingConnectionSize after updating
     */
    public int incrementCachedCreatingConnectionSize(HostSpec hostSpec, Properties properties) {
        if (!checkEnableLeastConn(properties)) {
            return 0;
        }
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        if (cachedClusters.containsKey(URLIdentifier)) {
            Cluster cluster = cachedClusters.get(URLIdentifier);
            if (cluster != null) {
                return cluster.incrementCachedCreatingConnectionSize(hostSpec);
            }
        } else {
            LOGGER.info(GT.tr("Can not find cluster: {0} in cached clusters.", URLIdentifier));
        }
        return 0;
    }

    /**
     * Decrement cachedCreatingConnectionSize.
     * CachedCreatingConnectionSize indicates the number of connections that have been load balanced,
     * but haven't been cached into cachedConnectionList yet.
     *
     * @param hostSpec hostSpec
     * @param properties properties
     * @return cachedCreatingConnectionSize after updating
     */
    public int decrementCachedCreatingConnectionSize(HostSpec hostSpec, Properties properties) {
        if (!checkEnableLeastConn(properties)) {
            return 0;
        }
        String URLIdentifier = QueryCNListUtils.keyFromURL(properties);
        if (cachedClusters.containsKey(URLIdentifier)) {
            Cluster cluster = cachedClusters.get(URLIdentifier);
            if (cluster != null) {
                return cluster.decrementCachedCreatingConnectionSize(hostSpec);
            }
        } else {
            LOGGER.info(GT.tr("Can not find cluster: {0} in cached clusters.", URLIdentifier));
        }
        return 0;
    }

    /**
     * Clear connection manager.
     */
    public void clear() {
        synchronized (cachedClusters) {
            cachedClusters.clear();
        }
    }

    /**
     * Get cached connection size.
     *
     * @return cached connection size
     */
    public int getCachedConnectionSize() {
        return cachedClusters.values().stream().mapToInt(Cluster::getCachedConnectionSize).sum();
    }
}