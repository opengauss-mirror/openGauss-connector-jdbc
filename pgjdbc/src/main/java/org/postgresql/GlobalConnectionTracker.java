package org.postgresql;

import org.postgresql.clusterhealthy.ClusterNodeCache;
import org.postgresql.core.QueryExecutor;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Keeps track of connection reference and host spec in a global map.
 */
public class GlobalConnectionTracker {

    private static final Map<String, HashMap<Integer, QueryExecutor>> connectionManager = new HashMap<>();
    private static Log LOGGER = Logger.getLogger(GlobalConnectionTracker.class.getName());

    /**
     *
     * @param props the parsed/defaulted connection properties
     * @return
     */
    private static boolean isForceTargetServerSlave(Properties props){
        return PGProperty.FORCE_TARGET_SERVER_SLAVE.getBoolean(props) &&
                ("slave".equals(PGProperty.TARGET_SERVER_TYPE.get(props)) || "secondary".equals(PGProperty.TARGET_SERVER_TYPE.get(props)));
    }

    /**
     *
     * @param props the parsed/defaulted connection properties
     * @return
     */
    private static boolean isTargetServerMaster(Properties props) {
        return "master".equals(PGProperty.TARGET_SERVER_TYPE.get(props));
    }

    /**
     * Store the actual query executor and connection host spec.
     *
     * @param props the parsed/defaulted connection properties
     * @param queryExecutor
     */
    public static void possessConnectionReference(QueryExecutor queryExecutor, Properties props) {
        if (!isForceTargetServerSlave(props) && !isTargetServerMaster(props)) {
            return;
        }
        int identityQueryExecute = System.identityHashCode(queryExecutor);
        String hostSpec = queryExecutor.getHostSpec().toString();
        synchronized (connectionManager) {
            HashMap<Integer, QueryExecutor> hostConnection = connectionManager.getOrDefault(hostSpec, null);
            if (hostConnection == null) {
                hostConnection = new HashMap<>();
            }
            hostConnection.put(identityQueryExecute, queryExecutor);
            connectionManager.put(hostSpec, hostConnection);
        }
        if (isTargetServerMaster(props)) {
            HostSpec[] hostSpecs = Driver.GetHostSpecs(props);
            ClusterNodeCache.pushHostSpecs(queryExecutor.getHostSpec(), hostSpecs, props);
        }
    }

    /**
     * Clear the reference when the connection is closed.
     *
     * @param props the parsed/defaulted connection properties
     * @param queryExecutor
     */
    public static void releaseConnectionReference(QueryExecutor queryExecutor, Properties props) {
        if (!isForceTargetServerSlave(props) && !isTargetServerMaster(props)) {
            return;
        }
        String hostSpec = queryExecutor.getHostSpec().toString();
        int identityQueryExecute = System.identityHashCode(queryExecutor);
        synchronized (connectionManager) {
            HashMap<Integer, QueryExecutor> hostConnection = connectionManager.getOrDefault(hostSpec, null);
            if (hostConnection != null) {
                if (hostConnection.get(identityQueryExecute) != null) {
                    hostConnection.put(identityQueryExecute, null);
                } else {
                    LOGGER.info("[SWITCHOVER] The identity of the queryExecutor has changed!");
                }
                ClusterNodeCache.updateDetection();
            } else {
                LOGGER.info("[SWITCHOVER] No connection found under this host!");
            }
        }
    }

    /**
     * Close all existing connections under the specified host.
     *
     * @param props the parsed/defaulted connection properties
     * @param hostSpec ip and port.
     */
    public static void closeOldConnection(String hostSpec, Properties props) {
        if(!isForceTargetServerSlave(props)) return;
        synchronized (connectionManager) {
            HashMap<Integer, QueryExecutor> hostConnection = connectionManager.getOrDefault(hostSpec, null);
            if (hostConnection != null) {
                LOGGER.info("[SWITCHOVER] The hostSpec: " + hostSpec + " status from slave to master, start to close the original connection.");
                for (QueryExecutor queryExecutor : hostConnection.values()) {
                    if (queryExecutor != null && !queryExecutor.isClosed()) {
                        queryExecutor.setAvailability(false);
                    }
                }
                hostConnection.clear();
                LOGGER.info("[SWITCHOVER] The hostSpec: " + hostSpec + " status from slave to master, end to close the original connection.");
            }
        }
    }

    /**
     * Close all existing connections under the specified host.
     *
     * @param hostSpec ip and port.
     */
    public static void closeConnectionOfCrash(String hostSpec) {
        synchronized (connectionManager) {
            HashMap<Integer, QueryExecutor> hostConnection = connectionManager.getOrDefault(hostSpec, null);
            if (hostConnection != null && !hostConnection.isEmpty()) {
                LOGGER.debug("[CRASH] The hostSpec: " + hostSpec + " fails, start to close the original connection.");
                for (QueryExecutor queryExecutor : hostConnection.values()) {
                    if (queryExecutor != null && !queryExecutor.isClosed()) {
                        queryExecutor.close();
                        queryExecutor.setAvailability(false);
                    }
                }
                hostConnection.clear();
                LOGGER.debug("[CRASH] The hostSpec: " + hostSpec + " fails, end to close the original connection.");
            }
        }
    }

    /**
     * get all existing connections under the specified host.
     *
     * @param hostSpec ip and port.
     */
    public static List<QueryExecutor> getConnections(String hostSpec) {
        synchronized (connectionManager) {
            List<QueryExecutor> ret = new ArrayList<>();
            HashMap<Integer, QueryExecutor> hostConnection = connectionManager.getOrDefault(hostSpec, null);
            if (hostConnection != null) {
                for (Map.Entry<Integer, QueryExecutor> queryExecutorEntry : hostConnection.entrySet()) {
                    ret.add(queryExecutorEntry.getValue());
                }
            }
            return ret;
        }
    }

    public static boolean hasConnection() {
        synchronized (connectionManager) {
            for (HashMap<Integer, QueryExecutor> queryExecutorMap : connectionManager.values()) {
                boolean exist = queryExecutorMap.entrySet().stream()
                        .anyMatch((entry) -> entry.getValue() != null && !entry.getValue().isClosed());
                if (exist) {
                    return exist;
                }
            }
        }
        return false;
    }



}