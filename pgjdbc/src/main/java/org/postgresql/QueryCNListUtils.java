package org.postgresql;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.SetupQueryRunner;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static java.lang.System.currentTimeMillis;


public class QueryCNListUtils {
    private static Map<String, CNList> clusterCNList = new HashMap<>();
    private static Map<String, Boolean> firstConnectMap = new ConcurrentHashMap<>();

    private static Log LOGGER = Logger.getLogger(QueryCNListUtils.class.getName());

    static class CNList {
        HostSpec[] list;
        long lastUpdated;

        CNList(HostSpec[] cnList) {
            this.list = cnList;
        }
    }

    private static void setLOGGER() {
        // Same as before reconstruction
        if(!Logger.isUsingJDKLogger()) {
            LOGGER = Logger.getLogger(QueryCNListUtils.class.getName());
        }
    }

    // If block is set to true, the waiting is blocked if no proper cluster is found.
    private static HostSpec[] getCNList(String key, Properties properties, boolean block) {
        int intervalWaitHasRefreshedCNList = 10;
        int timesWaitHasRefreshedCNList = 200;
        CNList coordinationNodeList;
        for (int i = 0; i <= timesWaitHasRefreshedCNList; i++) {
            synchronized (clusterCNList) {
                coordinationNodeList = clusterCNList.get(key);
                if (coordinationNodeList != null && coordinationNodeList.list != null) {
                    return coordinationNodeList.list;
                }
            }
            if (!block) break;
            try {
                Thread.sleep(intervalWaitHasRefreshedCNList);
            } catch (InterruptedException e) {
                LOGGER.info("InterruptedException. This caused by: \"Thread.sleep\", waiting for refreshing CN List from connection.");
            }
        }
        if(block){
            LOGGER.info("Blocking time exceeds 2 seconds need to pay attention.");
        }
        return Driver.GetHostSpecs(properties);
    }

    /**
     * Generate a key representing the cluster based on PGHOSTURL and PGPORTURL in props.
     *
     * @param props : Connection properties
     * @return Key representing the cluster
     */
    public static String keyFromURL(Properties props) {
        HostSpec[] hostSpecs = Driver.getURLHostSpecs(props);
        Arrays.sort(hostSpecs);
        return Arrays.toString(hostSpecs);
    }

    /**
     * Submit the cluster information in props to the polling thread.
     * and attempts to obtain the CN list of the cluster from the result output by the polling thread.
     *
     * @param props : Connection properties
     */
    public static void refreshProperties(Properties props) {
        setLOGGER();
        String key = keyFromURL(props);
        // The value is false only when it is obtained for the first time.
        boolean block;
        synchronized (firstConnectMap) {
            block = firstConnectMap.getOrDefault(key,false);
            firstConnectMap.put(key,true);
        }

        HostSpec[] coordinatorNodeList = getCNList(key, props, block);
        if (coordinatorNodeList.length == 0) {
            return;
        }

        LOGGER.info("[AUTOBALANCE] The cluster obtains CNList from the user thread." +
                " | Cluster: " + key +
                " | CNList: " + Arrays.toString(coordinatorNodeList)
        );
        // The result is updated to props.
        StringBuilder hosts = new StringBuilder();
        StringBuilder ports = new StringBuilder();
        for (HostSpec coordinatorNode : coordinatorNodeList) {
            hosts.append(coordinatorNode.getHost() + ',');
            ports.append(String.valueOf(coordinatorNode.getPort()) + ',');
        }
        props.setProperty("PGHOST", hosts.substring(0, hosts.length() - 1));
        props.setProperty("PGPORT", ports.substring(0, ports.length() - 1));
    }

    private static long getTimeToRefrshCNList(Properties props) {
        String refreshCNIpListTime = props.getProperty("refreshCNIpListTime", "10");
        Pattern pattern = Pattern.compile("[0-9]+");
        /* time(ms) * 1000 and longest time is 9999 */
        if (refreshCNIpListTime != null && pattern.matcher(refreshCNIpListTime).matches()
                && !refreshCNIpListTime.startsWith("0") && refreshCNIpListTime.length() < 5) {
            return (long) Integer.parseInt(refreshCNIpListTime) * 1000;
        } else {
            return 10 * 1000;
        }
    }

    /**
     * Refresh the global CN list using the established connections at the upper layer.
     *
     *
     * @param queryExecutor : QueryExecutor that has been connected
     * @param info : Connection properties
     */
    public static void runRereshCNListQueryies(QueryExecutor queryExecutor, Properties info) throws SQLException, IOException {
        String cluster = keyFromURL(info);
        long latestAllowedUpdate = currentTimeMillis() - getTimeToRefrshCNList(info);
        CNList cnList;
        long checkLatestUpdate;
        synchronized (clusterCNList) {
            cnList = clusterCNList.get(cluster);
            if (cnList == null) {
                cnList = new CNList(null);
                clusterCNList.put(cluster, cnList);
            }
            if (cnList.lastUpdated > latestAllowedUpdate) return;
            cnList.lastUpdated = currentTimeMillis();
            checkLatestUpdate = cnList.lastUpdated;
        }
        ArrayList<HostSpec> cnListRefreshed = new ArrayList<>();
        Boolean usingEip = PGProperty.USING_EIP.getBoolean(info);
        String query;
        if (usingEip) {
            query = "select node_host1,node_port1 from pgxc_node where node_type='C' and nodeis_active = true order by node_host1;";
        } else {
            query = "select node_host,node_port from pgxc_node where node_type='C' and nodeis_active = true order by node_host;";
        }
        List<byte[][]> results = SetupQueryRunner.runForList(queryExecutor, query, true);
        for (byte[][] result : results) {
            String host = queryExecutor.getEncoding().decode(result[0]);
            String port = queryExecutor.getEncoding().decode(result[1]);
            cnListRefreshed.add(new HostSpec(host, Integer.parseInt(port)));
        }
        LOGGER.info("[AUTOBALANCE] Try to refreshing CN list, the cluster: " + cnListRefreshed + " connect To: " +
                queryExecutor.getHostSpec());
        synchronized (clusterCNList) {
            // When the query time is greater than the refresh time and less than the timeout time, avoid slow queries to cover fast queries
            if(cnList.lastUpdated > checkLatestUpdate) return;
            cnList.list = cnListRefreshed.toArray(new HostSpec[0]);
            LOGGER.info("[AUTOBALANCE] For refreshing CN list, the cluster: " + Arrays.toString(cnList.list) + " connect To: " +
                    queryExecutor.getHostSpec());
            cnList.lastUpdated = currentTimeMillis();
        }
    }
}
