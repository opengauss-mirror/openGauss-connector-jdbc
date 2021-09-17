package org.postgresql.clusterchooser;

import org.postgresql.Driver;
import org.postgresql.PGProperty;
import org.postgresql.QueryCNListUtils;
import org.postgresql.hostchooser.MultiHostChooser;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.ClusterSpec;
import org.postgresql.util.HostSpec;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of HostSpec targets in a global map.
 */
public class GlobalClusterStatusTracker {

    private static final Map<String, ClusterSpecStatus> clusterStatusMap = new HashMap<>();
    private static final Map<String, Boolean> firstConnectionMap = new ConcurrentHashMap<>();
    private static Map<String, String> masterClusterList = new HashMap<>();
    private static Log LOGGER = Logger.getLogger(GlobalClusterStatusTracker.class.getName());

    /**
     * Store the actual observed cluster status.
     *
     * @param clusterSpec   The cluster whose status is known.
     * @param clusterStatus Latest known status for the cluster.
     */
    public static void reportClusterStatus(ClusterSpec clusterSpec, ClusterStatus clusterStatus) {
        String key = keyFromClusterSpec(clusterSpec);
        synchronized (clusterStatusMap) {
            ClusterSpecStatus clusterSpecStatus = clusterStatusMap.get(key);
            if (clusterSpecStatus == null) {
                clusterSpecStatus = new ClusterSpecStatus(clusterSpec);
                clusterStatusMap.put(key, clusterSpecStatus);
            }
            clusterSpecStatus.status = clusterStatus;
        }
    }

    /**
     * The actual primary cluster in the storage cluster.
     *
     * @param props
     * @param clusterSpec
     */
    public static void reportMasterCluster(Properties props, ClusterSpec clusterSpec) {
        String urlKey = QueryCNListUtils.keyFromURL(props);
        String masterClusterKey = keyFromClusterSpec(clusterSpec);
        synchronized (masterClusterList) {
            masterClusterList.put(urlKey, masterClusterKey);
        }
    }

    /**
     * @param hostSpecs
     * @return
     */
    public static ClusterStatus getClusterStatus(HostSpec[] hostSpecs) {
        HostSpec[] cloneHostSpecs = hostSpecs.clone();
        Arrays.sort(cloneHostSpecs);
        String clusterKey = Arrays.toString(cloneHostSpecs);
        synchronized (clusterStatusMap) {
            ClusterSpecStatus clusterSpecStatus = clusterStatusMap.get(clusterKey);
            if (clusterSpecStatus != null && clusterSpecStatus.status != null) {
                return clusterSpecStatus.status;
            }
        }
        return ClusterStatus.Unknown;
    }

    /**
     * Submit the cluster information in props.
     * And try to get the information of the master cluster from the refreshed results.
     *
     * @param props Connection properties.
     */
    public static void refreshProperties(Properties props) {
        String key = QueryCNListUtils.keyFromURL(props);
        // The value is false only when it is obtained for the first time.
        boolean block;
        synchronized (firstConnectionMap) {
            block = firstConnectionMap.getOrDefault(key, false);
            firstConnectionMap.put(key, true);
        }
        String masterClusterkey = getMasterClusterkey(key, block);
        if ("".equals(masterClusterkey)) {
            return;
        }
        LOGGER.info("[PRIORITYSERVERS] Find the main cluster in dual clusters." +
                " | DualCluster: " + key +
                " | MasterCluster:" + masterClusterkey
        );
        // The result is updated to props.
        props.setProperty("MASTERCLUSTER", masterClusterkey);
    }

    /**
     * If block is set to true, waiting will be prevented if the master cluster cannot be found.
     *
     * @param key   Key in the connection string.
     * @param block Concurrency lock.
     * @return
     */
    public static String getMasterClusterkey(String key, boolean block) {
        int intervalWaitHasRefreshedCNList = 10;
        int timesWaitHasRefreshedCNList = 200;
        for (int i = 0; i <= timesWaitHasRefreshedCNList; i++) {
            synchronized (masterClusterList) {
                String masterClusterKey = masterClusterList.get(key);
                if (masterClusterKey != null && !"".equals(masterClusterKey)) {
                    return masterClusterKey;
                }
            }
            if (!block) break;
            try {
                Thread.sleep(intervalWaitHasRefreshedCNList);
            } catch (InterruptedException e) {
                LOGGER.info("[PRIORITYSERVERS] InterruptedException. This caused by: \"Thread.sleep\", waiting for refreshing master cluster from connection.");
            }
        }
        if (block) {
            LOGGER.info("[PRIORITYSERVERS] Blocking time extends 2 seconds need to pay attention.");
        }
        return "";
    }

    /**
     * Determine whether the configuration to support disaster recovery switching is effective.
     * if the disaster tolerance switch function is used, the value of this parameter should be a number,
     * and the value should be greater than 0 and less than the number of nodes in the connection string.
     * <p>
     * Return true if the above conditions are met, otherwise, return false.
     *
     * @param props Connection properties
     * @return
     */
    public static boolean isVaildPriorityServers(Properties props) {
        String priorityServers = PGProperty.PRIORITY_SERVERS.get(props);
        try {
            int priorityServersNumber = Integer.parseInt(priorityServers);
            int lengthPGPORTURL = props.getProperty("PGPORTURL").split(",").length;
            if (lengthPGPORTURL <= priorityServersNumber || priorityServersNumber <= 0) {
                LOGGER.warn("When configuring priority servers, The number of priority nodes should be less than the number of nodes on the URL and greater than 0.");
                return false;
            }
        } catch (NumberFormatException e) {
            LOGGER.warn("When configuring priority servers, \"priorityServers\" should be number.");
            return false;
        }
        return true;
    }

    /**
     * Split the clusters in the url, get the master cluster first when there are dual clusters.
     *
     * @param hostSpecs Specification information of all hosts in the cluster.
     * @param info the parsed/defaulted connection properties
     * @return
     */
    public static Iterator<ClusterSpec> getClusterFromHostSpecs(HostSpec[] hostSpecs, Properties info) {
        String priorityServers = PGProperty.PRIORITY_SERVERS.get(info);
        ClusterSpec[] clusterSpecs;
        if (priorityServers != null) {
            clusterSpecs = new ClusterSpec[2];
            //cluster demarcation index.
            Integer index = Integer.valueOf(priorityServers);
            //known master cluster.
            String masterCluster = info.getProperty("MASTERCLUSTER");
            //When load balancing, use the real queried cluster information.
            if (MultiHostChooser.isUsingAutoLoadBalance(info) && masterCluster != null) {
                clusterSpecs[0] = new ClusterSpec(hostSpecs);
                HostSpec[] urlHostSpecs = Driver.getURLHostSpecs(info);
                HostSpec[] slaveHostSpecs = Arrays.copyOfRange(urlHostSpecs, index, urlHostSpecs.length);
                if(!masterCluster.contains(slaveHostSpecs[0].toString())){
                    clusterSpecs[1] = new ClusterSpec(slaveHostSpecs);
                }else{
                    clusterSpecs[1] = new ClusterSpec(Arrays.copyOfRange(urlHostSpecs, 0, index));
                }

            } else {
                HostSpec[] masterHostSpecs = Arrays.copyOfRange(hostSpecs, 0, index);
                HostSpec[] slaveHostSpecs = Arrays.copyOfRange(hostSpecs, index, hostSpecs.length);
                //Prioritize the known master cluster.
                if (masterCluster != null && masterCluster.contains(slaveHostSpecs[0].toString())) {
                    clusterSpecs[0] = new ClusterSpec(slaveHostSpecs);
                    clusterSpecs[1] = new ClusterSpec(masterHostSpecs);
                } else {
                    clusterSpecs[0] = new ClusterSpec(masterHostSpecs);
                    clusterSpecs[1] = new ClusterSpec(slaveHostSpecs);
                }
            }
        } else {
            clusterSpecs = new ClusterSpec[1];
            clusterSpecs[0] = new ClusterSpec(hostSpecs);
        }
        return new ArrayList<>(Arrays.asList(clusterSpecs)).iterator();
    }

    /**
     * Returns a key representing the cluster based on clusterSpec.
     *
     * @param clusterSpec Nodes in the cluster.
     * @return Key representing the cluster.
     */
    public static String keyFromClusterSpec(ClusterSpec clusterSpec) {
        HostSpec[] hostSpecs = clusterSpec.getHostSpecs();
        Arrays.sort(hostSpecs);
        return Arrays.toString(hostSpecs);
    }

    static class ClusterSpecStatus {
        final ClusterSpec cluster;
        ClusterStatus status;

        ClusterSpecStatus(ClusterSpec cluster) {
            this.cluster = cluster;
        }

        @Override
        public String toString() {
            return cluster.toString() + '=' + status;
        }
    }
}
