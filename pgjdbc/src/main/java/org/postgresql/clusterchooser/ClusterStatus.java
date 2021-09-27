package org.postgresql.clusterchooser;

/**
 * Known state of a cluster.
 */
public enum ClusterStatus {
    Unknown,
    ConnectFail,
    MasterCluster,
    SecondaryCluster
}
