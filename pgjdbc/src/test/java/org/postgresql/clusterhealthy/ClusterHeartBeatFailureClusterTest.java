package org.postgresql.clusterhealthy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.util.HostSpec;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.postgresql.clusterhealthy.ClusterHeartBeatUtil.getHostSpecs;
import static org.postgresql.clusterhealthy.ClusterHeartBeatUtil.getProperties;

public class ClusterHeartBeatFailureClusterTest {

    @Before
    public void initDirver() throws Exception {
        TestUtil.initDriver();
    }

    @Test
    public void runTest() {
        ClusterHeartBeatFailureCluster clusterHeartBeatFailureCluster = ClusterHeartBeatFailureCluster.getInstance();
        List<HostSpec> hostSpecs = getHostSpecs();
        HostSpec master = hostSpecs.get(0);
        hostSpecs.remove(master);
        HashSet<Properties> set = new HashSet<>();
        set.add(ClusterHeartBeatUtil.getProperties(getHostSpecs()));
        FailureCluster failureCluster = new FailureCluster(master, new HashSet<>(hostSpecs), set, 0);
        clusterHeartBeatFailureCluster.addFailureCluster(failureCluster);
        System.out.println(clusterHeartBeatFailureCluster.getFailureCluster());
        clusterHeartBeatFailureCluster.run();
        System.out.println(clusterHeartBeatFailureCluster.getFailureCluster());
        Assert.assertTrue(clusterHeartBeatFailureCluster.getFailureCluster().size() == 0);
    }
}
