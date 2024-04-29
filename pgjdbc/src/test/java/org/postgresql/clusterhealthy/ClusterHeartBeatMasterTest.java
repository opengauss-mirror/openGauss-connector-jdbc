
package org.postgresql.clusterhealthy;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.test.TestUtil;
import org.postgresql.util.HostSpec;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.postgresql.clusterhealthy.ClusterHeartBeatUtil.getHostSpecs;

// TODO 后续修复
@Ignore
public class ClusterHeartBeatMasterTest {
    @Before
    public void initDirver() throws Exception {
        TestUtil.initDriver();
    }

    @Test
    public void runTest() {
        ClusterHeartBeatMaster clusterHeartBeatMaster = ClusterHeartBeatMaster.getInstance();
        Map<HostSpec, Set<HostSpec>> clusterRelationship = clusterHeartBeatMaster.getClusterRelationship();
        List<HostSpec> hostSpecs = getHostSpecs();
        HostSpec master = hostSpecs.get(0);
        hostSpecs.remove(master);
        clusterRelationship.put(master, new HashSet<>(hostSpecs));
        HashSet<Properties> set = new HashSet<>();
        set.add(ClusterHeartBeatUtil.getProperties(getHostSpecs()));
        clusterHeartBeatMaster.addProperties(master, set);
        clusterHeartBeatMaster.run();
        Assert.assertTrue(clusterRelationship.containsKey(master));
    }

}
