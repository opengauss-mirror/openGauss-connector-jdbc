/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
