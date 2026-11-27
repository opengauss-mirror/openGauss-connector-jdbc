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
import java.util.Properties;

import static org.postgresql.clusterhealthy.ClusterHeartBeatUtil.getHostSpecs;

// TODO 后续修复
@Ignore
public class ClusterHeartBeatFailureMasterTest {
    @Before
    public void initDirver() throws Exception {
        TestUtil.initDriver();
    }

    @Test
    public void runTest() {
        ClusterHeartBeatFailureMaster instance = ClusterHeartBeatFailureMaster.getInstance();
        List<HostSpec> hostSpecs = getHostSpecs();
        instance.addFailureMaster(hostSpecs.get(0), hostSpecs.get(1));
        HashSet<Properties> set = new HashSet<>();
        set.add(ClusterHeartBeatUtil.getProperties(getHostSpecs()));
        instance.addProperties(hostSpecs.get(0), set);
        instance.addProperties(hostSpecs.get(1), set);
        Assert.assertTrue(instance.getFailureMaster().size() != 0);
        instance.run();
        System.out.println(instance.getFailureMaster());
        Assert.assertTrue(instance.getFailureMaster().size() == 0);
    }
}
