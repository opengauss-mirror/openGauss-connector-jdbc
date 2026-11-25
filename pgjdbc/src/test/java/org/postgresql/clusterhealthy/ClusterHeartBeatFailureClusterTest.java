/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
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
import static org.postgresql.clusterhealthy.ClusterHeartBeatUtil.getProperties;

// TODO 后续修复
@Ignore
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
