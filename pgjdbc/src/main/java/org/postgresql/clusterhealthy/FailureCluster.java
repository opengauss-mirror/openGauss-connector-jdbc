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

import org.postgresql.util.HostSpec;

import java.util.Properties;
import java.util.Set;

/**
 * Cluster information instance, including primary and secondary nodes and connection information
 */
public class FailureCluster {
    private HostSpec master;
    private Set<HostSpec> salves;
    private Set<Properties> props;
    private int frequency;

    /**
     *
     * @param master Current master node
     * @param salves Slave set
     * @param props Connection information
     */
    public FailureCluster(HostSpec master, Set<HostSpec> salves, Set<Properties> props, Integer frequency) {
        this.master = master;
        this.salves = salves;
        this.props = props;
        this.frequency = null == frequency ? 0 : frequency;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public void setSalves(Set<HostSpec> salves) {
        this.salves = salves;
    }

    public void setProps(Set<Properties> props) {
        this.props = props;
    }

    public void setMaster(HostSpec master) {
        this.master = master;
    }

    public HostSpec getMaster() {
        return master;
    }

    public Set<HostSpec> getSalves() {
        return salves;
    }

    public Set<Properties> getProps() {
        return props;
    }

    @Override
    public String toString() {
        return "FailureCluster{" +
                "master=" + master +
                ", salves=" + salves +
                '}';
    }
}
