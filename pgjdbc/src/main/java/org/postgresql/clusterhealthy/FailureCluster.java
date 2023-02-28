/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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

    /**
     *
     * @param master Current master node
     * @param salves Slave set
     * @param props Connection information
     */
    public FailureCluster(HostSpec master, Set<HostSpec> salves, Set<Properties> props) {
        this.master = master;
        this.salves = salves;
        this.props = props;
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
}
