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
