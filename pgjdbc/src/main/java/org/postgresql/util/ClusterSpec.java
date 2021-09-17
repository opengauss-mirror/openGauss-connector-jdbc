package org.postgresql.util;

import java.util.Arrays;

public class ClusterSpec implements Comparable {
    protected final HostSpec[] hostSpecs;

    public ClusterSpec(HostSpec[] hostSpecs) {
        this.hostSpecs = hostSpecs.clone();
    }

    public HostSpec[] getHostSpecs() {
        return hostSpecs.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterSpec that = (ClusterSpec) o;
        return Arrays.equals(hostSpecs, that.hostSpecs);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(hostSpecs);
    }

    @Override
    public int compareTo(Object o) {
        return this.toString().compareTo(o.toString());
    }
}