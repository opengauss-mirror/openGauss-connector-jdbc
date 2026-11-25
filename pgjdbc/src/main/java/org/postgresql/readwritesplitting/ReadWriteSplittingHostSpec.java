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

package org.postgresql.readwritesplitting;

import org.postgresql.hostchooser.HostChooser;
import org.postgresql.hostchooser.HostChooserFactory;
import org.postgresql.hostchooser.HostRequirement;
import org.postgresql.util.HostSpec;

import java.util.Properties;

/**
 * Read write splitting host spec.
 *
 * @since 2023-11-20
 */
public class ReadWriteSplittingHostSpec {
    private final HostSpec writeHostSpec;

    private final HostSpec[] readHostSpecs;

    private final HostRequirement targetServerType;

    private final HostChooser readChooser;

    /**
     * Constructor.
     *
     * @param writeHostSpec write host spec
     * @param hostSpecs host specs
     * @param targetServerType target server type
     * @param props props
     */
    public ReadWriteSplittingHostSpec(HostSpec writeHostSpec, HostSpec[] hostSpecs, HostRequirement targetServerType,
                                      Properties props) {
        this.writeHostSpec = writeHostSpec;
        this.readHostSpecs = createReadHostSpecs(hostSpecs, writeHostSpec);
        this.targetServerType = targetServerType;
        readChooser = HostChooserFactory.createHostChooser(readHostSpecs, targetServerType, props);
    }

    private HostSpec[] createReadHostSpecs(HostSpec[] hostSpecs, HostSpec writeHostSpec) {
        int index = 0;
        HostSpec[] result = new HostSpec[hostSpecs.length - 1];
        for (HostSpec each : hostSpecs) {
            if (!each.equals(writeHostSpec)) {
                result[index++] = each;
            }
        }
        return result;
    }

    /**
     * Get write host spec.
     *
     * @return write host spec
     */
    public HostSpec getWriteHostSpec() {
        return writeHostSpec;
    }

    /**
     * Get read host specs.
     *
     * @return read host specs
     */
    public HostSpec[] getReadHostSpecs() {
        return readHostSpecs;
    }

    /**
     * Get target server type.
     *
     * @return target server type
     */
    public HostRequirement getTargetServerType() {
        return targetServerType;
    }

    /**
     * Read load balance.
     *
     * @return routed host spec
     */
    public HostSpec readLoadBalance() {
        return readChooser.iterator().next().hostSpec;
    }
}
