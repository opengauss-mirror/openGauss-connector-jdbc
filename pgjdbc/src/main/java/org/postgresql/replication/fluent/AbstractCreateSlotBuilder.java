/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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

package org.postgresql.replication.fluent;

import java.util.regex.Pattern;

/**
 * Provides common state and validation helpers for replication slot creation builders.
 *
 * @since 2020-06-30
 */
public abstract class AbstractCreateSlotBuilder<T extends ChainedCommonCreateSlotBuilder<T>>
    implements ChainedCommonCreateSlotBuilder<T> {
    /** Pattern for replication slot names. */
    protected static final Pattern REPLICATION_SLOT_NAME = Pattern.compile("[a-z0-9_?.-]+");

    /** Pattern for logical replication output plugin names. */
    protected static final Pattern OUTPUT_PLUGIN_NAME = Pattern.compile("[a-z0-9_.-]+");

    /** Replication slot name configured by the builder. */
    protected String slotName;

    /**
     * Returns this builder instance with the concrete chained builder type.
     *
     * @return this builder instance
     */
    protected abstract T self();

    /**
     * Checks whether a replication command identifier matches the allowed token pattern.
     *
     * @param value identifier value to validate
     * @param pattern allowed token pattern for the specific identifier type
     * @return true if the identifier is safe for the replication command, false otherwise
     */
    protected static boolean isValidReplicationIdentifier(String value, Pattern pattern) {
        return value != null
            && !value.isEmpty()
            && !".".equals(value)
            && !"..".equals(value)
            && pattern.matcher(value).matches()
            && !value.contains("--");
    }

    @Override
    public T withSlotName(String slotName) {
        this.slotName = slotName;
        return self();
    }
}
