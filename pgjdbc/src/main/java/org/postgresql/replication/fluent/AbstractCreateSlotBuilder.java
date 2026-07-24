/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
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
