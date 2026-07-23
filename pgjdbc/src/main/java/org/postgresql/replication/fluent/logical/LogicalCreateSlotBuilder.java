/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.replication.fluent.logical;

import org.postgresql.core.BaseConnection;
import org.postgresql.replication.fluent.AbstractCreateSlotBuilder;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates a logical replication slot with the configured output plugin.
 *
 * @since 2020-06-30
 */
public class LogicalCreateSlotBuilder
    extends AbstractCreateSlotBuilder<ChainedLogicalCreateSlotBuilder>
    implements ChainedLogicalCreateSlotBuilder {
    private String outputPlugin;
    private BaseConnection connection;

    public LogicalCreateSlotBuilder(BaseConnection connection) {
        this.connection = connection;
    }

    @Override
    protected ChainedLogicalCreateSlotBuilder self() {
        return this;
    }

    @Override
    public ChainedLogicalCreateSlotBuilder withOutputPlugin(String outputPlugin) {
        this.outputPlugin = outputPlugin;
        return self();
    }

    @Override
    public void make() throws SQLException {
        // Reject characters that can split or alter the replication command syntax.
        if (!isValidReplicationIdentifier(slotName, REPLICATION_SLOT_NAME)) {
            throw new IllegalArgumentException(
                "Invalid replication slot name: must not be null or empty and must be valid");
        }

        if (!isValidReplicationIdentifier(outputPlugin, OUTPUT_PLUGIN_NAME)) {
            throw new IllegalArgumentException(
                "Invalid output plugin name: must not be null or empty and must be valid");
        }

        Statement statement = connection.createStatement();
        try {
            statement.execute(String.format("CREATE_REPLICATION_SLOT %s LOGICAL %s", slotName, outputPlugin));
        } finally {
            statement.close();
        }
    }
}
