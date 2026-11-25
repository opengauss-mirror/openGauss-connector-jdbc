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

package org.postgresql.replication.fluent.physical;

import org.postgresql.core.BaseConnection;
import org.postgresql.replication.fluent.AbstractCreateSlotBuilder;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Creates a physical replication slot by issuing the replication slot command.
 *
 * @since 2020-06-30
 */
public class PhysicalCreateSlotBuilder
    extends AbstractCreateSlotBuilder<ChainedPhysicalCreateSlotBuilder>
    implements ChainedPhysicalCreateSlotBuilder {
    private BaseConnection connection;

    public PhysicalCreateSlotBuilder(BaseConnection connection) {
        this.connection = connection;
    }

    @Override
    protected ChainedPhysicalCreateSlotBuilder self() {
        return this;
    }

    @Override
    public void make() throws SQLException {
        // Reject characters that can split or alter the replication command syntax.
        if (!isValidReplicationIdentifier(slotName, REPLICATION_SLOT_NAME)) {
            throw new IllegalArgumentException(
                "Invalid replication slot name: must not be null or empty and must be valid");
        }

        Statement statement = connection.createStatement();
        try {
            statement.execute(String.format("CREATE_REPLICATION_SLOT %s PHYSICAL", slotName));
        } finally {
            statement.close();
        }
    }
}
