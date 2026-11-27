/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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
