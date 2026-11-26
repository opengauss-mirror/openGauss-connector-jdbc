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

package org.postgresql.jdbc;

import org.postgresql.core.Utils;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * Savepoint
 *
 * @author zhangting
 * @since  2026-06-23
 */
public class ORSavepoint implements Savepoint {
    private int id = -1;
    private String name = null;
    private boolean isValid;

    /**
     * ORSavepoint constructor
     *
     * @param id savepointId
     */
    ORSavepoint(int id) {
        this.isValid = true;
        this.id = id;
    }

    /**
     * ORSavepoint constructor
     *
     * @param name savepointName
     */
    ORSavepoint(String name) {
        this.isValid = true;
        this.name = name;
    }

    /**
     * release savepoint
     */
    public void release() {
        this.isValid = false;
    }

    /**
     * get savepoint name
     *
     * @return savepoint name
     * @throws SQLException if a database access error occurs
     */
    public String getSavepointName() throws SQLException {
        if (!isValid) {
            throw new PSQLException(GT.tr("Savepoint is invalid, it has been released."),
                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
        }
        if (name == null) {
            return name;
        }
        return Utils.escapeIdentifier(null, name).toString();
    }

    /**
     * get savepoint id
     *
     * @return savepoint id
     * @throws SQLException if a database access error occurs
     */
    public int getSavepointId() throws SQLException {
        if (!isValid) {
            throw new PSQLException(GT.tr("Savepoint is invalid, it has been released."),
                    PSQLState.INVALID_SAVEPOINT_SPECIFICATION);
        }
        return this.id;
    }
}
