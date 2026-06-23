/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
