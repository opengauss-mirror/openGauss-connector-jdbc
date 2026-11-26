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

import org.postgresql.core.ORField;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.List;

/**
 * cursor ResultSet
 *
 * @author zhangting
 * @since  2025-09-29
 */
public class ORCursorResultSet extends ORResultSet {
    private int cursorMode;
    private boolean isFetched;

    /**
     * ORCursorResultSet constructor
     *
     * @param orStatement orStatement
     * @param cursorMode cursorMode
     * @throws SQLException if a database access error occurs
     */
    public ORCursorResultSet(ORStatement orStatement, int cursorMode) throws SQLException {
        super(orStatement);
        this.isFetched = false;
        this.cursorMode = cursorMode;
    }

    /**
     * get statement
     *
     * @return statement
     */
    public ORStatement getStatement() {
        return statement;
    }

    /**
     * is fetched
     *
     * @return isFetched
     */
    public boolean isFetched() {
        return isFetched;
    }

    /**
     * set isFetched
     *
     * @param isFetched is fetched
     */
    public void setFetched(boolean isFetched) {
        this.isFetched = isFetched;
    }

    /**
     * get cursorMode
     *
     * @return cursorMode
     */
    public int getCursorMode() {
        return cursorMode;
    }

    /**
     * set cursor fetch result
     *
     * @param total total rows
     * @param orFields fields
     * @param valueLens value length
     * @param dataRows data rows
     * @param hasRemain has remain
     */
    public void setCursorFetchInfo(int total, ORField[] orFields, List<int[]> valueLens,
                                   List<byte[][]> dataRows, boolean hasRemain) {
        this.orFields = orFields;
        setFetchInfo(total, valueLens, dataRows, hasRemain);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        boolean hasNext = this.hasNext();
        if (hasRemain && !hasNext) {
            try {
                this.statement.connection.getQueryExecutor().fetchCursor(this);
            } catch (SQLException e) {
                throw new PSQLException("fetch more rows failed.", PSQLState.IO_ERROR);
            }
            hasNext = hasNext();
        }
        return hasNext;
    }
}
