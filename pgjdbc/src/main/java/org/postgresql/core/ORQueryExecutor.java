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

package org.postgresql.core;

import org.postgresql.jdbc.ORCursorResultSet;
import org.postgresql.jdbc.ORStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * query executor interface
 *
 * @author zhangting
 * @since  2025-06-29
 */
public interface ORQueryExecutor {
    /**
     * execute query
     *
     * @param cachedQuery query info
     * @param batchParameters batch parameters
     * @throws SQLException if a database access error occurs
     */
    void execute(ORCachedQuery cachedQuery, List<ORParameterList> batchParameters) throws SQLException;

    /**
     * commit transaction
     *
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    void commit() throws IOException, SQLException;

    /**
     * transaction rollback
     *
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    void rollback() throws IOException, SQLException;

    /**
     * close connection
     */
    void close();

    /**
     * is the connection closed
     *
     * @return is closed
     */
    boolean isClosed();

    /**
     * close statement
     *
     * @param stat statement
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    void freeStatement(ORStatement stat) throws IOException, SQLException;

    /**
     * close resultSet
     *
     * @param ctStatement statement
     * @throws SQLException if a database access error occurs
     */
    void closeResultSet(ORStatement ctStatement) throws SQLException;

    /**
     * fetch data
     *
     * @param cachedQuery query info
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs
     */
    void fetch(ORCachedQuery cachedQuery) throws SQLException, IOException;

    /**
     * fetch cursor data
     *
     * @param cursorRs cursor ResultSet
     * @throws SQLException if a database access error occurs
     */
    void fetchCursor(ORCursorResultSet cursorRs) throws SQLException;

    /**
     * cancel
     *
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    void cancel() throws IOException, SQLException;

    /**
     * fetch lob data
     *
     * @param cachedQuery query info
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database access error occurs
     */
    void handleLobRead(ORCachedQuery cachedQuery) throws IOException, SQLException;
}
