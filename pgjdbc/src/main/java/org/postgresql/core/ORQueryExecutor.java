/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

package org.postgresql.core;

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
    void fetch(ORCachedQuery cachedQuery) throws SQLException, IOException ;
}
