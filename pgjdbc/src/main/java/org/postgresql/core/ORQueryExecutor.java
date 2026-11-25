/*
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
