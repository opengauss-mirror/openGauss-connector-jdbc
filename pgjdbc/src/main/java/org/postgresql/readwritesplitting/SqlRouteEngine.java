/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package org.postgresql.readwritesplitting;

import org.apache.shardingsphere.sql.parser.api.CacheOption;
import org.apache.shardingsphere.sql.parser.api.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.api.SQLStatementVisitorEngine;
import org.apache.shardingsphere.sql.parser.core.ParseASTNode;
import org.apache.shardingsphere.sql.parser.sql.common.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.handler.dml.SelectStatementHandler;
import org.postgresql.hostchooser.HostRequirement;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.HostSpec;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * SQL route engine.
 *
 * @since 2023-11-20
 */
public class SqlRouteEngine {
    private static final String DATABASE_TYPE = "openGauss";

    private static final SQLParserEngine PARSE_ENGINE = new SQLParserEngine(DATABASE_TYPE, new CacheOption(128, 1024L));

    private static Log LOGGER = Logger.getLogger(SqlRouteEngine.class.getName());

    /**
     * Route SQL.
     *
     * @param readWriteSplittingPgConnection read write splitting PG Connection
     * @param sql SQL
     * @return routed connection
     * @throws SQLException SQL exception
     */
    public static Connection getRoutedConnection(ReadWriteSplittingPgConnection readWriteSplittingPgConnection,
                                                 String sql) throws SQLException {
        HostSpec hostSpec = SqlRouteEngine.route(sql, readWriteSplittingPgConnection);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Routed connection host spec: " + hostSpec);
        }
        return readWriteSplittingPgConnection.getConnectionManager().getConnection(hostSpec);
    }

    /**
     * Route SQL.
     *
     * @param sql SQL
     * @param readWriteSplittingPgConnection read write splitting PG Connection
     * @return host spec
     * @throws SQLException sql exception
     */
    public static HostSpec route(String sql, ReadWriteSplittingPgConnection readWriteSplittingPgConnection)
            throws SQLException {
        ReadWriteSplittingHostSpec hostSpec = readWriteSplittingPgConnection.getReadWriteSplittingHostSpec();
        if (!readWriteSplittingPgConnection.getAutoCommit()) {
            return hostSpec.getWriteHostSpec();
        }
        try {
            if (HostRequirement.master == hostSpec.getTargetServerType()) {
                return hostSpec.getWriteHostSpec();
            }
            if (HostRequirement.secondary == hostSpec.getTargetServerType()) {
                return hostSpec.readLoadBalance();
            }
            ParseASTNode parseASTNode = PARSE_ENGINE.parse(sql, true);
            SQLStatement sqlStatement = new SQLStatementVisitorEngine(DATABASE_TYPE, false).visit(parseASTNode);
            if (isWriteRouteStatement(sqlStatement)) {
                return hostSpec.getWriteHostSpec();
            }
        } catch (final Exception ignored) {
            return hostSpec.getWriteHostSpec();
        }
        return hostSpec.readLoadBalance();
    }

    private static boolean isWriteRouteStatement(final SQLStatement sqlStatement) {
        return containsLockSegment(sqlStatement) || !(sqlStatement instanceof SelectStatement);
    }

    private static boolean containsLockSegment(final SQLStatement sqlStatement) {
        return sqlStatement instanceof SelectStatement
                && SelectStatementHandler.getLockSegment((SelectStatement) sqlStatement).isPresent();
    }
}
