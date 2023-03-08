/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

package org.postgresql.quickautobalance;

import org.postgresql.PGProperty;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.SetupQueryRunner;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.StatementCancelState;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * Connection info used in quick auto balance.
 */
public class ConnectionInfo {
    /**
     * Default maxIdleTimeBeforeTerminal.
     */
    public static final long DEFAULT_MAX_IDLE_TIME_BEFORE_TERMINAL = 30L;

    public static final String ENABLE_QUICK_AUTO_BALANCE_PARAMS = "true";

    private static final long MAX_IDLE_TIME_BEFORE_TERMINAL_MAX_VALUE = 9223372036854775L;

    private static Log LOGGER = Logger.getLogger(ConnectionInfo.class.getName());

    private final PgConnection pgConnection;

    private final long createTimeStamp;

    private final String autoBalance;

    private boolean enableQuickAutoBalance;

    // max idle time of connection, units: second
    private long maxIdleTimeBeforeTerminal;

    private final HostSpec hostSpec;

    private volatile StatementCancelState connectionState;

    // the timestamp when state change last time
    private volatile long stateLastChangedTimeStamp;

    @Override
    public int hashCode() {
        return Objects.hash(pgConnection, createTimeStamp, autoBalance, enableQuickAutoBalance,
            maxIdleTimeBeforeTerminal, hostSpec);
    }

    public ConnectionInfo(PgConnection pgConnection, Properties properties, HostSpec hostSpec)
        throws PSQLException {
        this.pgConnection = pgConnection;
        this.connectionState = StatementCancelState.IDLE;
        this.createTimeStamp = System.currentTimeMillis();
        this.stateLastChangedTimeStamp = createTimeStamp;
        this.autoBalance = properties.getProperty("autoBalance", "");
        this.maxIdleTimeBeforeTerminal = DEFAULT_MAX_IDLE_TIME_BEFORE_TERMINAL;
        this.hostSpec = hostSpec;
        this.maxIdleTimeBeforeTerminal = parseMaxIdleTimeBeforeTerminal(properties);
        this.enableQuickAutoBalance = parseEnableQuickAutoBalance(properties);
    }

    /**
     * Parse enableQuickAutoBalance.
     *
     * @param properties properties
     * @return enableQuickAutoBalance
     * @throws PSQLException EnableQuickAutoBalance parsed failed.
     */
    public static boolean parseEnableQuickAutoBalance(Properties properties) throws PSQLException {
        if (EnableQuickAutoBalanceParams.TRUE.getValue()
            .equals(PGProperty.ENABLE_QUICK_AUTO_BALANCE.get(properties))) {
            return true;
        } else if (EnableQuickAutoBalanceParams.FALSE.getValue()
            .equals(PGProperty.ENABLE_QUICK_AUTO_BALANCE.get(properties))) {
            return false;
        } else {
            throw new PSQLException(
                GT.tr("Parameter enableQuickAutoBalance={0} parsed failed, value range: '{true, false'}).",
                    PGProperty.ENABLE_QUICK_AUTO_BALANCE.get(properties)), PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    /**
     * Parse maxIdleTimeBeforeTerminal.
     *
     * @param properties properties
     * @return maxIdleTimeBeforeTerminal
     * @throws PSQLException MaxIdleTimeBeforeTerminal parse failed.
     */
    public static long  parseMaxIdleTimeBeforeTerminal(Properties properties) throws PSQLException {
        long inputMaxIdleTime;
        try {
            String param = PGProperty.MAX_IDLE_TIME_BEFORE_TERMINAL.get(properties);
            inputMaxIdleTime = Long.parseLong(param);
            if (inputMaxIdleTime >= MAX_IDLE_TIME_BEFORE_TERMINAL_MAX_VALUE) {
                throw new PSQLException(
                    GT.tr("Parameter maxIdleTimeBeforeTerminal={0} can not be bigger than {1}, value range: long & [0,{1}).",
                        inputMaxIdleTime, MAX_IDLE_TIME_BEFORE_TERMINAL_MAX_VALUE), PSQLState.INVALID_PARAMETER_VALUE);
            }
            if (inputMaxIdleTime < 0) {
                throw new PSQLException(
                    GT.tr("Parameter maxIdleTimeBeforeTerminal={0} can not be less than 0, value range: long & [0,{1}).",
                        inputMaxIdleTime, MAX_IDLE_TIME_BEFORE_TERMINAL_MAX_VALUE), PSQLState.INVALID_PARAMETER_VALUE);

            }
        } catch (NumberFormatException e) {
            throw new PSQLException(
                GT.tr("Parameter maxIdleTimeBeforeTerminal parsed failed, value range: long & [0,{0}).",
                    MAX_IDLE_TIME_BEFORE_TERMINAL_MAX_VALUE), PSQLState.INVALID_PARAMETER_TYPE);
        }
        return inputMaxIdleTime;
    }

    enum EnableQuickAutoBalanceParams {
        TRUE("true"),
        FALSE("false");

        private final String value;

        EnableQuickAutoBalanceParams(String value) {
            this.value = value;
        }

        /**
         * Get value.
         *
         * @return value
         */
        public String getValue() {
            return this.value;
        }
    }

    public StatementCancelState getConnectionState() {
        return connectionState;
    }

    public synchronized void setConnectionState(StatementCancelState state) {
        if (state != null && !connectionState.equals(state)) {
            connectionState = state;
            stateLastChangedTimeStamp = System.currentTimeMillis();
        }
    }

    public long getMaxIdleTimeBeforeTerminal() {
        return maxIdleTimeBeforeTerminal;
    }

    public String getAutoBalance() {
        return autoBalance;
    }

    public PgConnection getPgConnection() {
        return pgConnection;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final ConnectionInfo that = (ConnectionInfo) o;
        return createTimeStamp == that.createTimeStamp && enableQuickAutoBalance == that.enableQuickAutoBalance &&
            maxIdleTimeBeforeTerminal == that.maxIdleTimeBeforeTerminal && pgConnection.equals(that.pgConnection) &&
            autoBalance.equals(that.autoBalance) && hostSpec.equals(that.hostSpec);
    }

    /**
     * Check whether the connection can be closed.
     * The judgement conditions are as follows:
     * 1. The connection enables quickAutoBalance.
     * 2. The quickAutoBalance start time is later than the connection create time.
     * 3. The connection state is idle.
     * 4. The connection keeps idle at least maxIdleTimeBeforeTerminal seconds.
     *
     * @param quickAutoBalanceStartTime quickAutoBalance start time
     * @return whether the connection can be closed
     */
    public synchronized boolean checkConnectionCanBeClosed(long quickAutoBalanceStartTime) {
        if (pgConnection == null) {
            return false;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(GT.tr("checkConnectionCanBeClosed: server ip={0}, enableQuickAutoBalance={1}, " +
                    "quickAutoBalanceStartTime={2}, createTimeStamp={3}, connectionState={4}, " +
                    "stateLastChangedTimeStamp={5}, currentTimeMillis={6}",
                hostSpec.toString(), isEnableQuickAutoBalance(), quickAutoBalanceStartTime, createTimeStamp,
                connectionState, stateLastChangedTimeStamp, System.currentTimeMillis()));
        }
        if (!isEnableQuickAutoBalance()) {
            return false;
        }
        if (quickAutoBalanceStartTime < createTimeStamp) {
            return false;
        }
        if (!connectionState.equals(StatementCancelState.IDLE)) {
            return false;
        }
        return System.currentTimeMillis() - stateLastChangedTimeStamp > maxIdleTimeBeforeTerminal * 1000;
    }

    public boolean isEnableQuickAutoBalance() {
        return enableQuickAutoBalance;
    }

    /**
     * Check whether a connection is valid.
     *
     * @return whether a connection is valid
     */
    public boolean checkConnectionIsValid() {
        boolean isConnectionValid;
        try {
            QueryExecutor queryExecutor = pgConnection.getQueryExecutor();
            byte[][] bit = SetupQueryRunner.run(queryExecutor, "select 1", true);
            if (bit == null) {
                return false;
            }
            String result = queryExecutor.getEncoding().decode(bit[0]);
            isConnectionValid = result != null && result.equals("1");
        } catch (SQLException | IOException e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(GT.tr("CheckConnectionIsValid failed."));
            }
            isConnectionValid = false;
        }
        return isConnectionValid;
    }

    /**
     * get hostSpec
     *
     * @return hostSpec
     */
    public HostSpec getHostSpec() {
        return hostSpec;
    }
}
