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

package org.postgresql.readwritesplitting;

import org.postgresql.hostchooser.HostRequirement;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.HostSpec;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * PG connection manager.
 *
 * @since 2023-11-20
 */
public class PgConnectionManager implements AutoCloseable {
    private final MethodInvocationRecorder<Connection> methodInvocationRecorder = new MethodInvocationRecorder<>();

    private final ForceExecuteTemplate<PgConnection> forceExecuteTemplate = new ForceExecuteTemplate<>();

    private final Map<String, PgConnection> cachedConnections = new ConcurrentHashMap<>();

    private final AtomicReference<PgConnection> currentConnection = new AtomicReference<>();

    private final Properties props;

    private final String user;

    private final String database;

    private final String url;

    private final ReadWriteSplittingPgConnection readWriteSplittingPgConnection;

    /**
     * Constructor.
     *
     * @param props props
     * @param user user
     * @param database database
     * @param url url
     * @param connection read write splitting pg connection
     */
    public PgConnectionManager(Properties props, String user, String database, String url,
                               ReadWriteSplittingPgConnection connection) {
        this.props = props;
        this.user = user;
        this.database = database;
        this.url = url;
        this.readWriteSplittingPgConnection = connection;
    }

    /**
     * Get connection.
     *
     * @param hostSpec host spec
     * @return connection
     * @throws SQLException SQL exception
     */
    public synchronized PgConnection getConnection(HostSpec hostSpec) throws SQLException {
        String cacheKey = getCacheKey(hostSpec);
        PgConnection result = cachedConnections.get(cacheKey);
        if (result == null) {
            result = createConnection(hostSpec, cacheKey);
        }
        setCurrentConnection(result);
        return result;
    }

    private PgConnection createConnection(HostSpec hostSpec, String cacheKey) throws SQLException {
        PgConnection result = new PgConnection(new HostSpec[]{hostSpec}, user, database, props, url);
        methodInvocationRecorder.replay(result);
        cachedConnections.put(cacheKey, result);
        return result;
    }

    private void setCurrentConnection(PgConnection result) {
        currentConnection.set(result);
    }

    /**
     * Get current connection.
     *
     * @return current connection
     * @throws SQLException SQL exception
     */
    public PgConnection getCurrentConnection() throws SQLException {
        PgConnection result = currentConnection.get();
        return result == null ? getConnection(selectCurrentHostSpec()) : result;
    }

    private HostSpec selectCurrentHostSpec() {
        ReadWriteSplittingHostSpec readWriteHostSpec = readWriteSplittingPgConnection.getReadWriteSplittingHostSpec();
        if (HostRequirement.master == readWriteHostSpec.getTargetServerType()) {
            return readWriteHostSpec.getWriteHostSpec();
        }
        if (HostRequirement.secondary == readWriteHostSpec.getTargetServerType()) {
            return readWriteHostSpec.readLoadBalance();
        }
        return readWriteHostSpec.getWriteHostSpec();
    }

    private String getCacheKey(HostSpec hostSpec) {
        return hostSpec.getHost() + ":" + hostSpec.getPort();
    }

    @Override
    public void close() throws SQLException {
        try {
            forceExecuteTemplate.execute(cachedConnections.values(), PgConnection::close);
        } finally {
            cachedConnections.clear();
        }
    }

    /**
     * Set auto commit.
     *
     * @param isAutoCommit auto commit
     * @throws SQLException SQL exception
     */
    public void setAutoCommit(final boolean isAutoCommit) throws SQLException {
        methodInvocationRecorder.record("setAutoCommit", target -> target.setAutoCommit(isAutoCommit));
        forceExecuteTemplate.execute(cachedConnections.values(), connection -> connection.setAutoCommit(isAutoCommit));
    }

    /**
     * Set transaction isolation.
     *
     * @param level transaction isolation level
     * @throws SQLException SQL exception
     */
    public void setTransactionIsolation(int level) throws SQLException {
        methodInvocationRecorder.record("setTransactionIsolation",
                connection -> connection.setTransactionIsolation(level));
        forceExecuteTemplate.execute(cachedConnections.values(),
                connection -> connection.setTransactionIsolation(level));
    }

    /**
     * Set schema.
     *
     * @param schema schema
     * @throws SQLException SQL exception
     */
    public void setSchema(String schema) throws SQLException {
        methodInvocationRecorder.record("setSchema", connection -> connection.setSchema(schema));
        forceExecuteTemplate.execute(cachedConnections.values(), connection -> connection.setSchema(schema));
    }

    /**
     * Commit.
     *
     * @throws SQLException SQL exception
     */
    public void commit() throws SQLException {
        forceExecuteTemplate.execute(cachedConnections.values(), Connection::commit);
    }

    /**
     * Rollback.
     *
     * @throws SQLException SQL exception
     */
    public void rollback() throws SQLException {
        forceExecuteTemplate.execute(cachedConnections.values(), Connection::rollback);
    }

    /**
     * Set read only.
     *
     * @param isReadOnly read only
     * @throws SQLException SQL exception
     */
    public void setReadOnly(final boolean isReadOnly) throws SQLException {
        methodInvocationRecorder.record("setReadOnly", connection -> connection.setReadOnly(isReadOnly));
        forceExecuteTemplate.execute(cachedConnections.values(), connection -> connection.setReadOnly(isReadOnly));
    }

    /**
     * Whether connection valid.
     *
     * @param timeout timeout
     * @return connection valid or not
     * @throws SQLException SQL exception
     */
    public boolean isValid(final int timeout) throws SQLException {
        for (Connection each : cachedConnections.values()) {
            if (!each.isValid(timeout)) {
                return false;
            }
        }
        return true;
    }
}
