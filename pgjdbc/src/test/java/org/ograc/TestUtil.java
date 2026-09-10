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

package org.ograc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * test util
 *
 * @author zhangting
 * @since  2026-09-05
 */
public final class TestUtil {
    /**
     * JDBC URL prefix for oGRAC connections ({@code jdbc:oGRAC://}).
     */
    public static final String URL_PREFIX = "jdbc:oGRAC://";

    /**
     * Max numbered secondary nodes read from build.properties (ograc.port2 .. ograc.portN).
     */
    private static final int MAX_SECONDARY_NODES = 16;
    private static boolean isInitialized;

    private TestUtil() {
    }

    /**
     * Loads build.properties / system properties and registers the JDBC driver once.
     *
     * @throws Exception if driver initialization fails
     */
    public static synchronized void init() throws Exception {
        if (isInitialized) {
            return;
        }
        org.postgresql.test.TestUtil.initDriver();
        Properties p = loadPropertyFiles("build.properties");
        p.putAll(System.getProperties());
        System.getProperties().putAll(p);
        Class.forName("org.postgresql.Driver");
        isInitialized = true;
    }

    private static Properties loadPropertyFiles(String name) {
        Properties p = new Properties();
        File f = org.postgresql.test.TestUtil.getFile(name);
        if (f.exists()) {
            try (FileInputStream in = new FileInputStream(f)) {
                p.load(in);
            } catch (IOException ignore) {
                // ignore
            }
        }
        File local = org.postgresql.test.TestUtil.getFile("build.local.properties");
        if (local.exists()) {
            try (FileInputStream in = new FileInputStream(local)) {
                p.load(in);
            } catch (IOException ignore) {
                // ignore
            }
        }
        return p;
    }

    /**
     * Primary server host from {@code ograc.server} / {@code server}, default {@code localhost}.
     *
     * @return primary server host name or IP
     */
    public static String getServer() {
        return firstNonEmpty(System.getProperty("ograc.server"), System.getProperty("server"), "localhost");
    }

    /**
     * Primary server port from {@code ograc.port} / {@code port}, default {@code 5432}.
     *
     * @return primary server port number
     */
    public static int getPort() {
        String port = firstNonEmpty(System.getProperty("ograc.port"), System.getProperty("port"), "5432");
        return Integer.parseInt(port);
    }

    /**
     * Server for node index (1 = primary). Falls back to primary server when
     * {@code ograc.serverN} is unset.
     *
     * @param index node index, {@code 1} for primary
     * @return server host for the given node
     */
    public static String getServer(int index) {
        if (index <= 1) {
            return getServer();
        }
        return firstNonEmpty(
                System.getProperty("ograc.server" + index),
                System.getProperty("server" + index),
                getServer());
    }

    /**
     * Port for node index (1 = primary), or {@code null} when that secondary is not configured.
     *
     * @param index node index, {@code 1} for primary
     * @return port number, or {@code null} if the secondary node is not configured
     */
    public static Integer getPortOrNull(int index) {
        if (index <= 1) {
            return getPort();
        }
        String port = firstNonEmpty(
                System.getProperty("ograc.port" + index),
                System.getProperty("port" + index));
        if (port == null || port.isEmpty()) {
            return null;
        }
        return Integer.parseInt(port);
    }

    /**
     * All configured host:port pairs from build.properties / -D properties.
     * Primary always included; secondaries from {@code ograc.port2}..{@code ograc.portN}.
     *
     * @return array of {@code host:port} strings, at least the primary node
     */
    public static String[] getConfiguredHostPorts() {
        java.util.LinkedHashSet<String> hosts = new java.util.LinkedHashSet<String>();
        hosts.add(getServer() + ":" + getPort());
        for (int i = 2; i <= MAX_SECONDARY_NODES; i++) {
            Integer port = getPortOrNull(i);
            if (port == null) {
                continue;
            }
            hosts.add(getServer(i) + ":" + port);
        }
        return hosts.toArray(new String[0]);
    }

    /**
     * Comma-separated multi-host list for jdbc:oGRAC:// URLs.
     *
     * @return comma-separated {@code host:port} list
     */
    public static String getMultiHostPorts() {
        String[] hosts = getConfiguredHostPorts();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hosts.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(hosts[i]);
        }
        return sb.toString();
    }

    /**
     * True when at least two distinct host:port endpoints are configured.
     *
     * @return {@code true} if multi-host is configured; otherwise {@code false}
     */
    public static boolean hasMultiHost() {
        return getConfiguredHostPorts().length >= 2;
    }

    /**
     * Database user from {@code ograc.username} / {@code username}, default {@code test}.
     *
     * @return database user name
     */
    public static String getUser() {
        return firstNonEmpty(System.getProperty("ograc.username"), System.getProperty("username"), "test");
    }

    /**
     * Database password from {@code ograc.password} / {@code password}, default {@code test}.
     *
     * @return database password
     */
    public static String getPassword() {
        return firstNonEmpty(System.getProperty("ograc.password"), System.getProperty("password"), "test");
    }

    /**
     * Single-host jdbc:oGRAC:// URL for the primary server (no query string).
     *
     * @return jdbc:oGRAC:// URL for the primary host and port
     */
    public static String getUrl() {
        return getUrl(getServer(), getPort(), null);
    }

    /**
     * Single-host jdbc:oGRAC:// URL for the primary server with optional query parameters.
     *
     * @param query URL query string without leading {@code ?}, or {@code null}/{@code ""} for none
     * @return jdbc:oGRAC:// URL including the query string when present
     */
    public static String getUrl(String query) {
        return getUrl(getServer(), getPort(), query);
    }

    /**
     * Builds a single-host jdbc:oGRAC:// URL.
     *
     * @param server host name or IP
     * @param port   port number
     * @param query  URL query string without leading {@code ?}, or {@code null}/{@code ""} for none
     * @return jdbc:oGRAC:// URL for the given host, port and query
     */
    public static String getUrl(String server, int port, String query) {
        StringBuilder sb = new StringBuilder(URL_PREFIX).append(server).append(':').append(port);
        if (query != null && !query.isEmpty()) {
            sb.append('?').append(query);
        }
        return sb.toString();
    }

    /**
     * Builds a multi-host jdbc:oGRAC:// URL from an explicit host:port list.
     *
     * @param hostPorts comma-separated {@code host:port} pairs
     * @param query     URL query string without leading {@code ?}, or {@code null}/{@code ""} for none
     * @return jdbc:oGRAC:// multi-host URL
     */
    public static String getMultiHostUrl(String hostPorts, String query) {
        StringBuilder sb = new StringBuilder(URL_PREFIX).append(hostPorts);
        if (query != null && !query.isEmpty()) {
            sb.append('?').append(query);
        }
        return sb.toString();
    }

    /**
     * Multi-host URL built from all configured ograc nodes.
     *
     * @param query URL query string without leading {@code ?}, or {@code null}/{@code ""} for none
     * @return jdbc:oGRAC:// multi-host URL for all configured nodes
     */
    public static String getConfiguredMultiHostUrl(String query) {
        return getMultiHostUrl(getMultiHostPorts(), query);
    }

    /**
     * Opens a connection to the primary oGRAC server using configured credentials.
     *
     * @return open JDBC connection
     * @throws Exception if initialization or connection fails
     */
    public static Connection openDB() throws Exception {
        init();
        return DriverManager.getConnection(getUrl(), getUser(), getPassword());
    }

    /**
     * Opens a connection to the primary oGRAC server with extra URL query parameters.
     *
     * @param query URL query string without leading {@code ?}
     * @return open JDBC connection
     * @throws Exception if initialization or connection fails
     */
    public static Connection openDB(String query) throws Exception {
        init();
        return DriverManager.getConnection(getUrl(query), getUser(), getPassword());
    }

    /**
     * Opens a connection to the primary oGRAC server with the given JDBC properties.
     * Fills {@code user}/{@code password} from configuration when missing.
     *
     * @param props connection properties
     * @return open JDBC connection
     * @throws Exception if initialization or connection fails
     */
    public static Connection openDB(Properties props) throws Exception {
        init();
        Properties p = new Properties(props);
        if (p.getProperty("user") == null) {
            p.setProperty("user", getUser());
        }
        if (p.getProperty("password") == null) {
            p.setProperty("password", getPassword());
        }
        return DriverManager.getConnection(getUrl(), p);
    }

    /**
     * Closes a {@link Connection} and swallows any {@link SQLException}.
     *
     * @param conn connection to close; ignored when {@code null}
     */
    public static void closeQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException ignore) {
            // ignore
        }
    }

    /**
     * Closes a {@link Statement} and swallows any {@link SQLException}.
     *
     * @param stmt statement to close; ignored when {@code null}
     */
    public static void closeQuietly(Statement stmt) {
        if (stmt == null) {
            return;
        }
        try {
            stmt.close();
        } catch (SQLException ignore) {
            // ignore
        }
    }

    /**
     * Drops the given table if it exists.
     *
     * @param conn  open connection
     * @param table table name
     * @throws SQLException if the drop statement fails
     */
    public static void dropTable(Connection conn, String table) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("drop table if exists " + table);
        }
    }

    private static String firstNonEmpty(String... values) {
        for (String v : values) {
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return null;
    }
}
