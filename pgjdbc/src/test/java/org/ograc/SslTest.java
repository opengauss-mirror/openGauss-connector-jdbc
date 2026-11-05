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

package org.ograc;

import org.junit.Test;
import org.postgresql.core.ORStream;
import org.postgresql.jdbc.ORConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * test ssl
 *
 * @author zhangting
 * @since  2026-09-17
 */
public class SslTest {
    @Test
    public void testOrConnectionSslPropertyTrue() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=true")) {
            assertTrue(conn instanceof ORConnection);
            assertTrue(((ORConnection) conn).isSsl());
        }
    }

    @Test
    public void testOrConnectionSslPropertyFalse() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=false")) {
            assertTrue(conn instanceof ORConnection);
            assertFalse(((ORConnection) conn).isSsl());
        }
    }

    @Test
    public void testOrStreamUseSslDisabled() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=false&useSSL=false")) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertNotNull(stream);
            assertFalse(stream.isSsl());
            assertFalse(stream.isVerifyCert());
            assertTrue(conn.isValid(5));
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testOrStreamUseSslEnabled() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=true&useSSL=true")) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertNotNull(stream);
            assertTrue(orConn.isSsl());
            assertTrue(conn.isValid(5));
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testVerifyCertProperty() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=true&useSSL=true&verifyCert=true")) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertNotNull(stream);
            assertTrue(stream.isVerifyCert());
            assertTrue(conn.isValid(5));
        }
        try (Connection conn = TestUtil.openDB("ssl=true&useSSL=true&verifyCert=false")) {
            assertTrue(conn instanceof ORConnection);
            ORConnection orConn = (ORConnection) conn;
            ORStream stream = orConn.getORStream();
            assertNotNull(stream);
            assertFalse(stream.isVerifyCert());
        }
    }

    @Test
    public void testSslConnectionExecuteQuery() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=true&useSSL=true")) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("select 1 as c")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testNonSslConnectionExecuteQuery() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=false&useSSL=false")) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("select 2 as c")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    @Test
    public void testSslConnectionWithProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", TestUtil.getUser());
        props.setProperty("password", TestUtil.getPassword());
        props.setProperty("ssl", "true");
        props.setProperty("useSSL", "true");
        try (Connection conn = TestUtil.openDB(props)) {
            assertTrue(conn instanceof ORConnection);
            assertTrue(((ORConnection) conn).isSsl());
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testOnlySslPropertyDoesNotBreakConnection() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=true&useSSL=true&onlySSL=false")) {
            assertTrue(conn.isValid(5));
            assertEquals(1, queryOne(conn));
        }
    }

    @Test
    public void testSslAndNonSslConnectionsBothValid() throws Exception {
        try (Connection sslConn = TestUtil.openDB("ssl=true&useSSL=true");
             Connection plainConn = TestUtil.openDB("ssl=false&useSSL=false")) {
            assertTrue(sslConn.isValid(5));
            assertTrue(plainConn.isValid(5));
            assertEquals(queryOne(sslConn), queryOne(plainConn));
        }
    }

    @Test
    public void testMetaDataAccessibleOverSslConnection() throws Exception {
        try (Connection conn = TestUtil.openDB("ssl=true&useSSL=true")) {
            assertNotNull(conn.getMetaData());
            assertNotNull(conn.getMetaData().getDatabaseProductName());
            assertNotNull(conn.getMetaData().getDriverName());
        }
    }

    private static int queryOne(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("select 1")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    @Test
    public void testSSLConnection() throws Exception {
        Properties properties = new Properties();
        properties.put("javax.net.ssl.trustStore", "C:\\ssl\\ogracssl\\truststore");
        properties.put("javax.net.ssl.trustStorePassword", "changeme_123");
        properties.put("javax.net.ssl.trustStoreType", "PKCS12");
        properties.put("verifyCert", "false");
        properties.put("useSSL", "true");
        try (Connection conn = TestUtil.openDB(properties);
             Statement stat = conn.createStatement()) {
            stat.execute("select USERNAME, PIPE_TYPE from DV_SESSIONS where SID in (select SID from DV_ME)");
            stat.execute("drop table if exists t_test");
            stat.execute("create table t_test(id int, c1 varchar(32))");
            stat.execute("insert into t_test values(1, 'aaa')");
            stat.execute("insert into t_test values(2, 'bbb')");
            stat.execute("select * from t_test");
        }
    }

    @Test
    public void testTCPConnection() throws Exception {
        try (Connection conn = TestUtil.openDB();
             Statement stat = conn.createStatement()) {
            stat.execute("select USERNAME, PIPE_TYPE from DV_SESSIONS where SID in (select SID from DV_ME)");
            stat.execute("drop table if exists t_test");
            stat.execute("create table t_test(id int, c1 varchar(32))");
            stat.execute("insert into t_test values(1, 'aaa')");
            stat.execute("insert into t_test values(2, 'bbb')");
            stat.execute("select * from t_test");
        }
    }
}
