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

package org.ograc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * test url
 *
 * @author zhangting
 * @since  2026-10-16
 */
public class UrlTest {
    @BeforeClass
    public static void beforeAll() throws Exception {
        TestUtil.init();
    }

    @Test
    public void testAutoBalance() throws Exception {
        assumeTrue("configure ograc.port2(+)/ograc.server2(+) in build.properties for multi-node tests",
                TestUtil.hasMultiHost());
        Connection connection = java.sql.DriverManager.getConnection(
                TestUtil.getConfiguredMultiHostUrl("autoBalance=roundrobin&loggerLevel=debug"),
                TestUtil.getUser(),
                TestUtil.getPassword());
        try {
            try (Statement st = connection.createStatement()) {
                st.execute("drop table if exists t_varchar");
                st.execute("create table t_varchar (c1 varchar(64 byte), c2 varchar(64 char), c3 varchar(64))");
            }
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("select * from t_varchar");
                }
            }
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("insert into t_varchar (c1) values ('testAutoBalance')");
                }
            }
            try (Statement st = connection.createStatement()) {
                st.execute("drop table if exists t_varchar");
            }
        } finally {
            TestUtil.closeQuietly(connection);
        }
    }

    @Test
    public void testUserPasswordConnect() throws Exception {
        try (Connection connection = TestUtil.openDB()) {
            assertNotNull(connection);
            assertFalse(connection.isClosed());
            assertEquals(TestUtil.getUser(), connection.getMetaData().getUserName());
        }
    }

    @Test
    public void testFetchSize() throws Exception {
        try (Connection connection = TestUtil.openDB()) {
            try (Statement statement = connection.createStatement()) {
                assertEquals(0, statement.getFetchSize());
            }
            try (PreparedStatement preparedStatement = connection.prepareStatement("select 1")) {
                assertEquals(0, preparedStatement.getFetchSize());
            }
        }

        try (Connection connection = TestUtil.openDB("fetchsize=10")) {
            try (Statement statement = connection.createStatement()) {
                assertEquals(10, statement.getFetchSize());
            }
            try (PreparedStatement preparedStatement = connection.prepareStatement("select 1")) {
                assertEquals(10, preparedStatement.getFetchSize());
            }
        }
    }

    @Test
    public void testAutoCommit() throws Exception {
        try (Connection connection = TestUtil.openDB()) {
            assertTrue(connection.getAutoCommit());
            connection.setAutoCommit(false);
            assertFalse(connection.getAutoCommit());

            try (Statement st = connection.createStatement()) {
                st.execute("drop table if exists t_varchar");
                st.execute("create table t_varchar (c1 varchar(64 byte), c2 varchar(64 char), c3 varchar(64))");
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute("insert into t_varchar (c1) values ('testAutoCommit')");
            }
            try (PreparedStatement preparedStatement = connection.prepareStatement("select * from t_varchar");
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                assertTrue(resultSet.next());
                assertEquals("testAutoCommit", resultSet.getString(1));
            }

            connection.rollback();
            try (PreparedStatement preparedStatement = connection.prepareStatement("select * from t_varchar");
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                assertFalse(resultSet.next());
            }

            try (Statement statement = connection.createStatement()) {
                statement.execute("insert into t_varchar (c1) values ('testAutoCommit')");
            }
            connection.commit();
            connection.rollback();
            try (PreparedStatement preparedStatement = connection.prepareStatement("select * from t_varchar");
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                assertTrue(resultSet.next());
                assertEquals("testAutoCommit", resultSet.getString(1));
            }
            try (Statement st = connection.createStatement()) {
                st.execute("drop table if exists t_varchar");
            }
            try {
                connection.setAutoCommit(true);
            } catch (SQLException ignore) {
                // ignore
            }
        }
    }

    @Test
    public void testLoginTimeout() throws Exception {
        try (Connection connection = TestUtil.openDB("loginTimeout=0")) {
            assertNotNull(connection);
            assertTrue(connection.isValid(5));
        }
    }

    @Test
    public void testSocketTimeout() throws Exception {
        try (Connection connection = TestUtil.openDB("socketTimeout=0")) {
            assertNotNull(connection);
            assertTrue(connection.isValid(5));
        }
    }

    @Test
    public void testReceiveBufferSize() throws Exception {
        try (Connection connection = TestUtil.openDB("receiveBufferSize=8192")) {
            assertNotNull(connection);
            assertTrue(connection.isValid(5));
        }
    }

    @Test
    public void testSendBufferSize() throws Exception {
        try (Connection connection = TestUtil.openDB("sendBufferSize=8192")) {
            assertNotNull(connection);
            assertTrue(connection.isValid(5));
        }
    }
}
