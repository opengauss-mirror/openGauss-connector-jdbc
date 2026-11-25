/*
 * Copyright (c) 2005, PostgreSQL Global Development Group
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

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.postgresql.test.TestUtil;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class LoginTimeoutTest {

  @Before
  public void setUp() throws Exception {
    TestUtil.initDriver(); // Set up log levels, etc.
  }

  @Test
  public void testIntTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", TestUtil.getUser());
    props.setProperty("password", TestUtil.getPassword());
    props.setProperty("loginTimeout", "10");

    Connection conn = DriverManager.getConnection(TestUtil.getURL(), props);
    conn.close();
  }

  @Test
  public void testFloatTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", TestUtil.getUser());
    props.setProperty("password", TestUtil.getPassword());
    props.setProperty("loginTimeout", "10.0");

    Connection conn = DriverManager.getConnection(TestUtil.getURL(), props);
    conn.close();
  }

  @Test
  public void testZeroTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", TestUtil.getUser());
    props.setProperty("password", TestUtil.getPassword());
    props.setProperty("loginTimeout", "0");

    Connection conn = DriverManager.getConnection(TestUtil.getURL(), props);
    conn.close();
  }

  @Test
  public void testNegativeTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", TestUtil.getUser());
    props.setProperty("password", TestUtil.getPassword());
    props.setProperty("loginTimeout", "-1");

    Connection conn = DriverManager.getConnection(TestUtil.getURL(), props);
    conn.close();
  }

  @Test
  public void testBadTimeout() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", TestUtil.getUser());
    props.setProperty("password", TestUtil.getPassword());
    props.setProperty("loginTimeout", "zzzz");

    Connection conn = DriverManager.getConnection(TestUtil.getURL(), props);
    conn.close();
  }

  private static class TimeoutHelper implements Runnable {
    TimeoutHelper() throws IOException {
      InetAddress localAddr;
      try {
        localAddr = InetAddress.getLocalHost();
      } catch (UnknownHostException ex) {
        System.err.println("WARNING: Could not resolve local host name, trying 'localhost'. " + ex);
        localAddr = InetAddress.getByName("localhost");
      }
      this.listenSocket = new ServerSocket(0, 1, localAddr);
    }

    String getHost() {
      return listenSocket.getInetAddress().getHostAddress();
    }

    int getPort() {
      return listenSocket.getLocalPort();
    }

    @Override
    public void run() {
      try {
        Socket newSocket = listenSocket.accept();
        try {
          Thread.sleep(30000);
        } catch (InterruptedException e) {
          // Ignore it.
        }
        newSocket.close();
      } catch (IOException e) {
        // Ignore it.
      }
    }

    void kill() {
      try {
        listenSocket.close();
      } catch (IOException e) {
      }
    }

    private final ServerSocket listenSocket;
  }


  @Test
  public void testTimeoutOccurs() throws Exception {
    // Spawn a helper thread to accept a connection and do nothing with it;
    // this should trigger a timeout.
    TimeoutHelper helper = new TimeoutHelper();
    new Thread(helper, "timeout listen helper").start();

    try {
      String url = "jdbc:postgresql://" + helper.getHost() + ":" + helper.getPort() + "/dummy";
      Properties props = new Properties();
      props.setProperty("user", "dummy");
      props.setProperty("loginTimeout", "5");

      // This is a pretty crude check, but should help distinguish
      // "can't connect" from "timed out".
      long startTime = System.currentTimeMillis();
      Connection conn = null;
      try {
        conn = DriverManager.getConnection(url, props);
        fail("connection was unexpectedly successful");
      } catch (SQLException e) {
        // Ignored.
      } finally {
        if (conn != null) {
          conn.close();
        }
      }

      long endTime = System.currentTimeMillis();
      assertTrue(endTime > startTime + 2500);
    } finally {
      helper.kill();
    }
  }
}

