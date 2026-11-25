/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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

package org.postgresql.test.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.postgresql.util.HostSpec;

import org.junit.After;
import org.junit.Test;

/**
 * @author Joe Kutner on 10/19/17.
 *         Twitter: @codefinger
 */
public class HostSpecTest {

  @After
  public void cleanup() {
    System.clearProperty("socksProxyHost");
    System.clearProperty("socksProxyPort");
    System.clearProperty("socksNonProxyHosts");
  }

  @Test
  public void testShouldResolve() throws Exception {
    HostSpec hostSpec = new HostSpec("localhost", 5432);
    assertTrue(hostSpec.shouldResolve());
  }

  @Test
  public void testShouldResolveWithEmptySocksProxyHost() throws Exception {
    System.setProperty("socksProxyHost", "");
    HostSpec hostSpec = new HostSpec("localhost", 5432);
    assertTrue(hostSpec.shouldResolve());
  }

  @Test
  public void testShouldResolveWithWhiteSpaceSocksProxyHost() throws Exception {
    System.setProperty("socksProxyHost", " ");
    HostSpec hostSpec = new HostSpec("localhost", 5432);
    assertTrue(hostSpec.shouldResolve());
  }

  @Test
  public void testShouldResolveWithSocksProxyHost() throws Exception {
    System.setProperty("socksProxyHost", "fake-socks-proxy");
    HostSpec hostSpec = new HostSpec("example.com", 5432);
    assertFalse(hostSpec.shouldResolve());
  }

  @Test
  public void testShouldResolveWithSocksProxyHostWithLocalhost() throws Exception {
    System.setProperty("socksProxyHost", "fake-socks-proxy");
    HostSpec hostSpec = new HostSpec("localhost", 5432);
    assertTrue(hostSpec.shouldResolve());
  }

  @Test
  public void testShouldResolveWithSocksNonProxyHost() throws Exception {
    System.setProperty("socksProxyHost", "fake-socks-proxy");
    System.setProperty("socksNonProxyHosts", "example.com");
    HostSpec hostSpec = new HostSpec("example.com", 5432);
    assertTrue(hostSpec.shouldResolve());
  }

  @Test
  public void testShouldResolveWithSocksNonProxyHosts() throws Exception {
    System.setProperty("socksProxyHost", "fake-socks-proxy");
    System.setProperty("socksNonProxyHosts", "example.com|localhost");
    HostSpec hostSpec = new HostSpec("example.com", 5432);
    assertTrue(hostSpec.shouldResolve());
  }

  @Test
  public void testShouldResolveWithSocksNonProxyHostsNotMatching() throws Exception {
    System.setProperty("socksProxyHost", "fake-socks-proxy");
    System.setProperty("socksNonProxyHosts", "example.com|localhost");
    HostSpec hostSpec = new HostSpec("example.org", 5432);
    assertFalse(hostSpec.shouldResolve());
  }
}
