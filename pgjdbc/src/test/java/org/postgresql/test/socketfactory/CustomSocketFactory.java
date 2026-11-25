/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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

package org.postgresql.test.socketfactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;

public class CustomSocketFactory extends SocketFactory {

  private static CustomSocketFactory instance;

  private final String argument;
  private int socketCreated;

  public CustomSocketFactory(String argument) {
    if (instance != null) {
      throw new IllegalStateException("Test failed, multiple custom socket factory instanciation");
    }
    instance = this;
    this.argument = argument;
  }

  @Override
  public Socket createSocket(String arg0, int arg1) throws IOException, UnknownHostException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket(InetAddress arg0, int arg1) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket(String arg0, int arg1, InetAddress arg2, int arg3)
      throws IOException, UnknownHostException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket(InetAddress arg0, int arg1, InetAddress arg2, int arg3)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Socket createSocket() throws IOException {
    socketCreated++;
    return new Socket();
  }

  public String getArgument() {
    return argument;
  }

  public int getSocketCreated() {
    return socketCreated;
  }

  public static CustomSocketFactory getInstance() {
    return instance;
  }

}
