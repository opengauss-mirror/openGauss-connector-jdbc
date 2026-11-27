/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
