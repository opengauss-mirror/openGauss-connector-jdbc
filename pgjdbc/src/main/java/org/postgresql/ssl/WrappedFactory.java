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

package org.postgresql.ssl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import javax.net.ssl.SSLSocketFactory;

/**
 * Provide a wrapper to a real SSLSocketFactory delegating all calls to the contained instance. A
 * subclass needs only provide a constructor for the wrapped SSLSocketFactory.
 */
public abstract class WrappedFactory extends SSLSocketFactory {

  protected SSLSocketFactory _factory;

  public Socket createSocket(InetAddress host, int port) throws IOException {
    return _factory.createSocket(host, port);
  }

  public Socket createSocket(String host, int port) throws IOException {
    return _factory.createSocket(host, port);
  }

  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException {
    return _factory.createSocket(host, port, localHost, localPort);
  }

  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return _factory.createSocket(address, port, localAddress, localPort);
  }

  public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
      throws IOException {
    return _factory.createSocket(socket, host, port, autoClose);
  }

  public String[] getDefaultCipherSuites() {
    return _factory.getDefaultCipherSuites();
  }

  public String[] getSupportedCipherSuites() {
    return _factory.getSupportedCipherSuites();
  }
}
