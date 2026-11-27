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

import org.postgresql.PGProperty;
import org.postgresql.core.PGStream;
import org.postgresql.core.SocketFactoryFactory;
import org.postgresql.jdbc.SslMode;
import org.postgresql.util.GT;
import org.postgresql.util.ObjectFactory;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.io.IOException;
import java.util.Properties;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

public class MakeSSL extends ObjectFactory {

  private static Log LOGGER = Logger.getLogger(MakeSSL.class.getName());

  public static void convert(PGStream stream, Properties info)
      throws PSQLException, IOException {
    LOGGER.debug("converting regular socket connection to ssl");

    SSLSocketFactory factory;
    SSLSocket newConnection;
    boolean isTlcp = PGProperty.SSL_TLCP.getBoolean(info);
    if (isTlcp) {
      try {
        Class.forName("org.openeuler.BGMProvider");
      } catch (ClassNotFoundException ex) {
        throw new PSQLException(GT.tr("Could not found bgmProvider, please load bgmProvider jar package manually, and make sure the version is at least v1.1."),
          PSQLState.CONNECTION_FAILURE, ex);
      }
      factory = new LibPQTlcpFactory(info);
    } else {
      factory = SocketFactoryFactory.getSslSocketFactory(info);
    }
    try {
      newConnection = (SSLSocket) factory.createSocket(stream.getSocket(),
          stream.getHostSpec().getHost(), stream.getHostSpec().getPort(), true);
      // We must invoke manually, otherwise the exceptions are hidden
      newConnection.setUseClientMode(true);

      //set supported Cipher suites before SSL handshake
      if (isTlcp) {
        newConnection.setEnabledProtocols(new String[]{"GMTLS"});
        String[] tlcpCipherSuites = new String[] {"ECDHE_SM4_SM3", "ECDHE_SM4_GCM_SM3", "ECC_SM4_SM3", "ECC_SM4_GCM_SM3"};
        newConnection.setEnabledCipherSuites(tlcpCipherSuites);
      } else {
        String[] suppoertedCiphersSuites = getSupportedCiphersSuites(info);
        if (suppoertedCiphersSuites != null) {
            newConnection.setEnabledCipherSuites(suppoertedCiphersSuites);
        }
      }

      newConnection.startHandshake();
    } catch (IOException ex) {
      throw new PSQLException(GT.tr("SSL error: {0}", ex.getMessage()),
          PSQLState.CONNECTION_FAILURE, ex);
    }
    if (factory instanceof LibPQFactory) { // throw any KeyManager exception
      ((LibPQFactory) factory).throwKeyManagerException();
    }

    SslMode sslMode = SslMode.of(info);
    if (sslMode.verifyPeerName()) {
      verifyPeerName(stream, info, newConnection);
    }

    stream.changeSocket(newConnection);
  }

  private static String[] getSupportedCiphersSuites(Properties info) {
	  String supportedSSLCipherSuites = PGProperty.TLS_CIPHERS_SUPPERTED.get(info);
	  return supportedSSLCipherSuites.split(",");
  }

  private static void verifyPeerName(PGStream stream, Properties info, SSLSocket newConnection)
      throws PSQLException {
    String sslhostnameverifier = PGProperty.SSL_HOSTNAME_VERIFIER.get(info);
    HostnameVerifier hvn = PGjdbcHostnameVerifier.INSTANCE;
    if (sslhostnameverifier == null) {
      sslhostnameverifier = "PgjdbcHostnameVerifier";
    } else {
      // Only the built-in verifier is accepted. Loading an externally named class here would
      // execute its static initializer and constructor inside the application JVM.
      if (!PGjdbcHostnameVerifier.class.getName().equals(sslhostnameverifier)) {
        throw new PSQLException(
            GT.tr("The HostnameVerifier class provided {0} could not be instantiated.",
                sslhostnameverifier),
            PSQLState.CONNECTION_FAILURE);
      }
    }

    if (hvn.verify(stream.getHostSpec().getHost(), newConnection.getSession())) {
      return;
    }

    throw new PSQLException(
        GT.tr("The hostname {0} could not be verified by hostnameverifier {1}.",
            stream.getHostSpec().getHost(), sslhostnameverifier),
        PSQLState.CONNECTION_FAILURE);
  }

}
