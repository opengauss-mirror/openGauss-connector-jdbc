/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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


package org.postgresql.ssl;

import org.postgresql.PGProperty;
import org.postgresql.jdbc.SslMode;
import org.postgresql.ssl.NonValidatingFactory.NonValidatingTM;
import org.postgresql.util.GT;
import org.postgresql.util.ObjectFactory;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;
import org.openeuler.BGMProvider;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.util.Properties;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.callback.CallbackHandler;

/**
 * Provide an SSLSocketFactory that is compatible with the libpq tlcp behaviour.
 */
public class LibPQTlcpFactory extends WrappedFactory {

  LazyKeyManager signKm;
  LazyKeyManager encKm;
  private static Log LOGGER = Logger.getLogger(LibPQTlcpFactory.class.getName());

  /**
   * @param info the connection parameters The following parameters are used:
   *        sslmode,sslcert,sslenccert,sslkey,sslenckey,sslrootcert,sslhostnameverifier,sslPasswordCallback,sslpassword
   * @throws PSQLException if security error appears when initializing factory
   */
  public LibPQTlcpFactory(Properties info) throws PSQLException {
    try {
      TrustManager[] tm = null;
      KeyManager[] keyManagers = null;
      SslMode sslMode = SslMode.of(info);
      Security.insertProviderAt(new BGMProvider(), 1);
      SSLContext ctx = SSLContext.getInstance("GMTLS");

      // Determining the default file location
      String pathsep = System.getProperty("file.separator");
      String defaultDir;
      boolean defaultSignFile = false;
      boolean defaultEncFile = false;
      if (System.getProperty("os.name").toLowerCase().contains("windows")) { // It is Windows
        defaultDir = System.getenv("APPDATA") + pathsep + "postgresql" + pathsep;
      } else {
        defaultDir = System.getProperty("user.home") + pathsep + ".postgresql" + pathsep;
      }

      // get the file path related to tlcp
      String tlcpRootCertFile = PGProperty.SSL_ROOT_CERT.get(info);
      if (tlcpRootCertFile == null) { // Fall back to default
        tlcpRootCertFile = defaultDir + "root.crt";
      }
      String tlcpCertFile = PGProperty.SSL_CERT.get(info);
      if (tlcpCertFile == null) { // Fall back to default
        defaultSignFile = true;
        tlcpCertFile = defaultDir + "postgresql.crt";
      }
      String tlcpEncCertFile = PGProperty.SSL_ENC_CERT.get(info);
      if (tlcpEncCertFile == null) { // Fall back to default
        defaultEncFile = true;
        tlcpEncCertFile = defaultDir + "postgresql_enc.crt";
      }
      String tlcpKeyFile = PGProperty.SSL_KEY.get(info);
      if (tlcpKeyFile == null) { // Fall back to default
        defaultSignFile = true;
        tlcpKeyFile = defaultDir + "postgresql.pk8";
      }
      String tlcpEncKeyFile = PGProperty.SSL_ENC_KEY.get(info);
      if (tlcpEncKeyFile == null) { // Fall back to default
        defaultEncFile = true;
        tlcpEncKeyFile = defaultDir + "postgresql_enc.pk8";
      }

      // initialize trustManager(resolve root certificates)
      if (!sslMode.verifyCertificate()) {
        // server validation is not required
        tm = new TrustManager[]{new NonValidatingTM()};
      } else {
        // Load the server certificate
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        KeyStore ks;
        try {
          ks = KeyStore.getInstance("jks");
        } catch (KeyStoreException e) {
          // this should never happen
          throw new NoSuchAlgorithmException("jks KeyStore not available");
        }
        FileInputStream fis;
        try {
          fis = new FileInputStream(tlcpRootCertFile); // NOSONAR
        } catch (FileNotFoundException ex) {
          throw new PSQLException(
              GT.tr("Could not open SSL root certificate file {0}.", tlcpRootCertFile),
              PSQLState.CONNECTION_FAILURE, ex);
        }
        try {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          // not work in java 1.4
          Object[] certs = cf.generateCertificates(fis).toArray(new Certificate[]{});
          ks.load(null, null);
          for (int i = 0; i < certs.length; i++) {
            ks.setCertificateEntry("cert" + i, (Certificate) certs[i]);
          }
          tmf.init(ks);
        } catch (IOException ioex) {
          throw new PSQLException(
              GT.tr("Could not read SSL root certificate file {0}.", tlcpRootCertFile),
              PSQLState.CONNECTION_FAILURE, ioex);
        } catch (GeneralSecurityException gsex) {
          throw new PSQLException(
              GT.tr("Loading the SSL root certificate {0} into a TrustManager failed.",
                      tlcpRootCertFile),
              PSQLState.CONNECTION_FAILURE, gsex);
        } finally {
          try {
            fis.close();
          } catch (IOException e) {
              /* ignore */
              LOGGER.trace("Catch IOException on close:", e);
          }
        }
        tm = tmf.getTrustManagers();
      }

      // initialize keyManager(resolve server certificates and keys)
      // Determine the callback handler
      CallbackHandler cbh;
      String sslPasswordCallback = PGProperty.SSL_PASSWORD_CALLBACK.get(info);
      if (sslPasswordCallback != null) {
        try {
          cbh = ObjectFactory.instantiate(CallbackHandler.class, sslPasswordCallback, info, false, null);
        } catch (Exception e) {
          throw new PSQLException(
              GT.tr("The password callback class provided {0} could not be instantiated.",
                  sslPasswordCallback),
              PSQLState.CONNECTION_FAILURE, e);
        }
      } else {
        cbh = new LibPQFactory.ConsoleCallbackHandler(PGProperty.SSL_PASSWORD.get(info));
      }

      // If the properties are empty, give null to prevent client key selection
      signKm = new LazyKeyManager(("".equals(tlcpCertFile) ? null : tlcpCertFile),
          ("".equals(tlcpKeyFile) ? null : tlcpKeyFile), cbh, defaultSignFile, PGProperty.SSL_PRIVATEKEY_FACTORY.get(info));
      encKm = new LazyKeyManager(("".equals(tlcpEncCertFile) ? null : tlcpEncCertFile),
          ("".equals(tlcpEncKeyFile) ? null : tlcpEncKeyFile), cbh, defaultEncFile, PGProperty.SSL_PRIVATEKEY_FACTORY.get(info));
      try {
        PrivateKey signKey = signKm.getPrivateKey("signKey");
        PrivateKey encKey = encKm.getPrivateKey("encKey");
        if (signKey != null && encKey != null) {
          KeyStore ks;
          ks = KeyStore.getInstance("PKCS12");
          ks.load(null);
          ks.setKeyEntry("sign", signKey, null, signKm.getCertificateChain("signCert"));
          ks.setKeyEntry("enc", encKey, null, encKm.getCertificateChain("encCert"));
          KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          kmf.init(ks, null);
          keyManagers = kmf.getKeyManagers();
        } else {
          throwKeyManagerException();
        }
      } catch(CertificateException ex) {
        throw new PSQLException(GT.tr("Failed to verify tlcp certificates during the keystore loading phase: {0}.", ex.getMessage()),
          PSQLState.CONNECTION_FAILURE, ex);
      } catch(UnrecoverableKeyException ex) {
        throw new PSQLException(GT.tr("Could not recover key during the keystore initialization phase: {0}.", ex.getMessage()),
          PSQLState.CONNECTION_FAILURE, ex);
      } catch(IOException ex) {
        throw new PSQLException(GT.tr("Some io errors occur during the keystore loading phase: {0}.", ex.getMessage()),
          PSQLState.CONNECTION_FAILURE, ex);
      } catch(KeyStoreException ex) {
        throw new PSQLException(GT.tr("Could not finish keystore processing: {0}.", ex.getMessage()),
          PSQLState.CONNECTION_FAILURE, ex);
      }

      // finally we can initialize the context
      try {
        ctx.init(keyManagers, tm, null);
      } catch (KeyManagementException ex) {
        throw new PSQLException(GT.tr("Could not initialize SSL context."),
            PSQLState.CONNECTION_FAILURE, ex);
      }

      _factory = ctx.getSocketFactory();
    } catch (NoSuchAlgorithmException ex) {
      throw new PSQLException(GT.tr("Could not find a java cryptographic algorithm: {0}.",
              ex.getMessage()), PSQLState.CONNECTION_FAILURE, ex);
    }
  }

  /**
   * Propagates any exception from {@link LazyKeyManager}.
   *
   * @throws PSQLException if there is an exception to propagate
   */
  public void throwKeyManagerException() throws PSQLException {
    if (signKm != null) {
      signKm.throwKeyManagerException();
    }
    if (encKm != null) {
      encKm.throwKeyManagerException();
    }
  }
}
