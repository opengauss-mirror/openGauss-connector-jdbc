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

package org.postgresql.ssl;

import org.postgresql.core.ORStream;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.UnrecoverableKeyException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * ORStream SSL, create SSL socket
 *
 * @author zhangting
 * @since  2026-06-02
 */
public class ORStreamSSL {
    private static Log LOGGER = Logger.getLogger(ORStreamSSL.class.getName());
    private static final String SECURITY_PROTOCOL = "TLSv1.3";
    private static final String SSL_KEY_STORE = "javax.net.ssl.keyStore";
    private static final String SSL_KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";
    private static final String SSL_KEY_STORE_PWD = "javax.net.ssl.keyStorePassword";
    private static final String SSL_TRUST_STORE = "javax.net.ssl.trustStore";
    private static final String SSL_TRUST_STORE_TYPE = "javax.net.ssl.trustStoreType";
    private static final String SSL_TRUST_STORE_PWD = "javax.net.ssl.trustStorePassword";
    private static final String URL_PROTOCOL = "file";
    private static final int PROTOCOL_LEN = 5;

    /**
     * create SSL connection
     *
     * @param orStream orStream
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs
     */
    public static void createSSLSocket(ORStream orStream) throws SQLException, IOException {
        SSLSocket socket = getSSLSocket(orStream);
        if (socket == null) {
            return;
        }
        socket.setUseClientMode(true);
        List<String> supportedProtocols = Arrays.asList(socket.getSupportedProtocols());
        if (supportedProtocols.contains(SECURITY_PROTOCOL)) {
            socket.setEnabledProtocols(new String[]{"TLSv1.2", "TLSv1.3"});
        } else {
            socket.setEnabledProtocols(new String[]{"TLSv1.2"});
        }

        List<String> supportedCiphers = new ArrayList();
        for (String cipher : socket.getSupportedCipherSuites()) {
            if ("TLS_AES_256_GCM_SHA384".equals(cipher)
                    || "TLS_AES_128_GCM_SHA256".equals(cipher)
                    || "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384".equals(cipher)
                    || "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256".equals(cipher)
                    || "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384".equals(cipher)
                    || "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".equals(cipher)) {
                supportedCiphers.add(cipher);
            }
        }

        String[] ciphers = supportedCiphers.toArray(new String[0]);
        socket.setEnabledCipherSuites(ciphers);
        socket.startHandshake();
        orStream.setSslSocket(socket);
    }

    private static SSLSocket getSSLSocket(ORStream orStream) throws SQLException {
        KeyManager[] kms = null;
        List<TrustManager> tms = new ArrayList();
        Properties properties = orStream.getProperties();

        try {
            String sslKS = properties.getProperty(SSL_KEY_STORE);
            String ksType = properties.getProperty(SSL_KEY_STORE_TYPE, "JKS");
            String ksPwd = properties.getProperty(SSL_KEY_STORE_PWD);
            KeyStore ks = buildStore(sslKS, ksType, ksPwd);
            char[] pwd = ksPwd == null ? new char[0] : ksPwd.toCharArray();
            if (ks != null) {
                KeyManagerFactory keyfact = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyfact.init(ks, pwd);
                kms = keyfact.getKeyManagers();
            }

            String sslTS = properties.getProperty(SSL_TRUST_STORE);
            String tsType = properties.getProperty(SSL_TRUST_STORE_TYPE, "JKS");
            String tsPwd = properties.getProperty(SSL_TRUST_STORE_PWD);
            KeyStore ts = buildStore(sslTS, tsType, tsPwd);
            TrustManagerFactory trustfact = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustfact.init(ts);

            for (TrustManager manager : trustfact.getTrustManagers()) {
                if (manager instanceof X509TrustManager) {
                    X509TrustManager trustManager = (X509TrustManager) manager;
                    OGTrustManager ogTrustManager = new OGTrustManager(trustManager, orStream.isVerifyCert());
                    tms.add(ogTrustManager);
                } else {
                    tms.add(manager);
                }
            }
            if (tms.isEmpty()) {
                tms.add(new OGTrustManager());
            }
        } catch (NoSuchAlgorithmException nsaex) {
            throw new PSQLException(GT.tr("Unsupported keystore algorithm."), PSQLState.CONNECTION_FAILURE, nsaex);
        } catch (UnrecoverableKeyException ukex) {
            throw new PSQLException(GT.tr("Recover keys from client keystore failed."),
                    PSQLState.CONNECTION_FAILURE, ukex);
        } catch (KeyStoreException kse) {
            throw new PSQLException(GT.tr("Init keyStore manager factory failed."), PSQLState.CONNECTION_FAILURE, kse);
        }

        try {
            TrustManager[] managers = tms.toArray(new TrustManager[0]);
            SSLContext sslInstance = SSLContext.getInstance("TLS");
            sslInstance.init(kms, managers, null);
            Socket socket = sslInstance.getSocketFactory().createSocket(orStream.getSocket(),
                    orStream.getHostAddress().getAddress(), orStream.getHostAddress().getPort(), true);
            if (socket instanceof SSLSocket) {
                return (SSLSocket) socket;
            }
            return null;
        } catch (KeyManagementException kmex) {
            throw new PSQLException(GT.tr(kmex.getMessage()), PSQLState.CONNECTION_FAILURE, kmex);
        } catch (NoSuchAlgorithmException nsaex) {
            throw new PSQLException(GT.tr("SSL protocol is incorrect."), PSQLState.CONNECTION_FAILURE, nsaex);
        } catch (Exception ex) {
            throw new PSQLException(GT.tr("Create SSL socket failed."), PSQLState.CONNECTION_FAILURE, ex);
        }
    }

    private static KeyStore buildStore(String store, String type, String pwd) throws SQLException {
        String sslStore = store;
        InputStream stream = null;
        try {
            if (store != null && !store.isEmpty()) {
                new URL(store);
            }
        } catch (MalformedURLException ex) {
            sslStore = URL_PROTOCOL + ":" + store;
        }

        try {
            if (sslStore == null || sslStore.isEmpty() || type == null || type.isEmpty()) {
                return null;
            }

            char[] pwdCs = new char[0];
            if (pwd != null) {
                pwdCs = pwd.toCharArray();
            }
            URL url = new URL(sslStore);
            String ksUrl = sslStore.substring(PROTOCOL_LEN);
            File storeFile = new File(ksUrl);
            String protocol = url.getProtocol();
            if (URL_PROTOCOL.equalsIgnoreCase(protocol)) {
                url = new URL(URL_PROTOCOL + ":" + storeFile.getCanonicalPath());
            }
            KeyStore ks = KeyStore.getInstance(type);
            stream = url.openStream();
            ks.load(stream, pwdCs);
            return ks;
        } catch (KeyStoreException ksex) {
            throw new PSQLException(GT.tr("Create KeyStore instance failed."), PSQLState.CONNECTION_FAILURE, ksex);
        } catch (NoSuchAlgorithmException nsaex) {
            throw new PSQLException(GT.tr("Keystore algorithm is not supported."), PSQLState.CONNECTION_FAILURE, nsaex);
        } catch (CertificateException cex) {
            throw new PSQLException(GT.tr("Load keystore from {0} failed.", sslStore),
                    PSQLState.CONNECTION_FAILURE, cex);
        } catch (MalformedURLException muex) {
            throw new PSQLException(GT.tr("URL {0} is invalid.", sslStore), PSQLState.CONNECTION_FAILURE, muex);
        } catch (IOException e) {
            throw new PSQLException(GT.tr("Failed to open URL {0}.", sslStore), PSQLState.CONNECTION_FAILURE, e);
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
            } catch (IOException e) {
                LOGGER.warn("IOException occur on close stream: ", e);
            }
        }
    }
}
