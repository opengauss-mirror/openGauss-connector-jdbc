/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
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

package org.postgresql.core;

import org.postgresql.PGProperty;
import org.postgresql.jdbc.SslMode;
import org.postgresql.ssl.LibPQFactory;
import org.postgresql.util.GT;
import org.postgresql.util.ObjectFactory;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.util.Properties;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

/**
 * Instantiates {@link SocketFactory} based on the {@link PGProperty#SOCKET_FACTORY}.
 */
public class SocketFactoryFactory {
    /**
     * This built-in factory accepts every server certificate, so it must be handled before loading
     * the configured SSLSocketFactory.
     */
    private static final String NON_VALIDATING_SSL_FACTORY =
            "org.postgresql.ssl.NonValidatingFactory";

  /**
   * Instantiates {@link SocketFactory} based on the {@link PGProperty#SOCKET_FACTORY}.
   *
   * @param info connection properties
   * @return socket factory
   * @throws PSQLException if something goes wrong
   */
  public static SocketFactory getSocketFactory(Properties info) throws PSQLException {
    // Socket factory
    String socketFactoryClassName = PGProperty.SOCKET_FACTORY.get(info);
    if (socketFactoryClassName == null) {
      return SocketFactory.getDefault();
    }
    try {
      return ObjectFactory.instantiate(SocketFactory.class, socketFactoryClassName, info, true,
          PGProperty.SOCKET_FACTORY_ARG.get(info));
    } catch (Exception e) {
      throw new PSQLException(
          GT.tr("The SocketFactory class provided {0} could not be instantiated.",
              socketFactoryClassName),
          PSQLState.CONNECTION_FAILURE, e);
    }
  }

  /**
   * Instantiates {@link SSLSocketFactory} based on the {@link PGProperty#SSL_FACTORY}.
   *
   * @param info connection properties
   * @return SSL socket factory
   * @throws PSQLException if something goes wrong
   */
  public static SSLSocketFactory getSslSocketFactory(Properties info) throws PSQLException {
    String classname = PGProperty.SSL_FACTORY.get(info);
    if (classname == null
        || "org.postgresql.ssl.jdbc4.LibPQFactory".equals(classname)
        || "org.postgresql.ssl.LibPQFactory".equals(classname)) {
      return new LibPQFactory(info);
    }
    // Keep legacy non-verifying modes compatible, but do not let sslfactory bypass the contract of
    // verify-ca or verify-full where the caller explicitly requests server certificate checks.
    if (NON_VALIDATING_SSL_FACTORY.equals(classname)
            && SslMode.of(info).verifyCertificate()) {
        throw new PSQLException(
                GT.tr("The SSLSocketFactory class provided {0} disables certificate validation and "
                        + "cannot be used with sslmode verify-ca or verify-full.", classname),
                PSQLState.CONNECTION_FAILURE);
    }
    try {
      return ObjectFactory.instantiate(SSLSocketFactory.class, classname, info, true,
          PGProperty.SSL_FACTORY_ARG.get(info));
    } catch (Exception e) {
      throw new PSQLException(
          GT.tr("The SSLSocketFactory class provided {0} could not be instantiated.", classname),
          PSQLState.CONNECTION_FAILURE, e);
    }
  }

    /**
     * instance SocketFactory and create uds socket
     *
     * @param properties connection properties
     * @return Socket
     * @throws IOException if an I/O error occurs
     * @throws PSQLException if something goes wrong
     */
    public static Socket getUdsSocket(Properties properties) throws IOException, PSQLException {
        ClassLoader loader = ORStream.class.getClassLoader();
        String socketFactoryName = PGProperty.SOCKET_FACTORY.get(properties);
        if (socketFactoryName == null) {
            return SocketFactory.getDefault().createSocket();
        }
        String socketFactoryArg = PGProperty.SOCKET_FACTORY_ARG.get(properties);
        Object factory;
        try {
            Class<?> cls = loader.loadClass(socketFactoryName).asSubclass(SocketFactory.class);
            Constructor<?> constructor = cls.getDeclaredConstructor(String.class);
            factory = constructor.newInstance(socketFactoryArg);
        } catch (InvocationTargetException
                 | InstantiationException
                 | IllegalAccessException
                 | ClassNotFoundException
                 | NoSuchMethodException e) {
            throw new PSQLException(
                    GT.tr("The SocketFactory class provided {0} could not be instantiated.",
                            socketFactoryName), PSQLState.CONNECTION_FAILURE, e);
        }

        Socket socket = null;
        if (factory instanceof SocketFactory) {
            socket = ((SocketFactory) factory).createSocket();
        }
        return socket;
    }
}
