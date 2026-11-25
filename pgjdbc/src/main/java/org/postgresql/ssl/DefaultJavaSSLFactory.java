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

package org.postgresql.ssl;

import java.util.Properties;
import javax.net.ssl.SSLSocketFactory;

/**
 * Socket factory that uses Java's default truststore to validate server certificate.
 * Note: it always validates server certificate, so it might result to downgrade to non-encrypted
 * connection when default truststore lacks certificates to validate server.
 */
public class DefaultJavaSSLFactory extends WrappedFactory {
  public DefaultJavaSSLFactory(Properties info) {
    _factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
  }
}
