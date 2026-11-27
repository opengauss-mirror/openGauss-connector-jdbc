/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
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
