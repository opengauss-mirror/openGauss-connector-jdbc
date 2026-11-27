/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
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

package org.postgresql.jdbc;

import org.postgresql.PGProperty;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.util.Properties;

public enum SslMode {
  /**
   * Do not use encrypted connections.
   */
  DISABLE("disable"),
  /**
   * Start with non-encrypted connection, then try encrypted one.
   */
  ALLOW("allow"),
  /**
   * Start with encrypted connection, fallback to non-encrypted (default).
   */
  PREFER("prefer"),
  /**
   * Ensure connection is encrypted.
   */
  REQUIRE("require"),
  /**
   * Ensure connection is encrypted, and client trusts server certificate.
   */
  VERIFY_CA("verify-ca"),
  /**
   * Ensure connection is encrypted, client trusts server certificate, and server hostname matches
   * the one listed in the server certificate.
   */
  VERIFY_FULL("verify-full"),
  ;

  public static final SslMode[] VALUES = values();

  public final String value;

  SslMode(String value) {
    this.value = value;
  }

  public boolean requireEncryption() {
    return this.compareTo(REQUIRE) >= 0;
  }

  public boolean verifyCertificate() {
    return this == VERIFY_CA || this == VERIFY_FULL;
  }

  public boolean verifyPeerName() {
    return this == VERIFY_FULL;
  }

  public static SslMode of(Properties info) throws PSQLException {
    String sslmode = PGProperty.SSL_MODE.get(info);
    // If sslmode is not set, fallback to ssl parameter
    if (sslmode == null) {
      if (PGProperty.SSL.getBoolean(info)) {
        return VERIFY_FULL;
      }
      return DISABLE;
    }

    for (SslMode sslMode : VALUES) {
      if (sslMode.value.equalsIgnoreCase(sslmode)) {
        return sslMode;
      }
    }
    throw new PSQLException(GT.tr("Invalid sslmode value: {0}", sslmode),
        PSQLState.CONNECTION_UNABLE_TO_CONNECT);
  }
}
