/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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

/**
 * <p>Specifies which mode is used to execute queries to database: simple means ('Q' execute, no parse, no bind, text mode only),
 * extended means always use bind/execute messages, extendedForPrepared means extended for prepared statements only.</p>
 *
 * <p>Note: this is for debugging purposes only.</p>
 *
 * @see org.postgresql.PGProperty#PREFER_QUERY_MODE
 */
public enum PreferQueryMode {
  SIMPLE("simple"),
  EXTENDED_FOR_PREPARED("extendedForPrepared"),
  EXTENDED("extended"),
  EXTENDED_CACHE_EVERYTHING("extendedCacheEverything");

  private final String value;

  PreferQueryMode(String value) {
    this.value = value;
  }

  public static PreferQueryMode of(String mode) {
    for (PreferQueryMode preferQueryMode : values()) {
      if (preferQueryMode.value.equals(mode)) {
        return preferQueryMode;
      }
    }
    return EXTENDED;
  }

  public String value() {
    return value;
  }
}
