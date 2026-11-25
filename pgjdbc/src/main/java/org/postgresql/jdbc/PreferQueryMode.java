/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
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
