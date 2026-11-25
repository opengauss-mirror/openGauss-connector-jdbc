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

package org.postgresql.core;

public enum JavaVersion {
  // Note: order is important,
  v1_6,
  v1_7,
  v1_8,
  other;

  private static final JavaVersion RUNTIME_VERSION = from(System.getProperty("java.version"));

  /**
   * Returns enum value that represents current runtime. For instance, when using -jre7.jar via Java
   * 8, this would return v18
   *
   * @return enum value that represents current runtime.
   */
  public static JavaVersion getRuntimeVersion() {
    return RUNTIME_VERSION;
  }

  /**
   * Java version string like in {@code "java.version"} property.
   *
   * @param version string like 1.6, 1.7, etc
   * @return JavaVersion enum
   */
  public static JavaVersion from(String version) {
    // Minimum supported is Java 1.6
    if (version.startsWith("1.6")) {
      return v1_6;
    }
    if (version.startsWith("1.7")) {
      return v1_7;
    }
    if (version.startsWith("1.8")) {
      return v1_8;
    }
    return other;
  }
}
