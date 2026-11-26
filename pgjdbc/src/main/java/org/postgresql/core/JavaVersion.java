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
