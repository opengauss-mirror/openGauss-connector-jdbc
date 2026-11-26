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

package org.postgresql.util;

/**
 * Utility class with constants of Driver information.
 */
public final class DriverInfo {

  private DriverInfo() {
  }

  // Driver name
  public static final String DRIVER_NAME = "PostgreSQL JDBC Driver";
  public static final String DRIVER_SHORT_NAME = "PgJDBC";
  public static final String DRIVER_VERSION = "/*$mvn.project.property.parsedversion.osgiversion$*/";
  public static final String DRIVER_FULL_NAME = DRIVER_NAME + " " + DRIVER_VERSION;

  // Driver version
  public static final int MAJOR_VERSION = /*$mvn.project.property.parsedversion.majorversion+";"$*//*-*/42;
  public static final int MINOR_VERSION = /*$mvn.project.property.parsedversion.minorversion+";"$*//*-*/0;
  public static final int PATCH_VERSION = /*$mvn.project.property.parsedversion.incrementalversion+";"$*//*-*/0;

  // JDBC specification
  public static final String JDBC_VERSION = "/*$mvn.project.property.jdbc.specification.version$*/";
  private static final int JDBC_INTVERSION = /*$mvn.project.property.jdbc.specification.version.nodot+";"$*//*-*/42;
  public static final int JDBC_MAJOR_VERSION = JDBC_INTVERSION / 10;
  public static final int JDBC_MINOR_VERSION = JDBC_INTVERSION % 10;

}
