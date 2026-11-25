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
