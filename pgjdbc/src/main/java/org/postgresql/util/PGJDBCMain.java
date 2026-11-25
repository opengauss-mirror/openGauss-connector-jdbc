/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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

import org.postgresql.Driver;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public class PGJDBCMain {

  public static void main(String[] args) {
    if (args.length > 0 && args[0].compareTo("GSVERSION") == 0)
    {
        String gsversion = Driver.getGSVersion();
        System.out.println(gsversion);
        return;
    }
    java.net.URL url = Driver.class.getResource("/org/postgresql/Driver.class");
    System.out.printf("%n%s%n", org.postgresql.util.DriverInfo.DRIVER_FULL_NAME);
    System.out.printf("Found in: %s%n%n", url);

    System.out.printf("The PgJDBC driver is not an executable Java program.%n%n"
                       + "You must install it according to the JDBC driver installation "
                       + "instructions for your application / container / appserver, "
                       + "then use it by specifying a JDBC URL of the form %n    jdbc:postgresql://%n"
                       + "or using an application specific method.%n%n"
                       + "See the PgJDBC documentation: http://jdbc.postgresql.org/documentation/head/index.html%n%n"
                       + "This command has had no effect.%n");

    System.exit(1);
  }
}
