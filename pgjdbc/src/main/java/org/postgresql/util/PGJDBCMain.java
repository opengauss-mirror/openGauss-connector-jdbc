/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
