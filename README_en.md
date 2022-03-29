![openGauss Logo](https://opengauss.org/img/brand/view/logo2.jpg)



## What is openGauss-connector-jdbc

openGauss is an open source relational database management system. It has multi-core high-performance, full link security, intelligent operation and maintenance for enterprise features. openGauss, which is early originated from PostgreSQL, integrates Huawei's core experience in database field for many years. It optimizes the architecture, transaction, storage engine, optimizer and ARM architecture. At the meantime, openGauss as a global database open source community, aims to further advance the development and enrichment of the database software/hardware application ecosystem.

**Java Database Connectivity** (**JDBC**) is an application programming interface (API) for the programming language Java, which defines how a client may access a database. It is a Java-based data access technology used for Java database connectivity. It provides methods to query and update data in a database, and is oriented toward relational databases. openGauss-connector-jdbc is to provide users with access to the database through the Java language application interface . Users can use the jar package provided by the openGauss official website (refer to the [Direct Access section](#1)) or build their own jar package ([refer to the Building from Source section](#BuildfromSource) to operate the database using JDBC.




## Direct access {#1}

Before using the openGauss JDBC driver, make sure your server is up and running with the openGauss database (refer to the openGauss [Quickstart](https://opengauss.org/en/docs/latest/docs/Quickstart/Quickstart.html)）。

### Get from maven central repository

Java developers can get jar packages directly from the maven central repository with the following coordinates:

```
<groupId>org.opengauss</groupId>
<artifactId>opengauss-jdbc</artifactId>
```

### Get from the community website

1. Download the installation package from the official website.

   Click on [link](https://opengauss.org/en/download.html) and under the openGauss Connectors section, select the download button for JDBC_${version} according to the corresponding system of the server where you are deploying the database. ${version} is the version number you need.

2. Decompress the zip file.

   ```
   tar -zxvf openGauss-${version}-JDBC.tar.gz
   ```

3. After unpacking, you can see two jar packages in the same directory, opengauss-jdbc-${version}.jar and postgresql.jar. opengauss-jdbc-${version}.jar is a package that can coexist with PG-JDBC, the package name is changed from 2.0.1 to org.postgresql.jar. postgresql to org.opengauss, and the driver name is replaced from jdbc:postgresql:// to jdbc:opengauss://. This is the same package that is currently available from the maven central repository.

### INSTALLING THE DRIVER

To install the driver, the postgresql.jar file has to be in the classpath.

ie: under LINUX/SOLARIS (the example here is my linux box):

	export CLASSPATH=.:/usr/local/pgsql/share/java/postgresql.jar

or 

```
export CLASSPATH=.:/usr/local/pgsql/share/java/opengauss-jdbc-${version}.jar
```



## Build from Source {#BuildfromSource}

### Overview

The openGauss JDBC driver currently offers 3 ways to build. One is to build via the one-click script build.sh. The second is a step-by-step build via script. The third is to build via the mvn command.

This will compile the correct driver for your JVM, and build a .jar file (Java ARchive) called postgresql.jar and opengauss-jdbc--${version}.jar in output/, and you can get  openGauss-${version}-jdbc.tar.gz too.

Notice: postgresql.jar is conflict use with postgres database. Because all class was in package org.postgresql. opengauss-jdbc-${version}.jar is compatibility with postgres database, all java package renamed `org.opengauss`, and jdbc driver is: `jdbc:opengauss:/`

Remember: Once you have compiled the driver, it will work on ALL platforms that support that version of the API. You don't need to build it for each platform.

### OS and Software Dependency Requirements

 The openGauss JDBC driver is generated to support the following operating systems:

- CentOS 7.6（x86 architecture）
- openEuler-20.03-LTS（aarch64 architecture）
- Windows

The following table lists the software requirements for compiling the openGauss-connector-jdbc.

You are advised to use the default installation packages of the following dependent software in the listed OS installation CD-ROMs or sources. If the following software does not exist, refer to the recommended versions of the software.

Software dependency requirements are as follows:

| Software and Environment Requirements | Recommended Version |
| ------------------------------------- | ------------------- |
| maven                                 | 3.6.1               |
| java                                  | 1.8                 |
| Git Bash (Windows)                    | -                   |

### Downloading openGauss-connector-jdbc

You can download openGauss-connector-jdbc from open source community.

```
git clone https://gitee.com/opengauss/openGauss-connector-jdbc.git
```

Now we have completed openGauss-connector-jdbc code. For example, we store it in following directories.

- /sda/openGauss-connector-jdbc

### Compiling

#### Getting jar packages with one-click scripting (Linux/Windows)

The build.sh in the openGauss-connector-jdbc directory is an important scripting tool for the compilation process. This tool allows for quick code compilation and packaging.

so you can compile the openGauss-connector-jdbc by one command with build.sh. In build.sh, maven and java8 will be installed automatically and use to build target.

1. Execute the following command to get to the code directory:

   ```
   [user@linux sda]$ cd /sda/openGauss-connector-jdbc
   ```

2. Execute the following command to package using build.sh:

   ```
   [user@linux openGauss-connector-jdbc]$ sh build.sh
   ```

   When finished, the following will be displayed to indicate successful packaging:

   ```
   Successfully make postgresql.jar
   Successfully make opengauss-jdbc-${version} jar package
   packaging jdbc...
   Successfully make jdbc jar package in openGauss-${version}-${platform}-${bit}-Jdbc.tar.gz
   clean up temporary directory!
   now, all packages has finished!!
   ```

   After successful compilation, two jar packages will appear, opengauss-jdbc-${version}.jar and postgresql.jar. compiled jar package path is:**/sda/openGauss-connector-jdbc/output**.

#### Getting jar packages using the mvn command (Windows or Linux)

1. Prepare the Java and Maven environments.

2. Execute the following command to get to the code directory：

   ```
   [user@linux sda]$ cd /sda/openGauss-connector-jdbc
   ```

3. Execute the mvn command:

   ```
   [user@linux openGauss-connector-jdbc]$ mvn clean install -Dgpg.skip -Dmaven.test.skip=true
   ```

   A successful build on a Linux system will display the following result: 

   ```
   [INFO] Reactor Summary:
   [INFO] 
   [INFO] openGauss JDBC Driver ............................. SUCCESS [5.344s]
   [INFO] PostgreSQL JDBC Driver aggregate .................. SUCCESS [0.004s]
   [INFO] ------------------------------------------------------------------------
   [INFO] BUILD SUCCESS
   [INFO] ------------------------------------------------------------------------
   [INFO] Total time: 5.439s
   [INFO] Finished at: Tue Aug 31 21:55:01 EDT 2021
   [INFO] Final Memory: 44M/1763M
   [INFO] ------------------------------------------------------------------------
   ```

   Two jar packages will appear after a successful build, opengauss-jdbc-${version}.jar and original-opengauss-jdbc-${version}.jar. jar package path is /sda/openGauss-connector-jdbc/pgjdbc /target/.
   **notice: this build artifact's package name is org.postgresql which different with maven central repository. if you want build package with org.opengauss, please refer to build.sh.**


## Using JDBC

Reference [JDBC-based development](https://opengauss.org/en/docs/latest/docs/Developerguide/development-based-on-jdbc.html).

## Docs

For more details about the installation guide, tutorials, and APIs, please see the [User Documentation](https://gitee.com/opengauss/docs).

## Community

### Governance

Check out how openGauss implements open governance [works](https://gitee.com/opengauss/community/blob/master/governance.md).

### Communication

- WeLink- Communication platform for developers.
- IRC channel at `#opengauss-meeting` (only for meeting minutes logging purpose)
- Mailing-list: https://opengauss.org/en/community/onlineCommunication.html

## Contribution

Welcome contributions. See our [Contributor](https://opengauss.org/en/contribution.html) for more details.

## Release Notes

For the release notes, see our [RELEASE](https://opengauss.org/en/docs/2.0.0/docs/Releasenotes/Releasenotes.html).

## License

[MulanPSL-2.0](http://license.coscl.org.cn/MulanPSL2/)