![openGauss Logo](https://opengauss.org/img/brand/view/logo2.jpg)



## 什么是openGauss-connector-JDBC 

openGauss是一款开源的关系型数据库管理系统，它具有多核高性能、全链路安全性、智能运维等企业级特性。
openGauss内核早期源自开源数据库PostgreSQL，融合了华为在数据库领域多年的内核经验，在架构、事务、存储引擎、优化器及ARM架构上进行了适配与优化。作为一个开源数据库，期望与广泛的开发者共同构建一个多元化技术的开源数据库社区。

Java数据库连接，（Java Database Connectivity，简称**JDBC**）是Java语言中用来规范客户端程序如何来访问数据库的应用程序接口，提供了诸如查询和更新数据库中数据的方法。openGauss-connector-JDBC就是提供给用户通过Java语言访问数据库的应用程序接口。用户可以使用openGauss官网提供的jar包（[参考直接获取部分](#安装)），也可以自行构建jar包（[参考从源码构建部分](#从源码构建)）以使用JDBC操作数据库。




## 直接获取

在使用openGauss JDBC 驱动之前，请确保您的服务器已经可以正常运行 openGauss 数据库（参考openGauss[快速入门](https://opengauss.org/zh/docs/latest/docs/Quickstart/Quickstart.html)）。

### 从maven中央仓库获取

Java开发者可从maven中央仓库中直接获取jar包，坐标如下：

```
<groupId>org.opengauss</groupId>
<artifactId>opengauss-jdbc</artifactId>
```

### 从社区官网下载安装包

1. 在官网下载安装包。

   点击[链接](https://opengauss.org/zh/download.html)，在openGauss Connectors部分下，根据您部署数据库的服务器的对应系统选择JDBC_${version}的下载按钮。${version}即您需要的版本号。

2. 解压压缩包。

   ```
   tar -zxvf openGauss-${version}-JDBC.tar.gz
   ```

3. 解压后可以看到同级目录下出现了两个jar包，分别是opengauss-jdbc-${version}.jar和postgresql.jar。opengauss-jdbc-${version}.jar是可以与PG-JDBC共存的包, 包名自2.0.1之后的版本全部从org.postgresql变更为org.opengauss,并且驱动名称从jdbc:postgresql://替换为jdbc:opengauss://。目前从maven中央仓库中获取的也是这个包。


## 从源码构建

### 概述

openGauss JDBC 驱动目前提供3种构建方式。一是通过一键式脚本build.sh进行构建。二是通过脚本进行逐步构建。三是通过mvn命令进行构建。

### 操作系统和软件依赖要求

openGauss JDBC 驱动的生成支持以下操作系统：

- CentOS 7.6（x86架构）
- openEuler-20.03-LTS（aarch64架构）
- Windows

适配其他系统，参照博客[openGauss数据库编译指导](https://opengauss.org/zh/blogs/blogs.html?post/xingchen/opengauss_compile/)

以下表格列举了编译openGauss的软件要求。

建议使用从列出的操作系统安装盘或安装源中获取的以下依赖软件的默认安装包进行安装。如果不存在以下软件，请参考推荐的软件版本。

软件及环境依赖要求如下：

| 软件及环境要求      | 推荐版本   |
| ------------------- | ---------- |
| maven               | 3.6.1      |
| java                | 1.8        |
| Git Bash (Windows)  | 无推荐版本 |

### 下载openGauss-connector-jdbc源码

可以从开源社区下载openGauss-connector-jdbc源码。

```
git clone https://gitee.com/opengauss/openGauss-connector-jdbc.git
```


现在我们已经拥有完整的openGauss-connector-jdbc代码，把它存储在以下目录中（以sda为例）。

- /sda/openGauss-connector-jdbc

### jar包生成

#### 使用一键式脚本生成jar包（Linux/windows）

openGauss-connector-jdbc中的build.sh是编译过程中的重要脚本工具。该工具可快速进行代码编译和打包。

只需使用如下格式的命令即可编译openGauss-connector-jdbc。

1. 执行如下命令进入到代码目录：

   ```
   [user@linux sda]$ cd /sda/openGauss-connector-jdbc/
   ```

2. 执行如下命令使用build.sh进行打包：

   ```
   [user@linux openGauss-connector-jdbc]$ sh build.sh
   ```

   结束后会显示如下内容，表示打包成功：

   ```
   Successfully make postgresql.jar
   Successfully make opengauss-jdbc-${version} jar package
   packaging jdbc...
   Successfully make jdbc jar package in openGauss-${version}-${platform}-${bit}-Jdbc.tar.gz
   clean up temporary directory!
   now, all packages has finished!
   ```

   成功编译后会出现两个jar包，分别是opengauss-jdbc-${version}.jar与postgresql.jar。编译后的jar包路径为：**/sda/openGauss-connector-jdbc/output**。

#### 使用mvn命令生成jar包（Windows 或 Linux）

1. 准备 Java 与 Maven环境。

2. 执行如下命令进入到代码目录：

   ```
   [user@linux sda]$ cd /sda/openGauss-connector-jdbc
   ```

3. 执行mvn命令：

   ```
   [user@linux openGauss-connector-jdbc]$ mvn clean install -Dgpg.skip -Dmaven.test.skip=true
   ```

   Linux系统下构建成功后会显示如下结果：

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

   构建成功后会出现两个jar包，分别是opengauss-jdbc-${version}.jar与original-opengauss-jdbc-${version}.jar。jar包路径为/sda/openGauss-connector-jdbc/pgjdbc/target/。
   **注意:默认的mvn编译出的jdbc包名为org.postgresql,它与maven中央仓库的包名org.opengauss不同,想打包此包名，请参考build.sh脚本**

## JDBC的使用

参考[基于JDBC开发](https://opengauss.org/zh/docs/latest/docs/Developerguide/%E5%9F%BA%E4%BA%8EJDBC%E5%BC%80%E5%8F%91.html)。

## 文档

更多安装指南、教程和API请参考[用户文档](https://gitee.com/opengauss/docs)。

## 社区

### 治理

查看openGauss是如何实现开放[治理](https://gitee.com/opengauss/community/blob/master/governance.md)。

### 交流

- WeLink：开发者的交流平台。
- IRC频道：`#opengauss-meeting`（仅用于会议纪要）。
- 邮件列表：https://opengauss.org/zh/community/onlineCommunication.html

## 贡献

欢迎大家来参与贡献。详情请参阅我们的[社区贡献](https://opengauss.org/zh/contribution.html)。

## 发行说明

请参见[发行说明](https://opengauss.org/zh/docs/2.0.0/docs/Releasenotes/Releasenotes.html)。

## 许可证

[MulanPSL-2.0](http://license.coscl.org.cn/MulanPSL2/)