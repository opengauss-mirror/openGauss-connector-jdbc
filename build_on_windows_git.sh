#/bin/bash
# this script is to use in windows git bash shell to build opengauss jdbc jar.
# you need build tools in window's PATH below:
# java mvn zip tar xargs 
# after success build, the tar package in openGauss-2.0.0-JDBC.tar.gz
# or you can int output dir find postgresql.jar and opengauss-jdbc*.jar
# please notice: postgresql.jar is conflict with postgres database jdbc.
# if you want to compatibiliry use opengauss and pg database, use opengauss-jdbc*.jar instead.
# the driver string is jdbc:opengauss://, the driver class is:org.opengauss.Driver
JDBC_DIR=$(dirname $(readlink -f $0))
cd $JDBC_DIR
sh prepare_maven.sh
cd $JDBC_DIR
sh prepare_demo.sh
cd $JDBC_DIR
sh prepare_windows_build.sh
