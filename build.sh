#!/bin/bash
#############################################################################
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#   http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
# Description  : shell script for jdbc package.
#############################################################################
set -e

BUILD_FAILED=1
JDBC_DIR=$(dirname $(readlink -f $0))
LOG_FILE=$JDBC_DIR/logfile
ARCH=$(uname -m)
#detect platform information.
PLATFORM=32
bit=$(getconf LONG_BIT)
if [ "$bit" -eq 64 ]; then
   PLATFORM=64
fi
PKG_VERSION=3.0.5
#get OS distributed version.
kernel=""
version=""
if [ -f "/etc/euleros-release" ]; then
    kernel=$(cat /etc/euleros-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/euleros-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/openEuler-release" ]; then
    kernel=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/openEuler-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/centos-release" ]; then
    kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/centos-release | awk -F '(' '{print $2}'| awk -F ')' '{print $1}' | tr A-Z a-z)
elif [ -f "/etc/kylin-release" ]; then
    kernel=$(cat /etc/kylin-release | awk -F ' ' '{print $1}' | tr A-Z a-z)
    version=$(cat /etc/kylin-release | awk '{print $6}' | tr A-Z a-z)
else
    if [ "Darwin" == `uname -s` ]; then
        kernel="Darwin"
    else
        kernel=$(lsb_release -d | awk -F ' ' '{print $2}'| tr A-Z a-z)
        version=$(lsb_release -r | awk -F ' ' '{print $2}')
    fi
fi

if [ X"$kernel" == X"euleros" ]; then
    dist_version="EULER"
elif [ X"$kernel" == X"centos" ]; then
    dist_version="CENTOS"
elif [ X"$kernel" == X"openeuler" ]; then
    dist_version="OPENEULER"
elif [ X"$kernel" == X"kylin" ]; then
    dist_version="KYLIN"
elif [ X"$kernel" = X"suse" ]; then
    dist_version="SUSE"
elif [ X"$kernel" = X"redflag" ]; then
    dist_version="Asianux"
elif [ X"$kernel" = X"asianux" ]; then
    dist_version="Asianux"
elif [ X"$kernel" = X"Darwin" ]; then
    dist_version="Darwin"
else
    echo "WARN:Only EulerOS, OPENEULER(aarch64), SUSE, CentOS and Asianux platform support, there will set to UNKNOWN"
    dist_version="UNKNOWN"
fi

declare install_package_format='tar'
declare mppdb_name_for_package="openGauss"
pom_version_number=`awk '/<version>[^<]+<\/version>/{gsub(/<version>|<\/version>/,"",$1);print $1;exit;}' ${JDBC_DIR}/pgjdbc/pom.xml`
declare version_number="${pom_version_number}"
declare version_string="${mppdb_name_for_package}-${version_number}"
declare package_pre_name="${version_string}-${dist_version}-${PLATFORM}bit"
declare jdbc_package_name="${package_pre_name}-Jdbc.${install_package_format}.gz"

die()
{
    echo "ERROR: $@"
    exit $BUILD_FAILED
}

function prepare_java_env()
{
    echo "We no longer provide java, please makesure java(1.8*) already in PATH!"
    JAVA_VERSION=`java -version 2>&1 | awk -F '"' '/version/ {print $2}'`
    echo java version is $JAVA_VERSION
}

function prepare_env()
{
    prepare_java_env
    prepare_maven_env
}

function prepare_maven_env()
{
    echo "We no longer provide mvn, please makesure mvn(3.6.0+) already in PATH!"
    MAVEN_VERSION=`mvn -v 2>&1 | awk '/Apache Maven / {print $3}'`
    echo maven version is $MAVEN_VERSION
}

function install_jdbc()
{
    export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
    export COMMIT=$(git rev-parse --short HEAD)
    export OPENGAUSS_PACKAGE_NAME="org.opengauss";

    export GS_VERSION="openGauss-JDBC ${PKG_VERSION} build ${COMMIT} compiled at $(date '+%Y-%m-%d %H:%M:%S')"
    export OUTPUT_DIR="${JDBC_DIR}/output"
    echo "Begin make jdbc..."
    export CLASSPATH=".:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar"
    echo ${JDBC_DIR}
    rm -rf "${JDBC_DIR}/jdbc"
    cp -r "${JDBC_DIR}/pgjdbc" "${JDBC_DIR}/jdbc"
    cd "${JDBC_DIR}/jdbc"
    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
    find . -name 'DriverInfo.java' | xargs sed -i "s|DRIVER_VERSION = .*;|DRIVER_VERSION = \"${pom_version_number}\";|g"
    mvn clean install -Dgpg.skip -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "mvn install driver failed."
    fi
    echo ${OUTPUT_DIR}
    if [ ! -d "${OUTPUT_DIR}" ]; then
        mkdir ${OUTPUT_DIR}
    fi
    cd ${OUTPUT_DIR}
    rm -rf *.jar *.gz
    version=`awk '/<version>[^<]+<\/version>/{gsub(/<version>|<\/version>/,"",$1);print $1;exit;}' ${JDBC_DIR}/jdbc/pom.xml`
    mv ${JDBC_DIR}/jdbc/target/opengauss-jdbc-${version}.jar ./postgresql.jar
    echo "Successfully make postgresql.jar"

#    rm -rf "${JDBC_DIR}/jdbc"
#    cp "${JDBC_DIR}/pgjdbc" "${JDBC_DIR}/jdbc" -r
    cd "${JDBC_DIR}/jdbc"
#    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
    find . -name 'Driver.java' | xargs sed -i "s/jdbc:postgresql:/jdbc:opengauss:/g"
    find . -name 'java.sql.Driver' | xargs sed -i "s#org\.postgresql#${OPENGAUSS_PACKAGE_NAME}#g"
    find . -name '*.java' -type f | xargs sed -i "s#org\.postgresql#${OPENGAUSS_PACKAGE_NAME}#g"
    if [ $? -ne 0 ]; then
      die "failed to replace url name"
    fi
    find . -name 'BaseDataSource.java' | xargs sed -i "s/jdbc:postgresql:/jdbc:opengauss:/g"
    if [ $? -ne 0 ]; then
      die "fail to replace url name in BaseDataSource"
    fi

    mvn clean install -Dgpg.skip -Dmaven.test.skip=true -U >> "$LOG_FILE" 2>&1
    cp ${JDBC_DIR}/jdbc/target/opengauss-jdbc-${version}.jar ${OUTPUT_DIR}/
    echo "Successfully make opengauss-jdbc-${version} jar package"
}

function clean()
{
    if [ -d "${JDBC_DIR}/jdbc" ]; then
        rm -rf "${JDBC_DIR}/jdbc"
    fi
    if [ -f "${LOG_FILE}" ]; then
        rm -rf "${LOG_FILE}"
    fi
}

function select_package_command()
{

    case "$install_package_format" in
        tar)
            tar='tar'
            option=' -zcvf'
            package_command="$tar$option"
            ;;
        rpm)
            rpm='rpm'
            option=' -i'
            package_command="$rpm$option"
            ;;
    esac
}

function make_package()
{
    cd ${JDBC_DIR}/output
    select_package_command

    echo "packaging jdbc..."
    $package_command "${jdbc_package_name}"  *.jar >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${jdbc_package_name} failed"
    fi
    cp "${jdbc_package_name}" ../
    echo "$package_command tools is ${jdbc_package_name} of ${JDBC_DIR} directory " >> "$LOG_FILE" 2>&1
    echo "Successfully make jdbc jar package in ${jdbc_package_name}"
}

prepare_env
install_jdbc
make_package
if [ "$1" = "-n" ] ;then
    echo "the temporary directory has not been cleaned up, please clean up by yourself!"
else
    echo "clean up temporary directory!"
    clean
fi
echo "now, all packages has finished!"
exit 0
