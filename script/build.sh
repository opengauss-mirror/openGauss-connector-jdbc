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
java_path=""
ant_path=""
JDBC_DIR="$(dirname $(readlink -f $0))/../../openGauss-connector-jdbc"
LOG_FILE=$JDBC_DIR/logfile
THIRD_DIR="$(dirname $(readlink -f $0))/../../../.."
ARCH=$(uname -m)
MAVEN_SETTINGS=$JDBC_DIR/buildtools/settings.xml
libs=$JDBC_DIR/libs
#detect platform information.
PLATFORM=32
bit=$(getconf LONG_BIT)
if [ "$bit" -eq 64 ]; then
   PLATFORM=64
fi
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
    kernel=$(lsb_release -d | awk -F ' ' '{print $2}'| tr A-Z a-z)
    version=$(lsb_release -r | awk -F ' ' '{print $2}')
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
else
    echo "Only support EulerOS, OPENEULER(aarch64), SUSE, and CentOS platform."
    echo "Kernel is $kernel"
    exit 1
fi

declare install_package_format='tar'
declare mppdb_version='GaussDB Kernel'
declare mppdb_name_for_package="$(echo ${mppdb_version} | sed 's/ /-/g')"
declare version_number='V500R002C10'
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
    echo "Prepare the build enviroment."
    export JAVA_HOME=$THIRD_DIR/platform/huaweijdk8/${ARCH}/jdk
    export JRE_HOME=$JAVA_HOME/jre
    export LD_LIBRARY_PATH=$JRE_HOME/lib/amd64/server:$LD_LIBRARY_PATH
    export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo java version is $JAVA_VERSION
}

function prepare_env()
{
    prepare_java_env
}

function install_jdbc()
{
    export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
    export HUAWEI_JDBC_PACKAGE_NAME='com.huawei.gauss200.jdbc'
    export HUAWEI_JDBC_PACKAGE_DIR=$(echo $HUAWEI_JDBC_PACKAGE_NAME | sed 's#\.#/#g')
    export HUAWEI_OPENGAUSSJDBC_PACKAGE_NAME='com.huawei.opengauss.jdbc'
    export HUAWEI_OPENGAUSSJDBC_PACKAGE_DIR=$(echo $HUAWEI_JDBC_PACKAGE_NAME | sed 's#\.#/#g')
    export COMMIT=$(git rev-parse --short HEAD)
    export GS_VERSION="compiled at $(date +%Y-%m-%d-%H:%M:%S) build ${COMMIT}"
    export OUTPUT_DIR="${JDBC_DIR}/output"
    echo "Begin make jdbc..."
    export CLASSPATH=".:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar"
    echo ${JDBC_DIR}
    rm -rf "${JDBC_DIR}/jdbc"
    cp "${JDBC_DIR}/pgjdbc" "${JDBC_DIR}/jdbc" -r
    cd "${JDBC_DIR}/jdbc"
    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
    mvn clean install --settings ${MAVEN_SETTINGS} -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "mvn failed."
    fi
    echo ${OUTPUT_DIR}
    if [ ! -d "${OUTPUT_DIR}" ]; then
        mkdir ${OUTPUT_DIR}
    fi
    mkdir ${JDBC_DIR}/jdbc/target/tmp
    mv ${JDBC_DIR}/jdbc/target/opengauss-jdbc-2.0.0.jar ${JDBC_DIR}/jdbc/target/tmp
    cd ${JDBC_DIR}/jdbc/target/tmp
    jar -xvf opengauss-jdbc-2.0.0.jar
    rm -rf opengauss-jdbc-2.0.0.jar
    zip  -r opengauss-jdbc-2.0.0.jar ./ >> "$LOG_FILE" 2>&1
    mv ${JDBC_DIR}/jdbc/target/tmp/opengauss-jdbc-2.0.0.jar ${OUTPUT_DIR}/gsjdbc4.jar
    rm -rf "${JDBC_DIR}/jdbc"
    cp  "${JDBC_DIR}/pgjdbc"  "${JDBC_DIR}/jdbc" -r
    cd "${JDBC_DIR}/jdbc"
    echo "Successfully make gsjdbc4.jar"
    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
    find -name '*.java' -type f | xargs sed -i "s#org\.postgresql#${HUAWEI_JDBC_PACKAGE_NAME}#g"
    find . -name 'Driver.java' | xargs sed -i "s/jdbc:postgresql:/jdbc:gaussdb:/g"
    if [ $? -ne 0 ]; then
      die "failed to replace url name."
    fi
    find . -name 'BaseDataSource.java' | xargs sed -i "s/jdbc:postgresql:/jdbc:gaussdb:/g"
    if [ $? -ne 0 ]; then
      die "failed to replace url name."
    fi
    mvn clean install --settings ${MAVEN_SETTINGS} -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "mvn failed."
    fi
    mkdir ${JDBC_DIR}/jdbc/target/tmp
    mv ${JDBC_DIR}/jdbc/target/opengauss-jdbc-2.0.0.jar ${JDBC_DIR}/jdbc/target/tmp
    cd ${JDBC_DIR}/jdbc/target/tmp
    jar -xvf opengauss-jdbc-2.0.0.jar
    rm -rf opengauss-jdbc-2.0.0.jar
    zip  -r opengauss-jdbc-2.0.0.jar ./ >> "$LOG_FILE" 2>&1
    mv ${JDBC_DIR}/jdbc/target/tmp/opengauss-jdbc-2.0.0.jar ${OUTPUT_DIR}/gsjdbc200.jar
    rm -rf "${JDBC_DIR}/jdbc"
    cp "${JDBC_DIR}/pgjdbc"  "${JDBC_DIR}/jdbc" -r
    cd "${JDBC_DIR}/jdbc"
    echo "Successfully make gsjdbc200.jar"
    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
    find . -name '*.java' -type f | xargs sed -i "s#org\.postgresql#${HUAWEI_OPENGAUSSJDBC_PACKAGE_NAME}#g"
    find . -name 'Driver.java' | xargs sed -i "s/jdbc:postgresql:/jdbc:opengauss:/g"
    if [ $? -ne 0 ]; then
      die "failed to replace url name."
    fi
    find . -name 'BaseDataSource.java' | xargs sed -i "s/jdbc:postgresql:/jdbc:opengauss:/g"
    if [ $? -ne 0 ]; then
      die "failed to replace url name."
    fi
    mvn clean install --settings ${MAVEN_SETTINGS} -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
      die "mvn failed."
    fi
    mkdir ${JDBC_DIR}/jdbc/target/tmp
    mv ${JDBC_DIR}/jdbc/target/opengauss-jdbc-2.0.0.jar ${JDBC_DIR}/jdbc/target/tmp
    cd ${JDBC_DIR}/jdbc/target/tmp
    jar -xvf opengauss-jdbc-2.0.0.jar
    rm -rf opengauss-jdbc-2.0.0.jar
    zip -r opengauss-jdbc-2.0.0.jar ./ >> "$LOG_FILE" 2>&1
    mv ${JDBC_DIR}/jdbc/target/tmp/opengauss-jdbc-2.0.0.jar ${OUTPUT_DIR}/opengaussjdbc.jar
    echo "Successfully make opengaussjdbc.jar"
}

function clean()
{
    if [ -d "${JDBC_DIR}/shade/temp" ]; then
        rm -rf "${JDBC_DIR}/shade/temp"
    fi
    if [ -d "${JDBC_DIR}/shade/target" ]; then
        rm -rf "${JDBC_DIR}/shade/target"
    fi
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
    $package_command "${jdbc_package_name}" ./gsjdbc200.jar ./gsjdbc4.jar ./opengaussjdbc.jar  >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${jdbc_package_name} failed"
    fi
    cd $JDBC_DIR
    cp ./output/"${jdbc_package_name}" ../../../output
    echo "$pkgname tools is ${jdbc_package_name} of ${JDBC_DIR} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}
function register_jars()
{
     cd $THIRD_PART_LIB/common/slf4j
     cp *.jar $libs

     cd $libs
     prepare_env
     mvn install:install-file -Dfile=./slf4j-api-1.7.30.jar -DgroupId=org.slf4j  -DartifactId=slf4j-api -Dversion=1.7.30 -Dpackaging=jar     
}
prepare_env
export THIRD_PART_LIB=""
if [ ! -d "${libs}" ]; then
mkdir ${libs}
fi
case "$1" in
   -3rd | --3rd)
     if [ ! -n "$2" ]; then
        die "3rd should not be empty"
     fi
     THIRD_PART_LIB="$2"
     register_jars
     ;;
   *);;
esac
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
