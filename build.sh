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

BUILD_FAILED=1
java_path=""
ant_path=""
JDBC_DIR=$(dirname $(readlink -f $0))
LOG_FILE=$JDBC_DIR/logfile
THIRD_DIR=$JDBC_DIR/buildtools
libs=$JDBC_DIR/libs
NOTICE_FILE='Copyright Notice.doc'
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
else
    echo "Only support EulerOS, OPENEULER(aarch64) and CentOS platform."
    echo "Kernel is $kernel"
    exit 1
fi

export PLAT_FORM_STR=$(sh "${JDBC_DIR}/get_PlatForm_str.sh")
declare install_package_format='tar'
declare mppdb_version='GaussDB Kernel'
declare mppdb_name_for_package="$(echo ${mppdb_version} | sed 's/ /-/g')"
declare version_number='V500R001C20'
declare version_string="${mppdb_name_for_package}-${version_number}"
declare package_pre_name="${version_string}-${dist_version}-${PLATFORM}bit"
declare jdbc_package_name="${package_pre_name}-Jdbc.${install_package_format}.gz"

coretype=$(uname -p)
mvn_name="apache-maven-3.6.3-bin.tar.gz"
jdk_name="OpenJDK8U-jdk_x64_linux_hotspot_8u222b10.tar.gz"

if [ X"$coretype" == X"aarch64" ]; then
    jdk_name="OpenJDK8U-jdk_aarch64_linux_hotspot_8u222b10.tar.gz"
fi
    
tar -zxvf buildtools/$jdk_name -C buildtools/ > /dev/null
mkdir -p buildtools/maven
tar -zxvf buildtools/$mvn_name -C buildtools/maven/ > /dev/null

die()
{
    echo "ERROR: $@"
    exit $BUILD_FAILED
}

function prepare_java_env()
{
    echo "Prepare the build enviroment."
    export JAVA_HOME=$THIRD_DIR/jdk8u222-b10
    export JRE_HOME=$JAVA_HOME/jre
    export LD_LIBRARY_PATH=$JRE_HOME/lib/amd64/server:$LD_LIBRARY_PATH
    export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH	
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
    export MAVEN_HOME=$THIRD_DIR/maven/apache-maven-3.6.3/
    export PATH=$MAVEN_HOME/bin:$PATH
    MAVEN_VERSION=`mvn -v 2>&1 | awk '/Apache Maven / {print $3}'`
    echo maven version is $MAVEN_VERSION
}
function install_jdbc()
{
    export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
    export COMMIT=$(git rev-parse --short HEAD)
    export OPENGAUSS_PACKAGE_NAME="org.opengauss";

    export GS_VERSION="compiled at $(date +%Y-%m-%d-%H:%M:%S) build ${COMMIT}"
    export OUTPUT_DIR="${JDBC_DIR}/output"
    echo "Begin make jdbc..."
    export CLASSPATH=".:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar"
    echo ${JDBC_DIR}
    cd "${JDBC_DIR}/shade"
    mvn clean install -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    cd  "${JDBC_DIR}/shade/target"
    jar -xf demo-0.0.1-SNAPSHOT.jar
    mkdir "${JDBC_DIR}/shade/temp/"
    cp -r ./com "${JDBC_DIR}/shade/temp/"
    cd "${JDBC_DIR}/shade/temp"
    find ./com -name "*" | sort |xargs zip demo-0.0.1-SNAPSHOT.jar >> "$LOG_FILE" 2>&1
    mvn install:install-file -Dfile=${JDBC_DIR}/shade/temp/demo-0.0.1-SNAPSHOT.jar -DgroupId=com.huawei -DartifactId=demo-0.0.1-SNAPSHOT -Dversion=0.0.1 -Dpackaging=jar
    if [ $? -ne 0 ]; then
        die "mvn failed."
    fi
    rm -rf "${JDBC_DIR}/jdbc"
    cp "${JDBC_DIR}/pgjdbc" "${JDBC_DIR}/jdbc" -r
    cd "${JDBC_DIR}/jdbc"
    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
    mvn clean install -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "mvn failed."
    fi
    echo ${OUTPUT_DIR}
    if [ ! -d "${OUTPUT_DIR}" ]; then
        mkdir ${OUTPUT_DIR}
    fi
    cd ${OUTPUT_DIR}
    rm -rf *.jar
    version=`awk '/<version>[^<]+<\/version>/{gsub(/<version>|<\/version>/,"",$1);print $1;exit;}' ${JDBC_DIR}/jdbc/pom.xml`
    mv ${JDBC_DIR}/jdbc/target/opengauss-jdbc-${version}.jar ./postgresql.jar
    echo "Successfully make postgresql.jar"

    rm -rf "${JDBC_DIR}/jdbc"
    cp "${JDBC_DIR}/pgjdbc" "${JDBC_DIR}/jdbc" -r
    cd "${JDBC_DIR}/jdbc"
    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
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

    mvn clean install -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    cp ${JDBC_DIR}/jdbc/target/opengauss-jdbc-${version}.jar ${OUTPUT_DIR}/
    echo "Successfully make opengauss-jdbc jar package"

    cd ${OUTPUT_DIR}/
    tar -zcvf ${JDBC_DIR}/openGauss-${version}-JDBC.tar.gz *.jar
    echo "Successfully make jdbc jar package"
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
    cp ${JDBC_DIR}/"${NOTICE_FILE}" ./

    select_package_command

    echo "packaging jdbc..."
    $package_command "${jdbc_package_name}"  ./gsjdbc4.jar "${NOTICE_FILE}" >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "$package_command ${jdbc_package_name} failed" 
    fi
    cp "${jdbc_package_name}" ../
    echo "$pkgname tools is ${jdbc_package_name} of ${JDBC_DIR} directory " >> "$LOG_FILE" 2>&1
    echo "success!"
}
function registerJars()
{
     for src in `find $third_part_lib -name '*.jar'`
     do
        cp $src $libs/
     done
     echo "copy finished"
     cd $libs
     prepare_env
     mvn install:install-file -Dfile=./commons-logging-1.2.jar -DgroupId=commons-logging -DartifactId=commons-logging -Dversion=1.2 -Dpackaging=jar
     mvn install:install-file -Dfile=./commons-codec-1.11.jar -DgroupId=commons-codec -DartifactId=commons-codec -Dversion=1.11 -Dpackaging=jar
     mvn install:install-file -Dfile=./httpclient-4.5.13.jar  -DgroupId=org.apache.httpcomponents -DartifactId=httpclient -Dversion=4.5.13 -Dpackaging=jar
     mvn install:install-file -Dfile=./httpcore-4.4.13.jar  -DgroupId=org.apache.httpcomponents -DartifactId=httpcore -Dversion=4.4.13 -Dpackaging=jar
     mvn install:install-file -Dfile=./fastjson-1.2.70.jar  -DgroupId=com.alibaba -DartifactId=fastjson -Dversion=1.2.70 -Dpackaging=jar
     mvn install:install-file -Dfile=./joda-time-2.10.6.jar -DgroupId=joda-time -DartifactId=joda-time -Dversion=2.10.6 -Dpackaging=jar
     mvn install:install-file -Dfile=./jackson-databind-2.11.2.jar -DgroupId=com.fasterxml.jackson.core -DartifactId=jackson-databind -Dversion=2.11.2 -Dpackaging=jar
     mvn install:install-file -Dfile=./jackson-core-2.11.2.jar -DgroupId=com.fasterxml.jackson.core -DartifactId=jackson-core -Dversion=2.11.2 -Dpackaging=jar
     mvn install:install-file -Dfile=./jackson-annotations-2.11.2.jar -DgroupId=com.fasterxml.jackson.core  -DartifactId=jackson-annotations -Dversion=2.11.2 -Dpackaging=jar
     mvn install:install-file -Dfile=./slf4j-api-1.7.30.jar -DgroupId=org.slf4j  -DartifactId=slf4j-api -Dversion=1.7.30 -Dpackaging=jar
     mvn install:install-file -Dfile=./java-sdk-core-3.0.12.jar -DgroupId=com.huawei.apigateway  -DartifactId=hw-java-sdk-core -Dversion=3.0.12 -Dpackaging=jar     
     mvn install:install-file -Dfile=./bcprov-jdk15on-1.68.jar -DgroupId=org.bouncycastle  -DartifactId=bcprov-jdk15on -Dversion=1.68 -Dpackaging=jar
}
prepare_env
export third_part_lib=""
if [ ! -d "${libs}" ]; then
mkdir ${libs}
fi
case $1 in
   -3rd | --3rd)
     if [ ! -n "$2" ]; then
        die "3rd should not be empty"
     fi
     third_part_lib="$2"
     registerJars
     ;;
   *);;
esac
install_jdbc
clean
echo "now, all packages has finished!"
exit 0
