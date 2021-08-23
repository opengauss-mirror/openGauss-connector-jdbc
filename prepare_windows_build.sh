#/bin/bash
echo begin run
BUILD_FAILED=1
die()
{
    echo "ERROR: $@"
    exit $BUILD_FAILED
}

JDBC_DIR=$(dirname $(readlink -f $0))
LOG_FILE=$JDBC_DIR/logfile
export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"
export COMMIT=$(git rev-parse --short HEAD)
export OPENGAUSS_PACKAGE_NAME="org.opengauss";

export GS_VERSION="compiled at $(date +%Y-%m-%d-%H:%M:%S) build ${COMMIT}"
export OUTPUT_DIR="${JDBC_DIR}/output"
export CLASSPATH=".:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar"

make_output_dir()
{
    if [ ! -d "${OUTPUT_DIR}" ]; then
        mkdir ${OUTPUT_DIR}
    fi
    cd ${OUTPUT_DIR}
    rm -rf *.jar
}

build_postgres()
{
    cd ${JDBC_DIR}/
    rm -rf "${JDBC_DIR}/jdbc"
    cp "${JDBC_DIR}/pgjdbc" "${JDBC_DIR}/jdbc" -r
    cd "${JDBC_DIR}/jdbc"
    find . -name 'Driver.java' | xargs sed -i "s/@GSVERSION@/${GS_VERSION}/g"
    mvn clean install -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "mvn failed."
    fi
    version=`awk '/<version>[^<]+<\/version>/{gsub(/<version>|<\/version>/,"",$1);print $1;exit;}' ${JDBC_DIR}/jdbc/pom.xml`
    mv ${JDBC_DIR}/jdbc/target/opengauss-jdbc-${version}.jar ${OUTPUT_DIR}/postgresql.jar
    echo "Successfully make postgresql.jar package in ${OUTPUT_DIR}/postgresql.jar"
}

build_opengauss()
{
    cd ${JDBC_DIR}
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
    
    version=`awk '/<version>[^<]+<\/version>/{gsub(/<version>|<\/version>/,"",$1);print $1;exit;}' ${JDBC_DIR}/jdbc/pom.xml`
    mvn clean install -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    cp ${JDBC_DIR}/jdbc/target/opengauss-jdbc-${version}.jar ${OUTPUT_DIR}/
    echo "Successfully make opengauss-jdbc jar package in ${OUTPUT_DIR}/opengauss-jdbc-${version}.jar"
}

tar_output()
{
    cd ${OUTPUT_DIR}/
    tar -zcvf ${JDBC_DIR}/openGauss-${version}-JDBC.tar.gz *.jar 2>&1 1>null
    echo "Successfully make jdbc jar package in ${JDBC_DIR}/openGauss-${version}-JDBC.tar.gz"
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

make_output_dir
build_postgres
build_opengauss
tar_output
clean
