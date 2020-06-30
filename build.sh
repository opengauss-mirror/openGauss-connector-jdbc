
BUILD_FAILED=1
java_path=""
ant_path=""
JDBC_DIR=$(dirname $(readlink -f $0))
LOG_FILE=$JDBC_DIR/logfile
THIRD_DIR=$JDBC_DIR/buildtools
PLATFORM=""

die()
{
    echo "ERROR: $@"
    exit $BUILD_FAILED
}

function get_platform()
{
	echo "Get the platform"
	PLATFORM=`uname -i`
	echo "Current platform is" $PLATFORM
}

function prepare_x86_java_env()
{
    echo "Prepare the build enviroment."
	cd $THIRD_DIR
	if [ ! -d $THIRD_DIR/jdk8u222-b10 ]; then
		tar -xf OpenJDK8U-jdk_x64_linux_hotspot_8u222b10.tar.gz
		if [ $? -ne 0 ]; then
			echo "Failed to uncompress jdk."
			exit 1
		fi
	fi
	export JAVA_HOME=$THIRD_DIR/jdk8u222-b10
	export JRE_HOME=$JAVA_HOME/jre
	export LD_LIBRARY_PATH=$JRE_HOME/lib/amd64/server:$LD_LIBRARY_PATH
	export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
	
	JAVA_VERSION=`java -version 2>&1 | awk -F '"' '/version/ {print $2}'`
	echo java version is $JAVA_VERSION
}

function prepare_arm_java_env()
{
    echo "Prepare the build enviroment."
	cd $THIRD_DIR
	if [ ! -d $THIRD_DIR/jdk8u222-b10 ]; then
		tar -xf OpenJDK8U-jdk_aarch64_linux_hotspot_8u222b10.tar.gz
		if [ $? -ne 0 ]; then
			echo "Failed to uncompress jdk."
			exit 1
		fi
	fi
	export JAVA_HOME=$THIRD_DIR/jdk8u222-b10
	export JRE_HOME=$JAVA_HOME/jre
	export LD_LIBRARY_PATH=$JRE_HOME/lib/aarch64/server:$LD_LIBRARY_PATH
	export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$PATH
	
	JAVA_VERSION=`java -version 2>&1 | awk -F '"' '/version/ {print $2}'`
	echo java version is $JAVA_VERSION
}

function prepare_env()
{
	if [ "$PLATFORM" = "x86_64" ]; then
		prepare_x86_java_env	
	elif [ "$PLATFORM" = "aarch64" ]; then
		prepare_arm_java_env
	else
	    die "Failed to get platform with command uname -i."
	fi
	prepare_maven_env		
}

function prepare_maven_env()
{
    echo "Prepare the build enviroment."
	cd $THIRD_DIR
	if [ ! -d $THIRD_DIR/apache-maven-3.6.3 ]; then
		tar -xf apache-maven-3.6.3-bin.tar.gz
		if [ $? -ne 0 ]; then
			echo "Failed to uncompress ant."
			exit 1
		fi
	fi

	export MAVEN_HOME=$THIRD_DIR/apache-maven-3.6.3
	export PATH=$MAVEN_HOME/bin:$PATH
	MAVEN_VERSION=`mvn -v 2>&1 | awk '/Apache Maven / {print $3}'`
	echo maven version is $MAVEN_VERSION
}
function install_jdbc()
{
    export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"

    echo "Begin make jdbc..."
    export CLASSPATH=".:${JAVA_HOME}/lib/dt.jar:${JAVA_HOME}/lib/tools.jar"

    cd "${JDBC_DIR}"	
    mvn clean install -Dmaven.test.skip=true >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        die "mvn failed."
    fi
	mv pgjdbc/target/postgresql-42.2.5.jar postgresql.jar
    echo "Successfully make jdbc. The package is postgresql.jar"
}

function clean()
{
	rm -rf "${JDBC_DIR}/build" "${THIRD_DIR}/apache-maven-3.6.3" "${THIRD_DIR}/jdk8u222-b10"
	if [ $? -ne 0 ]; then
        die "Failed to remove folder."
	fi
}

get_platform
prepare_env
install_jdbc
clean
