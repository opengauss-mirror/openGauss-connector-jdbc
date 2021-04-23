echo begin run
mkdir libs
for src in `find open_source -name '*.jar'`
do
   cp $src ./libs/
done
mvn install:install-file -Dfile=./libs/commons-logging-1.2.jar -DgroupId=commons-logging -DartifactId=commons-logging -Dversion=1.2 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/commons-codec-1.11.jar -DgroupId=commons-codec -DartifactId=commons-codec -Dversion=1.11 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/httpclient-4.5.13.jar  -DgroupId=org.apache.httpcomponents -DartifactId=httpclient -Dversion=4.5.13 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/httpcore-4.4.13.jar  -DgroupId=org.apache.httpcomponents -DartifactId=httpcore -Dversion=4.4.13 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/fastjson-1.2.70.jar  -DgroupId=com.alibaba -DartifactId=fastjson -Dversion=1.2.70 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/joda-time-2.10.6.jar -DgroupId=joda-time -DartifactId=joda-time -Dversion=2.10.6 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/jackson-databind-2.11.2.jar -DgroupId=com.fasterxml.jackson.core -DartifactId=jackson-databind -Dversion=2.11.2 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/jackson-core-2.11.2.jar -DgroupId=com.fasterxml.jackson.core -DartifactId=jackson-core -Dversion=2.11.2 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/jackson-annotations-2.11.2.jar -DgroupId=com.fasterxml.jackson.core  -DartifactId=jackson-annotations -Dversion=2.11.2 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/slf4j-api-1.7.30.jar -DgroupId=org.slf4j  -DartifactId=slf4j-api -Dversion=1.7.30 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/java-sdk-core-3.0.12.jar -DgroupId=com.huawei.apigateway  -DartifactId=hw-java-sdk-core -Dversion=3.0.12 -Dpackaging=jar
mvn install:install-file -Dfile=./libs/bcprov-jdk15on-1.68.jar -DgroupId=org.bouncycastle  -DartifactId=bcprov-jdk15on -Dversion=1.68 -Dpackaging=jar
