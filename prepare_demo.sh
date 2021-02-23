cd shade
mvn clean install -Dmaven.test.skip=true
rm -rf temp
mkdir temp
cp target/demo-0.0.1-SNAPSHOT.jar ./temp/
cd temp
jar -xf demo-0.0.1-SNAPSHOT.jar 
find ./com -name "*" | sort | xargs zip demo-0.0.1-SNAPSHOT_new.jar 
mvn install:install-file -Dfile=./demo-0.0.1-SNAPSHOT_new.jar -DgroupId=com.huawei -DartifactId=demo-0.0.1-SNAPSHOT -Dversion=0.0.1 -Dpackaging=jar
