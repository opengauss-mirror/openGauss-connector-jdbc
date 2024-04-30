# Test

1. 配置文件里配置3个兼容属性数据库. A/B/PG
2. 单元测试里分别使用

    | DBCOMPATIBILITY | BaseTest4        | 说明                     |
    |----------------|------------------|------------------------|
    | A              | BaseTest4.java   | A模式继承 /TestUtil.openDB |
    | PG             | BaseTest4PG.java | PG模式继承 TestUtil.openDBPG |
    | B              | BaseTest4B.java  | B模式继承 TestUtil.openDBB |

3. 增加版本断言. 针对需要至少那个版本才能运行
    
    ```java
    // assumeMiniOgVersion
    public void setUp() throws Exception {
        super.setUp();
        assumeMiniOgVersion("opengauss 6.0.0",6,0,0);
    }
    ```

## create database

创建A/PG/B三种兼容模式的数据库

```shell
gsql 

create database jdbc_utf8_a ENCODING='utf8' DBCOMPATIBILITY='A';

create database jdbc_utf8_pg ENCODING='utf8' DBCOMPATIBILITY='PG';

create database jdbc_utf8_b ENCODING='utf8' DBCOMPATIBILITY='B';

\c jdbc_utf8_a

\c jdbc_utf8_pg

\c jdbc_utf8_b

```

## create user

```shell
gsql -r postgres
create user jdbc with password 'jdbc@123' sysadmin;
```

## pg_hba.conf

数据库密码认证方式为sha256

```shell
host    all             jdbc    0.0.0.0/0       sha256
host    replication     jdbc        0.0.0.0/0            sha256
```

## config build.properties

```shell
cp build.properties build.local.properties

server=localhost
port=5432
database_a=jdbc_utf8_a
database_pg=jdbc_utf8_pg
database_b=jdbc_utf8_b

```


## TODO

1. [clusterhealthy](pgjdbc%2Fsrc%2Ftest%2Fjava%2Forg%2Fpostgresql%2Fclusterhealthy) 先忽略
2. GetObject310Test.testGetLocalDateTime 注释部分测试数据
3. SetObject310Test.getZoneIdsToTest 注释部分测试数据
4. time 类似数据问题                                                                        需要解决
   1. GetObject310Test.testGetLocalTime  数据问题,需要修复
   2. SetObject310Test.testSetLocalTimeAndReadBack --> time without zone 
   3. testLocalTimeMax
   4. testTimeStampRounding
   5. testTimeStampRoundingWithType
   6. org.postgresql.test.dolphintest.TimeTest
   7. org.postgresql.test.jdbc2.TimeTest#testGetTimeZone
5. MultiHostsConnectionTest/ReadWriteSplittingConnectionTest/TlcpTest                     ignore
6. LoadBalanceHeartBeatingTest/ClusterTest/ConnectionInfoTest/ConnectionManagerTest       ignore
7. checkDnStateWithPropertiesConnectionFailedTest                                         ignore
8. B模式的测试用例 move 到 dolphintest](pgjdbc%2Fsrc%2Ftest%2Fjava%2Forg%2Fpostgresql%2Ftest%2Fdolphintest)
9. org.postgresql.test.jdbc4.ArrayTest   boll[]数据解析问题. PG已解决                        ignore
10. bool/bit                                                                              需要解决
    1. org.postgresql.test.jdbc3.TypesTest#testCallableBoolean
         registerBool/但是强改成了bit
         case Types.BOOLEAN:
         sqlType = Types.BIT;
         break;
    2. org.postgresql.test.jdbc3.Jdbc3CallableStatementTest#testGetBoolean01
    3. org.postgresql.test.jdbc3.Jdbc3CallableStatementTest#testInOut
    4. org.postgresql.test.jdbc2.ResultSetTest#testBooleanInt
    5. org.postgresql.test.jdbc2.ResultSetTest#testgetBadBoolean
    6. org.postgresql.test.jdbc2.ResultSetTest#testBooleanString
    7. org.postgresql.test.jdbc2.ResultSetTest#testGetBadUuidBoolean
    8. org.postgresql.test.jdbc2.PreparedStatementTest#testBadBoolean
    9. org.postgresql.test.jdbc2.PreparedStatementTest#testBoolean()
    10. org.postgresql.test.jdbc2.PgCallableStatementTest#testCommonTypesOutParam
11. org.postgresql.test.jdbc2.optional.ConnectionPoolTest#testBackendIsClosed               --fix
12. org.postgresql.test.jdbc2.DriverTest#testSetLogStream                                   ignore
13. org.postgresql.test.jdbc2.DriverTest#testSetLogWriter                                   ignore
14. NotifyTest                                                                              ignore
15. org.postgresql.test.jdbc2.ResultSetMetaDataTest#testIdentityColumn                      ignore
16. org.postgresql.test.jdbc2.MiscTest#xtestLocking
17. org.postgresql.test.jdbc2.DatabaseEncodingTest#testTruncatedUTF8Decode                       ignore
18. org.postgresql.test.jdbc2.DatabaseEncodingTest#testBadUTF8Decode
19. org.postgresql.test.jdbc2.UpsertTest                                                     ignore 不支持语法
20. org.postgresql.test.jdbc2.CopyTest#testLockReleaseOnCancelFailure 不稳定ignore
21. org.postgresql.test.jdbc2.CopyTest#testChangeDateStyle 不稳定ignore
22. org.postgresql.test.jdbc2.CopyTest#testCopyInFromStreamFail 不稳定ignore


5.0.x  ignore 183
6.0.0  ignore 162