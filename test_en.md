# Test

1. Configure 3 compatible attribute databases in the configuration file. A/B/PG
2. Used separately in unit tests

   | DBCOMPATIBILITY | BaseTest4        | Desc                   |
   |-----------------|------------------|------------------------|
   | A               | BaseTest4.java   | A /TestUtil.openDB     |
   | PG              | BaseTest4PG.java | PG / TestUtil.openDBPG |
   | B               | BaseTest4B.java  | B / TestUtil.openDBB   |

3. Add version assertion. At least that version is required to run.

    ```java
    // assumeMiniOgVersion
    public void setUp() throws Exception {
        super.setUp();
        assumeMiniOgVersion("opengauss 6.0.0",6,0,0);
    }
    ```

## create database

Create a database in three compatible modes A/PG/B

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

The database password authentication method is sha256

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