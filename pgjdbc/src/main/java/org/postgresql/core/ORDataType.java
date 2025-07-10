/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.postgresql.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.sql.Types;
import java.sql.SQLException;

import java.util.List;

/**
 * data type info
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORDataType {
    /**
     * unknown type
     */
    public static final int UNSPECIFIED = -1;

    /**
     * dbtype real
     */
    public static final int REAL = 3;

    /**
     * dbtype numeric
     */
    public static final int NUMERIC = 4;

    /**
     * dbtype decimal
     */
    public static final int DECIMAL = 5;

    /**
     * dbtype text
     */
    public static final int TEXT = 10;

    /**
     * dbtype clob
     */
    public static final int CLOB = 13;

    /**
     * dbtype blob
     */
    public static final int BLOB = 14;

    /**
     * dbtype char
     */
    public static final int CHAR = 8;

    /**
     * dbtype varchar
     */
    public static final int VARCHAR = 9;

    /**
     * dbtype bool
     */
    public static final int BOOL = 17;

    /**
     * dbtype binary
     */
    public static final int BINARY = 11;

    /**
     * dbtype varbinary
     */
    public static final int VARBINARY = 12;

    /**
     * dbtype int
     */
    public static final int INT = 1;

    /**
     * dbtype bigint
     */
    public static final int BIGINT = 2;

    /**
     * dbtype date
     */
    public static final int DATE = 6;

    /**
     * dbtype timestamp
     */
    public static final int TIMESTAMP = 7;

    /**
     * dbtype timestamp_ltz
     */
    public static final int TIMESTAMP_LTZ = 19;

    /**
     * dbtype timestamp_tz
     */
    public static final int TIMESTAMP_TZ = 32;

    /**
     * dbtype raw
     */
    public static final int RAW = 23;

    /**
     * dbtype image
     */
    public static final int IMAGE = 24;

    /**
     * dbtype UTC
     */
    public static final int UTC = 106;

    /**
     * dbtype time
     */
    public static final int TIME = 113;

    /**
     * dbtype array
     */
    public static final int ARRAY = 33;

    private static List<Object[]> types = new ArrayList<>();
    private static HashMap<Integer, Object[]> dbToType = new HashMap<>();
    private static HashMap<Integer, Integer> dbToJavaType = new HashMap<>();

    static {
        setTypes();
        for (Object[] type : types) {
            dbToType.put(Integer.valueOf(type[1].toString()), type);
            dbToJavaType.put(Integer.valueOf(type[1].toString()), Integer.valueOf(type[2].toString()));
        }
    }

    private static void setTypes() {
        types.add(new Object[]{"REAL", REAL, Types.DOUBLE, "java.lang.Double"});
        types.add(new Object[]{"NUMERIC", NUMERIC, Types.NUMERIC, "java.math.BigDecimal"});
        types.add(new Object[]{"DECIMAL", DECIMAL, Types.NUMERIC, "java.math.BigDecimal"});
        types.add(new Object[]{"TEXT", TEXT, Types.VARCHAR, "java.lang.String"});
        types.add(new Object[]{"CLOB", CLOB, Types.CLOB, "java.sql.Clob"});
        types.add(new Object[]{"BLOB", BLOB, Types.BLOB, "java.sql.Blob"});
        types.add(new Object[]{"CHAR", CHAR, Types.CHAR, "java.lang.String"});
        types.add(new Object[]{"VARCHAR", VARCHAR, Types.VARCHAR, "java.lang.String"});
        types.add(new Object[]{"BOOL", BOOL, Types.BOOLEAN, "java.lang.Boolean"});
        types.add(new Object[]{"BINARY", BINARY, Types.BINARY, "[B"});
        types.add(new Object[]{"VARBINARY", VARBINARY, Types.VARBINARY, "[B"});
        types.add(new Object[]{"INT", INT, Types.INTEGER, "java.lang.Integer"});
        types.add(new Object[]{"BIGINT", BIGINT, Types.BIGINT, "java.lang.Long"});
        types.add(new Object[]{"DATE", DATE, Types.DATE, "java.sql.Date"});
        types.add(new Object[]{"TIMESTAMP", TIMESTAMP, Types.TIMESTAMP, "java.sql.Timestamp"});
        types.add(new Object[]{"TIMESTAMP_LTZ", TIMESTAMP_LTZ, Types.TIMESTAMP, "java.sql.Timestamp"});
        types.add(new Object[]{"TIMESTAMP_TZ", TIMESTAMP_TZ, Types.TIMESTAMP_WITH_TIMEZONE, "java.sql.Timestamp"});
        types.add(new Object[]{"RAW", RAW, Types.BINARY, "[B"});
        types.add(new Object[]{"IMAGE", IMAGE, Types.BLOB, "java.sql.Blob"});
        types.add(new Object[]{"UTC", UTC, Types.TIMESTAMP, "java.sql.Timestamp"});
        types.add(new Object[]{"TIME", TIME, Types.TIME, "java.sql.Time"});
        types.add(new Object[]{"ARRAY", ARRAY, Types.ARRAY, "java.sql.Array"});
    }

    /**
     * get jdbc type
     *
     * @param dbType db type
     * @return jdbc type
     * @throws SQLException if a database access error occurs
     */
    public static int getType(int dbType) throws SQLException {
        if (dbToJavaType.containsKey(dbType)) {
            return dbToJavaType.get(dbType);
        }
        throw new SQLException("the dbType " + dbType + " is invalid.");
    }

    /**
     * get type info
     *
     * @param dbType db type
     * @return type info
     * @throws SQLException if a database access error occurs
     */
    public static Object[] getDataType(int dbType) throws SQLException {
        if (dbToType.containsKey(dbType)) {
            return dbToType.get(dbType);
        }
        throw new SQLException("the dbType " + dbType + " is invalid.");
    }
}
