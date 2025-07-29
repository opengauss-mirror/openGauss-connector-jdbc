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

package org.postgresql.jdbc;

import org.postgresql.core.ORField;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Array is used collect one column of query result data.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORArray implements java.sql.Array {
    private ORField field;

    private int type;

    private Object value;

    /**
     * set field
     *
     * @param field field
     */
    public void setField(ORField field) {
        this.field = field;
    }

    /**
     * set db type
     *
     * @param type db type
     */
    public void setType(int type) {
        this.type = type;
    }

    /**
     * set value
     *
     * @param value value
     */
    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String getBaseTypeName() {
        return field.getTypeInfo()[0].toString();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throw new SQLException("getArray(long,int) not implemented");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("getResultSet(Map) not implemented");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLException("getResultSet(long, int) not implemented");
    }

    @Override
    public int getBaseType() {
        return (int) field.getTypeInfo()[2];
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("getArray(Map) not implemented");
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getArray(map);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("getResultSet(long,int,Map) not implemented");
    }

    @Override
    public Object getArray() {
        return value;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLException("getResultSet() not implemented");
    }

    @Override
    public void free() throws SQLException {
        throw new SQLException("free() not implemented");
    }
}