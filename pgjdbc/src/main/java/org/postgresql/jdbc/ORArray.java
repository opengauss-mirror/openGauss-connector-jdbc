/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the 2-Clause BSD License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/bsd-2-clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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