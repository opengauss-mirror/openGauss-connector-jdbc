/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.
 */

package org.postgresql.jdbc;

import org.postgresql.core.ORField;
import org.postgresql.core.ORDataType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * the resultSet column info
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORResultSetMetaData implements ResultSetMetaData {
    private final ORField[] fields;

    /**
     * resultSetMetaData constructor
     *
     * @param fields column fields
     */
    public ORResultSetMetaData(ORField[] fields) {
        this.fields = fields;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return fields.length;
    }

    @Override
    public int isNullable(int index) throws SQLException {
        return getCTField(index).getNullableFlag();
    }

    private ORField getCTField(int index) throws SQLException {
        if (index < 1 || index > fields.length) {
            throw new SQLException("index is out of range");
        }
        return fields[index - 1];
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int index) throws SQLException {
        return getCTField(index).getLength();
    }

    @Override
    public boolean isCaseSensitive(int index) throws SQLException {
        ORField field = getCTField(index);
        int type = Integer.valueOf(field.getTypeInfo()[2].toString());
        switch (type) {
            case Types.INTEGER:
            case Types.BOOLEAN:
            case Types.BIGINT:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            case Types.TIME:
            case Types.DATE:
                return false;
            default:
                return true;
        }
    }

    @Override
    public boolean isCurrency(int index) {
        return false;
    }

    @Override
    public boolean isSigned(int index) throws SQLException {
        ORField fieldDef = getCTField(index);
        int type = Integer.parseInt(fieldDef.getTypeInfo()[2].toString());
        switch (type) {
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean isReadOnly(int column) {
        return false;
    }

    @Override
    public boolean isSearchable(int column) {
        return true;
    }

    @Override
    public String getColumnLabel(int index) throws SQLException {
        return getColumnName(index);
    }

    @Override
    public String getSchemaName(int index) {
        return null;
    }

    @Override
    public int getPrecision(int index) throws SQLException {
        ORField field = getCTField(index);
        int type = Integer.parseInt(field.getTypeInfo()[1].toString());
        switch (type) {
            case ORDataType.BINARY:
            case ORDataType.VARBINARY:
            case ORDataType.CHAR:
            case ORDataType.VARCHAR:
            case ORDataType.TEXT:
                return field.getLength();
            default:
                return field.getPrecision();
        }
    }

    @Override
    public int getScale(int index) throws SQLException {
        return getCTField(index).getScale();
    }

    @Override
    public boolean isAutoIncrement(int index) throws SQLException {
        return getCTField(index).isAutoIncrement();
    }

    @Override
    public String getTableName(int column) {
        return null;
    }

    @Override
    public int getColumnType(int index) throws SQLException {
        Object type = getCTField(index).getTypeInfo()[2];
        return Integer.parseInt(type.toString()) ;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("isWrapperFor(Class<T>) not implemented");
    }

    @Override
    public String getColumnTypeName(int index) throws SQLException {
        return getCTField(index).getTypeInfo()[0].toString();
    }

    @Override
    public String getColumnName(int index) throws SQLException {
        return getCTField(index).getColumnName();
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("unwrap(Class<T>) not implemented");
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public String getColumnClassName(int index) throws SQLException {
        return getCTField(index).getTypeInfo()[3].toString();
    }
}