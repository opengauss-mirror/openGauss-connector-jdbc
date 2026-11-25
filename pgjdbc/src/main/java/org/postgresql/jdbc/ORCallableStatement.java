/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
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
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.core.ORDataType;
import org.postgresql.core.ORField;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.Ref;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Array;
import java.sql.RowId;
import java.sql.NClob;
import java.sql.SQLXML;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * ORCallableStatement
 *
 * @author zhangting
 * @since  2025-09-29
 */
public class ORCallableStatement extends ORPreparedStatement implements CallableStatement {
    private int currentIndex = 0;

    /**
     * preparedStatement constructor
     *
     * @param conn                 connection
     * @param sql                  sql
     * @param resultSetType        resultSetType
     * @param resultSetConcurrency resultSetConcurrency
     * @param resultSetHoldability resultSetHoldability
     */
    public ORCallableStatement(ORConnection conn, String sql, int resultSetType,
                               int resultSetConcurrency, int resultSetHoldability) {
        super(conn, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        verifyClosed();
        preparedParameters.bindType(parameterIndex, sqlType);
    }

    @Override
    public void addBatch() throws SQLException {
        if (this.preparedParameters.isProcedure()) {
            throw new SQLException("procedure do not support batch processing.");
        }
        hasParam = true;
        addParameters();
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (currentIndex < 1) {
            throw new SQLException("wasNull cannot be call before get data");
        }
        List<int[]> valueLens = callableRs.getValueLens();
        if (!valueLens.isEmpty()) {
            int length = valueLens.get(0)[currentIndex - 1];
            return length < 0;
        }
        return true;
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getString(outParamIndex);
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getBoolean(outParamIndex);
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getByte(outParamIndex);
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getShort(outParamIndex);
    }

    private int getOutParamIndex(int parameterIndex) throws SQLException {
        if (outParams == null) {
            throw new SQLException("outParams is null.");
        }
        return outParams.getOutParamIndex(parameterIndex);
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getInt(outParamIndex);
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getLong(outParamIndex);
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getFloat(outParamIndex);
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getDouble(outParamIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return this.getBigDecimal(outParamIndex);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getBytes(outParamIndex);
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getDate(outParamIndex);
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getTime(outParamIndex);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getTimestamp(outParamIndex);
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        ORField[] orFields = callableRs.getOrFields();
        Object[] type = orFields[outParamIndex - 1].getTypeInfo();
        int dbType = Integer.parseInt(type[1].toString());
        if (dbType == ORDataType.REF_CURSOR) {
            List<int[]> valueLens = callableRs.getValueLens();
            if (valueLens == null || valueLens.isEmpty()) {
                throw new SQLException("callable value is null.");
            }
            int cursorLen = valueLens.get(0)[outParamIndex - 1];
            if (cursorLen < 0) {
                throw new SQLException("cursor is invalid.");
            }
            long cursorId = callableRs.getLong(outParamIndex);
            return getRefCursor(cursorId);
        }
        return callableRs.getObject(outParamIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getBigDecimal(outParamIndex);
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getObject(outParamIndex, map);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getRef(outParamIndex);
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getBlob(outParamIndex);
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getClob(outParamIndex);
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getArray(outParamIndex);
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getDate(outParamIndex, cal);
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getTime(outParamIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getTimestamp(outParamIndex, cal);
    }

    private void updateIndex(int parameterIndex) {
        this.currentIndex = parameterIndex;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "registerOutParameter(int, int, String)");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "registerOutParameter(String, int)");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "registerOutParameter(String, int, int)");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "registerOutParameter(String, int, String)");
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return callableRs.getURL(outParamIndex);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setURL(String, URL)");
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNull(String, int)");
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBoolean(String,boolean)");
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setByte(String,byte)");
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setShort(String,short)");
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setInt(String,int)");
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setLong(String,long)");
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setFloat(String,float)");
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setDouble(String,double)");
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBigDecimal(String,BigDecimal)");
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setString(String,String)");
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBytes(String,byte)");
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setDate(String,Date)");
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setTime(String,Time)");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setTimestamp(String,Timestamp)");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setAsciiStream(String,InputStream,int)");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBinaryStream(String,InputStream,int)");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setObject(String,Object,int,int)");
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setObject(String,Object,int)");
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setObject(String,Object,int)");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setCharacterStream(String,Reader,int)");
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setDate(String,Date,Calendar)");
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setTime(String,Time,Calendar)");
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setTimestamp(String,Timestamp,Calendar)");
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNull(String,int,String)");
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getString(index);
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getBoolean(index);
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getByte(index);
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getShort(index);
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getInt(index);
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getLong(index);
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getFloat(index);
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getDouble(index);
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getBytes(index);
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getDate(index);
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getTime(index);
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getTimestamp(index);
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getObject(index);
    }

    private int getParameterIndex(String parameterName) throws SQLException {
        if (outParams == null) {
            throw new SQLException("outParams is null.");
        }
        return outParams.getParameterIndex(parameterName.toUpperCase(Locale.ENGLISH));
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getBigDecimal(index);
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getObject(String,Map)");
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getRef(String)");
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getBlob(index);
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getClob(index);
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getArray(String)");
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getDate(index);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getTime(index);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getTimestamp(index);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getURL(String)");
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getRowId(int)");
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getRowId(String)");
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setRowId(String, RowId)");
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNString(String, String)");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNCharacterStream(String, Reader, long)");
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNClob(String, NClob)");
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setClob(String, Reader, long)");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBlob(String, InputStream, long)");
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNClob(String, Reader, long)");
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getNClob(int)");
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getNClob(String)");
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setSQLXML(String, SQLXML)");
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getSQLXML(int)");
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getSQLXML(String)");
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getNString(int)");
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getNString(String)");
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getNCharacterStream(int)");
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getNCharacterStream(String)");
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        int outParamIndex = getOutParamIndex(parameterIndex);
        updateIndex(outParamIndex);
        return new StringReader(callableRs.getString(outParamIndex));
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        int index = getParameterIndex(parameterName);
        updateIndex(index);
        return getCharacterStream(index);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBlob(String, Blob)");
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setClob(String, Clob)");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setAsciiStream(String, InputStream, long)");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBinaryStream(String, InputStream, long)");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setCharacterStream(String, Reader, long)");
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setAsciiStream(String, InputStream)");
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBinaryStream(String, InputStream)");
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setCharacterStream(String, Reader)");
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNCharacterStream(String, Reader)");
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setClob(String, Reader)");
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setBlob(String, InputStream)");
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "setNClob(String, Reader)");
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getObject(int, Class<T>)");
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getObject(String, Class<T>)");
    }
}
