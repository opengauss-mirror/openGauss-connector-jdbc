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

package org.postgresql.core;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * parameter info
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORParameterList {
    /**
     * in param flag
     */
    public static final int IN_FLAG = 64;

    /**
     * out param flag
     */
    public static final int OUT_FLAG = 128;

    private Object[] paramValues;
    private byte[][] byteValues;
    private int[] dbTypes;
    private int paramCount;
    private boolean[] paramNoNull;
    private int[] inOut;
    private int totalOutParam;
    private List<Integer> outParam;

    /**
     * parameter list constructor
     *
     * @param paramCount param count
     */
    public ORParameterList(int paramCount) {
        this.paramValues = new Object[paramCount];
        this.byteValues = new byte[paramCount][];
        this.dbTypes = new int[paramCount];
        this.paramNoNull = new boolean[paramCount];
        this.inOut = new int[paramCount];
        this.paramCount = paramCount;
        this.outParam = new ArrayList<>();
    }

    /**
     * get param byte value
     *
     * @param index param index
     * @return byte value
     */
    public byte[] getByteValue(int index) {
        return byteValues[index];
    }

    /**
     * getOutParam
     *
     * @return outParam
     */
    public List<Integer> getOutParam() {
        return outParam;
    }

    /**
     * set byte[] type parameters
     *
     * @param orStream data inputstream processor
     * @param index param index
     * @param dbType data type
     * @param paramValue param value
     * @throws SQLException if a database access error occurs
     */
    public void bindParam(ORStream orStream, int index, int dbType, Object paramValue) throws SQLException {
        verifyIndex(index);
        this.dbTypes[index - 1] = dbType;
        this.paramValues[index - 1] = paramValue;
        this.paramNoNull[index - 1] = true;
        this.inOut[index - 1] = this.inOut[index - 1] | IN_FLAG;
        if (paramValue == null) {
            byteValues[index - 1] = new byte[0];
            return;
        }
        switch (dbType) {
            case ORDataType.INT:
                byteValues[index - 1] = orStream.getInteger4Bytes(Integer.valueOf(paramValue.toString()));
                break;
            case ORDataType.BIGINT:
                byteValues[index - 1] = orStream.getInteger8Bytes(Long.valueOf(paramValue.toString()));
                break;
            case ORDataType.REAL:
                long lp = Double.doubleToRawLongBits(Double.valueOf(paramValue.toString()));
                byteValues[index - 1] = orStream.getInteger8Bytes(lp);
                break;
            case ORDataType.TIME:
                byteValues[index - 1] = orStream.getInteger8Bytes(Long.valueOf(paramValue.toString()));
                break;
            case ORDataType.DATE:
                byteValues[index - 1] = orStream.getInteger8Bytes(Long.valueOf(paramValue.toString()));
                break;
            case ORDataType.TIMESTAMP:
            case ORDataType.TIMESTAMP_LTZ:
                byteValues[index - 1] = orStream.getInteger8Bytes(Long.valueOf(paramValue.toString()));
                break;
            case ORDataType.NUMERIC:
            case ORDataType.DECIMAL:
            case ORDataType.CHAR:
            case ORDataType.VARCHAR:
            case ORDataType.TEXT:
                byte[] data = String.valueOf(paramValue).getBytes(orStream.getCharset());
                byteValues[index - 1] = getParamBytes(orStream, data);
                break;
            case ORDataType.VARBINARY:
            case ORDataType.BINARY:
            case ORDataType.RAW:
                byteValues[index - 1] = getParamBytes(orStream, (byte[]) paramValue);
                break;
            default:
                throw new SQLException("type " + ORDataType.getDataType(dbType)[0] + " is invalid.");
        }
    }

    /**
     * bind param type
     *
     * @param index index
     * @param sqlType sqlType
     * @throws SQLException if a database access error occurs
     */
    public void bindType(int index, int sqlType) throws SQLException {
        verifyIndex(index);
        int dbType = getDbType(sqlType);
        this.paramNoNull[index - 1] = true;
        if ((inOut[index - 1] & IN_FLAG) == 0) {
            this.dbTypes[index - 1] = dbType;
        }
        totalOutParam++;
        outParam.add(index);
        inOut[index - 1] = inOut[index - 1] | OUT_FLAG;
    }

    private int getDbType(int sqlType) {
        switch (sqlType) {
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return ORDataType.REAL;
            case Types.NUMERIC:
                return ORDataType.NUMERIC;
            case Types.DECIMAL:
                return ORDataType.DECIMAL;
            case Types.VARCHAR:
                return ORDataType.TEXT;
            case Types.CLOB:
            case Types.LONGVARCHAR:
                return ORDataType.CLOB;
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return ORDataType.BLOB;
            case Types.CHAR:
                return ORDataType.CHAR;
            case Types.BOOLEAN:
                return ORDataType.BOOL;
            case Types.BINARY:
                return ORDataType.BINARY;
            case Types.VARBINARY:
                return ORDataType.VARBINARY;
            case Types.BIT:
            case Types.INTEGER:
            case Types.TINYINT:
            case Types.SMALLINT:
                return ORDataType.INT;
            case Types.BIGINT:
                return ORDataType.BIGINT;
            case Types.DATE:
                return ORDataType.DATE;
            case Types.TIMESTAMP:
                return ORDataType.TIMESTAMP;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return ORDataType.TIMESTAMP_TZ;
            case Types.ARRAY:
                return ORDataType.VARCHAR;
            case Types.REF_CURSOR:
                return ORDataType.REF_CURSOR;
            case Types.NULL:
                return ORDataType.VARCHAR;
            case Types.OTHER:
                return ORDataType.UNSPECIFIED;
            default:
                return ORDataType.VARCHAR;
        }
    }

    /**
     * Convert db type
     *
     * @param orStream orStream
     * @param paramIndex oaram index
     * @param targetType target type
     * @throws SQLException if a database access error occurs
     */
    public void transferType(ORStream orStream, int paramIndex, int targetType) throws SQLException {
        verifyIndex(paramIndex + 1);
        dbTypes[paramIndex] = targetType;
        if (paramValues[paramIndex] == null) {
            return;
        }
        switch (targetType) {
            case ORDataType.INT:
                int intValue = Integer.parseInt(paramValues[paramIndex].toString());
                byteValues[paramIndex] = orStream.getInteger4Bytes(intValue);
                break;
            case ORDataType.BIGINT:
                long longValue = Long.valueOf(paramValues[paramIndex].toString());
                byteValues[paramIndex] = orStream.getInteger8Bytes(longValue);
                break;
            case ORDataType.REAL:
                double doubleValue = Double.valueOf(paramValues[paramIndex].toString());
                long lp = Double.doubleToRawLongBits(doubleValue);
                byteValues[paramIndex] = orStream.getInteger8Bytes(lp);
                break;
            case ORDataType.TIME:
                long time = Long.valueOf(paramValues[paramIndex].toString());
                byteValues[paramIndex] = orStream.getInteger8Bytes(time);
                break;
            case ORDataType.DATE:
                long dateValue = Long.valueOf(paramValues[paramIndex].toString());
                byteValues[paramIndex] = orStream.getInteger8Bytes(dateValue);
                break;
            case ORDataType.TIMESTAMP:
            case ORDataType.TIMESTAMP_LTZ:
                long timestamp = Long.valueOf(paramValues[paramIndex].toString());
                byteValues[paramIndex] = orStream.getInteger8Bytes(timestamp);
                break;
            case ORDataType.NUMERIC:
            case ORDataType.DECIMAL:
            case ORDataType.CHAR:
            case ORDataType.VARCHAR:
            case ORDataType.TEXT:
                byte[] data = String.valueOf(paramValues[paramIndex]).getBytes(orStream.getCharset());
                byteValues[paramIndex] = getParamBytes(orStream, data);
                break;
            case ORDataType.VARBINARY:
            case ORDataType.BINARY:
            case ORDataType.RAW:
                byteValues[paramIndex] = getParamBytes(orStream, (byte[]) paramValues[paramIndex]);
                break;
            default:
                throw new SQLException("type " + targetType + " is invalid.");
        }
    }

    private void verifyIndex(int index) throws SQLException {
        if (index < 1 || index > paramCount) {
            throw new SQLException("The column index is out of range: " + index + ", number of columns: " + paramCount);
        }
    }

    private byte[] getParamBytes(ORStream orStream, byte[] data) {
        byte[] lenByte = orStream.getInteger4Bytes(data.length);
        byte[] dataByte = data;
        if (data.length % 4 != 0) {
            int len = data.length + 4 - data.length % 4;
            dataByte = new byte[len];
            setParam(data, dataByte, 0);
        }
        byte[] paramByte = new byte[lenByte.length + dataByte.length];
        setParam(lenByte, paramByte, 0);
        setParam(dataByte, paramByte, lenByte.length);
        return paramByte;
    }

    /**
     * copy byte array
     *
     * @param srcByte source bytes
     * @param destByte dest bytes
     * @param destPos dest position
     */
    public void setParam(byte[] srcByte, byte[] destByte, int destPos) {
        for (int i = 0; i < srcByte.length; i++) {
            destByte[i + destPos] = srcByte[i];
        }
    }

    /**
     * Is it a stored procedure
     *
     * @return has out param
     */
    public boolean isProcedure() {
        return totalOutParam > 0;
    }

    /**
     * get param count
     *
     * @return param count
     */
    public int getParamCount() {
        return paramCount;
    }

    /**
     * Ensure that all parameters in this list have been assigned values. Return silently if all is
     * well, otherwise throw an appropriate exception.
     *
     * @throws SQLException if a database access error occurs
     */
    public void checkAllParametersSet() throws SQLException {
        for (int i = 0; i < paramCount; i++) {
            if (!paramNoNull[i]) {
                throw new PSQLException(GT.tr("No value specified for parameter {0}.", i + 1),
                        PSQLState.INVALID_PARAMETER_VALUE);
            }
        }
    }

    /**
     * get all params
     *
     * @return param
     */
    public Object[] getParamValues() {
        return paramValues;
    }

    /**
     * get param type
     *
     * @return param type
     */
    public int[] getInOut() {
        return inOut;
    }

    /**
     * get out param count
     *
     * @return out param count
     */
    public int getTotalOutParam() {
        return totalOutParam;
    }

    /**
     * get db types of all fields
     *
     * @return dbTypes
     */
    public int[] getDbTypes() {
        return dbTypes;
    }

    /**
     * clear parameters
     */
    public void clear() {
        Arrays.fill(paramValues, null);
        Arrays.fill(dbTypes, 0);
        Arrays.fill(paramNoNull, false);
    }
}
