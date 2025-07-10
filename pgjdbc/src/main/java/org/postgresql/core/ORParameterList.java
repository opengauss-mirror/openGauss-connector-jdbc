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

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.Arrays;

/**
 * parameter info
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORParameterList {
    private Object[] paramValues;
    private byte[][] byteValues;
    private int[] dbTypes;
    private int paramCount;
    private boolean[] paramNoNull;

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
        this.paramCount = paramCount;
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
     * set byte[] type parameters
     *
     * @param orStream data inputstream processor
     * @param index param index
     * @param dbType data type
     * @param paramValue param value
     * @throws SQLException if a database access error occurs
     */
    public void bindParam(ORStream orStream, int index, int dbType, Object paramValue) throws SQLException {
        this.dbTypes[index - 1] = dbType;
        this.paramValues[index - 1] = paramValue;
        this.paramNoNull[index - 1] = true;
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
