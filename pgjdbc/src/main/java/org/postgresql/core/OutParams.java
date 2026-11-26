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

package org.postgresql.core;

import org.postgresql.jdbc.ORResultSet;
import org.postgresql.jdbc.ORStatement;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * callable out param
 *
 * @author zhangting
 * @since  2025-09-29
 */
public class OutParams {
    private static final Map<Integer, Integer> indexMap = new HashMap<>();
    private static final Map<String, Integer> nameMap = new HashMap<>();
    private static final int BYTE0_SIGN = 0;
    private static final int BYTE4_SIGN = 1;
    private static final int BYTE8_SIGN = 2;
    private static final int PARAM_NAME_LEN = 68;
    private static final int COLUMNS_THRESHOLD = 13;
    private static final int LEN_OPERATION = 16777200;

    private int totalParam;
    private ORStatement statement;
    private ORStream orStream;
    private ORResultSet resultSet;
    private ORField[] fields;

    /**
     * OutParams constructor
     *
     * @param statement statement
     * @param orStream orStream
     */
    public OutParams(ORStatement statement, ORStream orStream) {
        this.statement = statement;
        this.orStream = orStream;
    }

    /**
     * decode out param
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs
     */
    public void handleOutParam() throws SQLException, IOException {
        this.totalParam = orStream.receiveInteger4();
        this.fields = new ORField[this.totalParam];
        handleFields();
        int paramCount = 0;
        List<ORParameterList> parameterLists = this.statement.getParametersList();
        if (parameterLists != null && !parameterLists.isEmpty()) {
            ORParameterList params = parameterLists.get(0);
            paramCount = params.getTotalOutParam();
            handleParamMap(params);
        }
        if (paramCount != this.totalParam) {
            throw new SQLException("total out parameter is unexpected. expected "
                    + paramCount + " but " + this.totalParam);
        }
        this.statement.setOutParams(this);
    }

    private void handleParamMap(ORParameterList params) {
        int index = 0;
        List<Integer> outParam = params.getOutParam();
        indexMap.clear();
        nameMap.clear();
        for (int i = 0; i < params.getParamCount(); i++) {
            if (outParam.contains(i + 1)) {
                indexMap.put(i + 1, index + 1);
                String columnName = this.fields[index].getColumnName();
                nameMap.put(columnName, i + 1);
                index++;
            }
        }
    }

    private void handleFields() throws IOException {
        for (int i = 0; i < this.totalParam; i++) {
            ORField field = new ORField();
            byte[] columnNameBytes = orStream.receive(PARAM_NAME_LEN);
            String columnName = getColumnName(columnNameBytes);
            field.setColumnName(columnName);
            byte[] width = orStream.receive(2);
            int inOutFlag = orStream.receiveChar();
            field.setInOutFlag(inOutFlag);
            int dbType = orStream.receiveChar();
            field.setTypeInfo(ORDataType.getDataType(dbType));
            if (dbType == ORDataType.DECIMAL || dbType == ORDataType.NUMERIC) {
                field.setPrecision(width[0]);
                field.setScale(width[1]);
            } else if (dbType == ORDataType.DATE || dbType == ORDataType.TIMESTAMP
                    || dbType == ORDataType.TIMESTAMP_TZ || dbType == ORDataType.TIMESTAMP_LTZ) {
                field.setPrecision(width[0]);
            }
            this.fields[i] = field;
        }
    }

    private String getColumnName(byte[] bytes) {
        int endIndex = 0;
        while (bytes[endIndex] != 0) {
            endIndex++;
        }
        byte[] columnNameBytes = Arrays.copyOfRange(bytes, 0, endIndex);
        return new String(columnNameBytes, orStream.getCharset());
    }

    /**
     * getOutParamIndex
     *
     * @param parameterIndex parameterIndex
     * @return outParamIndex
     * @throws SQLException if a database access error occurs
     */
    public int getOutParamIndex(int parameterIndex) throws SQLException {
        if (!indexMap.containsKey(parameterIndex)) {
            throw new SQLException("parameterIndex " + parameterIndex + " is invalid.");
        }
        return indexMap.get(parameterIndex);
    }

    /**
     * getParameterIndex
     *
     * @param parameterName parameterName
     * @return parameterIndex
     * @throws SQLException if a database access error occurs
     */
    public int getParameterIndex(String parameterName) throws SQLException {
        if (!nameMap.containsKey(parameterName)) {
            throw new SQLException("parameterName " + parameterName + " is invalid.");
        }
        return nameMap.get(parameterName);
    }

    /**
     * handle callable result
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs
     */
    public void handleCallableResult() throws SQLException, IOException {
        orStream.receiveInteger2();
        orStream.receiveInteger2();
        orStream.receiveChar();
        byte[][] values = new byte[this.totalParam][];
        int[] valueLens = new int[this.totalParam];
        getRow(values, valueLens);
        List<byte[][]> allValues = new ArrayList<>();
        allValues.add(values);
        List<int[]> allValueLens = new ArrayList<>();
        allValueLens.add(valueLens);
        this.resultSet = new ORResultSet(this.statement, this.fields, allValueLens, allValues);
    }

    private void getRow(byte[][] values, int[] valueLens) throws IOException {
        int lenOperation = 3;
        if (this.totalParam >= COLUMNS_THRESHOLD) {
            lenOperation += ((this.totalParam + 3 & LEN_OPERATION) / 4);
        }

        byte[] lenMark = orStream.receive(lenOperation);
        int index = 0;
        int colIndex = 0;
        while (true) {
            int p = index * 4;
            int mark = lenMark[index];
            for (int k = p; k < p + 4; k++) {
                if (k >= this.totalParam) {
                    colIndex = k;
                    break;
                }
                int lenId = mark & 3;
                handleValue(lenId, valueLens, values, k);
                mark = mark >> 2;
                colIndex = k;
            }
            if (colIndex >= this.totalParam - 1) {
                break;
            }
            index++;
        }
    }

    private void handleValue(int lenId, int[] valueLen, byte[][] value, int k) throws IOException {
        if (lenId == BYTE0_SIGN) {
            valueLen[k] = -1;
            value[k] = new byte[0];
        } else if (lenId == BYTE4_SIGN) {
            valueLen[k] = 4;
            value[k] = orStream.receive(4);
        } else if (lenId == BYTE8_SIGN) {
            valueLen[k] = 8;
            value[k] = orStream.receive(8);
        } else {
            int len = orStream.receiveInteger2();
            valueLen[k] = len;
            int byte4Len = (4 - (len + 2) % 4) % 4;
            value[k] = orStream.receive(byte4Len + len);
        }
    }

    /**
     * get resultSet
     *
     * @return resultSet
     */
    public ORResultSet getResultSet() {
        return resultSet;
    }
}