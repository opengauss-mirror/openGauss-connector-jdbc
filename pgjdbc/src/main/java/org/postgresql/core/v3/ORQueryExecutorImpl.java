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

package org.postgresql.core.v3;

import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.ORCachedQuery;
import org.postgresql.core.ORQueryExecutor;
import org.postgresql.core.ORStream;
import org.postgresql.core.ORParameterList;
import org.postgresql.core.ORField;
import org.postgresql.core.ORDataType;
import org.postgresql.util.ORPackageHead;
import org.postgresql.util.PSQLState;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ORRequestCommand;
import org.postgresql.jdbc.ORStatement;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.channels.SocketChannel;
import java.io.IOException;

/**
 * QueryExecutor implementation for the ogRAC protocol.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORQueryExecutorImpl implements ORQueryExecutor {
    private static Log LOGGER = Logger.getLogger(ORQueryExecutorImpl.class.getName());
    private static final int AGENT = 256;
    private static final int NULL_SIGN = 65;
    private static final int NON_NULL_SIGN = 64;
    private static final int LEN_OPERATION = 16777200;
    private static final int QUERY_FLAG_OPERATION = 4 * 1024 - 1;
    private static final int QUERY_MODE_OPERATION = 4 * 1024;
    private static final int BYTE0_SIGN = 0;
    private static final int BYTE4_SIGN = 1;
    private static final int BYTE8_SIGN = 2;
    private static final int PACKAGE_HEAD_SIZE = 16;
    private static final int COLUMNS_THRESHOLD = 13;

    private SocketChannel socketChannel;
    private ORStream orStream;
    private ORBaseConnection connection;
    private boolean isClosed = false;

    /**
     * query executor constructor
     *
     * @param orStream  orStream
     * @param connection connection
     * @throws IOException if an I/O error occurs
     */
    public ORQueryExecutorImpl(ORStream orStream, ORBaseConnection connection) throws IOException {
        this.orStream = orStream;
        this.socketChannel = SocketChannel.open();
        this.socketChannel.configureBlocking(true);
        this.connection = connection;
    }

    @Override
    public synchronized void execute(ORCachedQuery cachedQuery, List<ORParameterList> batchParameters)
            throws SQLException {
        try {
            ORPackageHead packageHead = new ORPackageHead();
            short statId = (short) cachedQuery.getCtStatement().getMark();
            if (cachedQuery.isPrepare() && statId != -1) {
                sendPrepareQuery(cachedQuery, packageHead, batchParameters);
            } else {
                sendQuery(cachedQuery, packageHead, batchParameters);
            }
            processResults(cachedQuery, packageHead);
            int headRequestCount = packageHead.getRequestCount();
            int requestCount = orStream.getRequestCount();
            while (headRequestCount < requestCount) {
                processResults(cachedQuery, packageHead);
                headRequestCount = packageHead.getRequestCount();
            }
            if (headRequestCount > requestCount) {
                throw new PSQLException(GT.tr("request count error, actual request number is "
                        + requestCount + ", head request count is " + headRequestCount),
                        PSQLState.DATA_ERROR);
            }
        } catch (IOException e) {
            try {
                orStream.getSocket().close();
            } catch (IOException e2) {
                LOGGER.trace("Catch IOException on close:", e2);
            }
            isClosed = true;
            String socketStatus = getSocketStatus();
            throw new PSQLException(GT.tr(socketStatus + "An I/O error occured while "
                    + "sending to the backend." + "detail:" + e.getMessage() + "; "),
                    PSQLState.CONNECTION_FAILURE, e);
        } catch (SQLException e2) {
            throw e2;
        }
    }

    private String getSocketStatus() throws SQLException {
        if (connection.isClosed()) {
            return "socket is closed; ";
        }
        return "socket is not closed; ";
    }

    @Override
    public synchronized void commit() throws IOException, SQLException {
        ORPackageHead commitPackageHead = new ORPackageHead();
        commitPackageHead.setExecCmd((byte) ORRequestCommand.COMMIT);
        transactionHandle(commitPackageHead);
        processResults(null, commitPackageHead);
    }

    @Override
    public synchronized void rollback() throws IOException, SQLException {
        ORPackageHead rollbackPackageHead = new ORPackageHead();
        rollbackPackageHead.setExecCmd((byte) ORRequestCommand.ROLLBACK);
        transactionHandle(rollbackPackageHead);
        processResults(null, rollbackPackageHead);
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        try {
            ORPackageHead closePackageHead = new ORPackageHead();
            closePackageHead.setExecCmd((byte) ORRequestCommand.LOGOUT);
            transactionHandle(closePackageHead);
            orStream.close();
        } catch (IOException ioe) {
            LOGGER.trace("Discarding IOException on close:", ioe);
        }
        isClosed = true;
    }

    @Override
    public void closeResultSet(ORStatement ctStatement) {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void fetch(ORCachedQuery cachedQuery) throws SQLException, IOException {
        ORPackageHead fetchRowPackageHead = new ORPackageHead();
        fetchRowPackageHead.setExecCmd((byte) ORRequestCommand.FETCH);
        fetchRowPackageHead.setRequestCount(orStream.addRequestCount());
        byte[] headBytes = getHeadBytes(fetchRowPackageHead);
        orStream.sendInteger4(PACKAGE_HEAD_SIZE + 4);
        orStream.send(headBytes);
        short statId = (short) cachedQuery.getCtStatement().getMark();
        orStream.sendInteger2(statId);
        orStream.sendChar(0);
        orStream.sendChar(0);
        orStream.flush();
        processResults(cachedQuery, fetchRowPackageHead);
    }

    private void transactionHandle(ORPackageHead packageHead) throws IOException {
        packageHead.setRequestCount(orStream.addRequestCount());
        byte[] headBytes = getHeadBytes(packageHead);
        orStream.sendInteger4(PACKAGE_HEAD_SIZE);
        orStream.send(headBytes);
        orStream.flush();
    }

    @Override
    public void freeStatement(ORStatement stat) throws IOException, SQLException {
        ORPackageHead freePackageHead = new ORPackageHead();
        freePackageHead.setExecCmd((byte) ORRequestCommand.FREE_STMT);
        freePackageHead.setRequestCount(orStream.addRequestCount());
        byte[] headBytes = getHeadBytes(freePackageHead);

        orStream.sendInteger4(PACKAGE_HEAD_SIZE + 4);
        orStream.send(headBytes);
        short statId = (short) stat.getMark();
        orStream.sendInteger2(statId);
        orStream.sendChar(0);
        orStream.sendChar(0);
        orStream.flush();
        processResults(null, freePackageHead);
    }

    private void sendQuery(ORCachedQuery cachedQuery, ORPackageHead packageHead,
                           List<ORParameterList> batchParameters) throws SQLException, IOException {
        packageHead.setExecCmd((byte) ORRequestCommand.PREP_AND_EXECUTE);
        packageHead.setRequestCount(orStream.addRequestCount());
        try {
            int msgLen = PACKAGE_HEAD_SIZE;
            msgLen += 4;
            msgLen += 4;

            String sql = cachedQuery.getSql();
            byte[] sqlData = sql.getBytes(this.orStream.getCharset());
            int align4BytesSqlLength = sqlData.length % 4 == 0 ? sqlData.length
                    : sqlData.length + (4 - sqlData.length % 4);
            byte[] tempSql = new byte[align4BytesSqlLength];
            System.arraycopy(sqlData, 0, tempSql, 0, sqlData.length);
            msgLen = msgLen + 4 + tempSql.length;

            msgLen += 4;
            msgLen++;
            msgLen += 3;

            List<byte[]> byteParams = getBytesParam(batchParameters);
            int executeSize = 1;
            if (!byteParams.isEmpty()) {
                executeSize = byteParams.size();
            }
            for (byte[] param : byteParams) {
                msgLen += param.length;
            }
            byte[] headData = getHeadBytes(packageHead);
            orStream.sendInteger4(msgLen);
            orStream.send(headData);
            int statId = cachedQuery.getCtStatement().getMark();
            orStream.sendInteger2(statId);
            orStream.sendChar(0);
            orStream.sendChar(0);
            byte[] bs = orStream.getInteger4Bytes(0);
            orStream.send(bs);
            orStream.sendInteger4(sqlData.length);
            orStream.send(tempSql);
            orStream.sendInteger2(executeSize);
            int fetchSize = cachedQuery.getCtStatement().getFetchSize();
            orStream.sendInteger2(fetchSize);
            int autoCommit = cachedQuery.getConn().getAutoCommit() ? 1 : 0;
            orStream.sendChar(autoCommit);
            orStream.sendChar(0);
            orStream.sendChar(0);
            orStream.sendChar(0);
            for (byte[] paramToServer : byteParams) {
                orStream.send(paramToServer);
            }
            orStream.flush();
        } catch (IOException | SQLException e) {
            throw e;
        }
    }

    private List<byte[]> getBytesParam(List<ORParameterList> batchParameters) throws SQLException {
        if (batchParameters == null || batchParameters.isEmpty()) {
            return new ArrayList<>();
        }
        int paramCount = batchParameters.get(0).getParamCount();
        int byte4Count = paramCount % 4 == 0 ? paramCount : paramCount + (4 - paramCount % 4);
        byte[] paramType = new byte[byte4Count];
        int[] typeMode = batchParameters.get(0).getDbTypes();
        for (int i = 0; i < paramCount; i++) {
            if (typeMode[i] == ORDataType.TIME) {
                paramType[i] = ORDataType.TIMESTAMP;
            } else {
                paramType[i] = (byte) typeMode[i];
            }
        }

        return encodeParam(batchParameters, paramCount, typeMode, byte4Count, paramType);
    }

    private List<byte[]> encodeParam(List<ORParameterList> batchParameters, int paramCount,
                                     int[] typeMode, int byte4Count, byte[] paramType) {
        List<byte[]> byteParams = new ArrayList<>();
        boolean isHead = true;
        for (ORParameterList paramList : batchParameters) {
            for (int i = 0; i < paramCount; i++) {
                if (typeMode[i] != paramList.getDbTypes()[i]) {
                    return byteParams;
                }
            }
            List<byte[]> byteParam = new ArrayList<>();
            int size = 0;
            byte[] nullSign = new byte[byte4Count];
            int count = paramList.getParamCount();
            for (int i = 0; i < count; i++) {
                if (paramList.getParamValues()[i] == null) {
                    nullSign[i] = NULL_SIGN;
                } else {
                    nullSign[i] = NON_NULL_SIGN;
                }
                byte[] param = paramList.getByteValue(i);
                size += param.length;
                byteParam.add(param);
            }
            byte[] allParam = new byte[size];
            int index = 0;
            for (byte[] p : byteParam) {
                System.arraycopy(p, 0, allParam, index, p.length);
                index += p.length;
            }

            int point = 0;
            size += 4;
            size += byte4Count;
            byte[] sizeByte = orStream.getInteger4Bytes(size);
            byte[] paramData = new byte[size];
            if (isHead) {
                size += byte4Count;
                paramData = new byte[size];
                paramList.setParam(paramType, paramData, 0);
                point = paramType.length;
            }
            paramList.setParam(sizeByte, paramData, point);
            point += sizeByte.length;
            paramList.setParam(nullSign, paramData, point);
            point += nullSign.length;
            paramList.setParam(allParam, paramData, point);
            byteParams.add(paramData);
            isHead = false;
        }
        return byteParams;
    }

    private void sendPrepareQuery(ORCachedQuery cachedQuery, ORPackageHead packageHead,
                                  List<ORParameterList> batchParameters) throws SQLException, IOException {
        packageHead.setExecCmd((byte) ORRequestCommand.EXECUTE);
        packageHead.setRequestCount(orStream.addRequestCount());
        try {
            int msgLen = PACKAGE_HEAD_SIZE;
            msgLen += 6;
            msgLen++;
            msgLen++;
            List<byte[]> byteParams = getBytesParam(batchParameters);
            int executeSize = 1;
            if (!byteParams.isEmpty()) {
                executeSize = byteParams.size();
            }
            for (byte[] param : byteParams) {
                msgLen += param.length;
            }
            byte[] headData = getHeadBytes(packageHead);
            orStream.sendInteger4(msgLen);
            orStream.send(headData);
            int statId = cachedQuery.getCtStatement().getMark();
            orStream.sendInteger2(statId);
            orStream.sendInteger2(executeSize);
            int fetchSize = cachedQuery.getCtStatement().getFetchSize();
            orStream.sendInteger2(fetchSize);
            int autoCommit = cachedQuery.getConn().getAutoCommit() ? 1 : 0;
            orStream.sendChar(autoCommit);
            orStream.sendChar(0);
            for (byte[] paramToServer : byteParams) {
                orStream.send(paramToServer);
            }
            orStream.flush();
        } catch (IOException | SQLException e) {
            throw e;
        }
    }

    private byte[] getHeadBytes(ORPackageHead packageHead) {
        byte[] flagByte = orStream.getInteger2Bytes(packageHead.getFlags());
        byte[] data = new byte[12];
        int index = 0;
        data[index++] = packageHead.getExecCmd();
        data[index++] = packageHead.getExecResult();
        for (int i = 0; i < flagByte.length; i++) {
            data[index++] = flagByte[i];
        }

        data[index++] = packageHead.getVersion();
        data[index++] = packageHead.getVersion1();
        data[index++] = packageHead.getVersion2();
        data[index++] = 0;
        byte[] serialNumberByte = orStream.getInteger4Bytes(packageHead.getRequestCount());
        for (int i = 0; i < serialNumberByte.length; i++) {
            data[index++] = serialNumberByte[i];
        }
        return data;
    }

    private void processResults(ORCachedQuery cachedQuery, ORPackageHead packageHead) throws SQLException, IOException {
        while (true) {
            packageHead.setSize(orStream.receiveInteger4());
            packageHead.setExecCmd((byte) orStream.receiveChar());
            packageHead.setExecResult((byte) orStream.receiveChar());
            packageHead.setFlags((short) orStream.receiveInteger2());
            packageHead.setVersion((byte) orStream.receiveChar());
            packageHead.setVersion1((byte) orStream.receiveChar());
            packageHead.setVersion2((byte) orStream.receiveChar());
            orStream.receiveChar();
            packageHead.setRequestCount(orStream.receiveInteger4());
            int remainLen = packageHead.getSize() - PACKAGE_HEAD_SIZE;
            int requestCount = orStream.getRequestCount();
            if (packageHead.getRequestCount() == requestCount && remainLen > 0) {
                getResult(cachedQuery, packageHead, remainLen);
            }
            if ((packageHead.getFlags() & AGENT) == 0) {
                break;
            }
        }
    }

    private void getResult(ORCachedQuery cachedQuery, ORPackageHead packageHead, int remainLen)
            throws SQLException, IOException {
        if (packageHead.getExecResult() != 0) {
            int offset = 0;
            orStream.receiveInteger4();
            offset += 4;
            orStream.receiveInteger2();
            offset += 2;
            orStream.receiveInteger2();
            offset += 2;
            orStream.receiveInteger2();
            offset += 2;
            orStream.receiveInteger2();
            offset += 2;
            byte[] errBytes = orStream.receive(remainLen - offset);
            int msgLen = 0;
            for (int i = 0; i < errBytes.length; i++) {
                if (errBytes[i] == 0) {
                    msgLen = i;
                    break;
                }
            }
            String errorMsg = new String(errBytes, 0, msgLen, orStream.getCharset());
            throw new PSQLException(GT.tr(errorMsg), PSQLState.CONNECTION_FAILURE);
        } else {
            if (packageHead.getExecCmd() == ORRequestCommand.FETCH) {
                orStream.receiveInteger4();
                int total = orStream.receiveInteger2();
                int remain = orStream.receiveChar();
                orStream.receiveChar();
                if (total > 0) {
                    List<int[]> valueLens = new ArrayList<>();
                    List<byte[][]> rows = new ArrayList<>();
                    this.getByteData(valueLens, rows, total);
                    boolean hasRemain = remain == 1;
                    cachedQuery.setNewData(total, valueLens, rows, hasRemain);
                }
            } else if (packageHead.getExecCmd() == ORRequestCommand.EXECUTE) {
                int queryMode = cachedQuery.getCtStatement().getQueryMode();
                if (queryMode == 2 || queryMode == 3) {
                    cachedQuery.getCtStatement().setUpdateCount(0);
                } else {
                    orStream.receiveInteger4();
                    int updateCount = orStream.receiveInteger4();
                    cachedQuery.getCtStatement().setUpdateCount(updateCount);
                    this.handleResult(cachedQuery, new ORField[0]);
                }
            } else {
                handleData(cachedQuery);
            }
        }
    }

    private void handleData(ORCachedQuery cachedQuery) throws SQLException, IOException {
        ORStatement stat = cachedQuery.getCtStatement();
        int id = orStream.receiveInteger2();
        stat.setMark(id);
        int statFlag = orStream.receiveInteger2();
        stat.setQueryFlag(statFlag & QUERY_FLAG_OPERATION);
        int queryMode = statFlag / QUERY_MODE_OPERATION & 0x0F;
        stat.setQueryMode(queryMode);
        int cols = orStream.receiveInteger2();
        byte[] ba = orStream.receive(2);
        int paramSize = orStream.bytesToShort(ba);

        for (int i = 0; i < paramSize * 2; i++) {
            orStream.receiveInteger4();
        }
        ORField[] fields = new ORField[cols];
        getFieldInfo(fields);
        if (queryMode == 2 || queryMode == 3) {
            stat.setUpdateCount(0);
        } else {
            orStream.receiveInteger4();
            int updateCount = orStream.receiveInteger4();
            stat.setUpdateCount(updateCount);
            handleResult(cachedQuery, fields);
        }
    }

    private void getFieldInfo(ORField[] fields) throws SQLException, IOException {
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new ORField();
            fields[i].setLength(orStream.receiveInteger2());
            fields[i].setPrecision(orStream.receiveChar());
            fields[i].setScale(orStream.receiveChar());
            int dbType = orStream.receiveInteger2();
            fields[i].setTypeInfo(ORDataType.getDataType(dbType));
            byte b = (byte) orStream.receiveChar();
            int nullable = (b & 1);
            fields[i].setNullableFlag(nullable);

            boolean isAutoIncrement = (b & 2) != 0;
            fields[i].setAutoIncrement(isAutoIncrement);
            orStream.receiveChar();
            orStream.receiveInteger2();
            int labelLen = orStream.receiveInteger2();
            fields[i].setLabelLen(labelLen);
            int byte4LabelLen = labelLen;
            if (labelLen % 4 != 0) {
                byte4LabelLen = labelLen + (4 - labelLen % 4);
            }
            byte[] label = orStream.receive(byte4LabelLen);
            String columnLabel = new String(Arrays.copyOf(label, labelLen), orStream.getCharset());
            fields[i].setColumnName(columnLabel);
        }
    }

    private void handleResult(ORCachedQuery cachedQuery, ORField[] fields) throws SQLException, IOException {
        byte[] totalBytes = orStream.receive(2);
        int total = orStream.bytesToShort(totalBytes);
        int remain = orStream.receiveChar();
        boolean hasRemain = remain == 1;
        orStream.receiveChar();
        orStream.receiveInteger2();
        orStream.receiveInteger2();
        List<int[]> valueLens = new ArrayList<>();
        List<byte[][]> rows = new ArrayList<>();
        if (fields.length > 0) {
            if (total > 0) {
                this.getByteData(valueLens, rows, total);
            }
            cachedQuery.handleResultRows(fields, valueLens, rows, hasRemain);
        }
    }

    private void getByteData(List<int[]> valueLens, List<byte[][]> rows, int total) throws IOException {
        for (int i = 0; i < total; i++) {
            getRow(valueLens, rows);
        }
    }

    private void getRow(List<int[]> valueLens, List<byte[][]> rows) throws IOException {
        orStream.receiveInteger2();
        int columns = orStream.receiveInteger2();
        int[] lens = new int[columns];
        byte[][] value = new byte[columns][];
        int lenOperation = 3;
        if (columns >= COLUMNS_THRESHOLD) {
            lenOperation += ((columns + 3 & LEN_OPERATION) / 4);
        }

        orStream.receiveChar();
        byte[] lenMark = orStream.receive(lenOperation);
        int index = 0;
        int colIndex = 0;
        while (true) {
            if (colIndex >= columns) {
                break;
            }
            int p = index * 4;
            int mark = lenMark[index];
            for (int k = p; k < p + 4; k++) {
                if (k >= columns) {
                    colIndex = k;
                    break;
                }
                int lenId = mark & 3;
                handleValue(lenId, lens, value, k);
                mark = mark >> 2;
                colIndex = k;
            }
            index++;
        }
        valueLens.add(lens);
        rows.add(value);
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
}
