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

import org.postgresql.Driver;
import org.postgresql.PGStatement;
import org.postgresql.core.ORBaseConnection;
import org.postgresql.core.types.PGBlob;
import org.postgresql.core.types.PGClob;
import org.postgresql.core.ORField;
import org.postgresql.core.ORDataType;
import org.postgresql.util.PSQLException;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLState;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.SQLXML;
import java.sql.RowId;
import java.sql.NClob;
import java.time.LocalTime;
import java.time.Instant;
import java.time.ZoneId;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Locale;
import java.util.UUID;

import java.io.InputStream;
import java.io.Reader;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;

/**
 * the execution resultSet of connecting oGRAC.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORResultSet extends PgResultSet {
    private static final int SEGMENT_LENGTH = 13;
    private static final int[] SEGMENT_SIZE = new int[]{10, 100, 1000, 10000, 32767};

    /**
     * statement
     */
    protected ORStatement statement;

    /**
     * fields
     */
    protected ORField[] orFields;

    /**
     * data rows
     */
    protected List<byte[][]> dataRows;

    /**
     * values length
     */
    protected List<int[]> valueLens;

    /**
     * has remaining rows
     */
    protected boolean hasRemain;
    private final int resultSetType;
    private final int resultsetconcurrency;
    private String sql;
    private TimeZone defaultTimeZone;
    private ORBaseConnection connection;
    private boolean isInsertRow = false;
    private int totalRows;
    private boolean isClosed;
    private int currentRow = -1;
    private Map<String, Integer> columnNameIndexMap;

    /**
     * resultSet constructor
     *
     * @param orStatement oGRAC statement
     * @param sql sql
     * @param fields column fields
     * @param valueLens value length
     * @param dataRows dataRows
     * @param hasRemain hasRemain
     * @param resultSetType resultSetType
     * @param resultSetConcurrency resultSetConcurrency
     * @throws SQLException if a database access error occurs
     */
    public ORResultSet(ORStatement orStatement, String sql, ORField[] fields, List<int[]> valueLens,
                       List<byte[][]> dataRows, boolean hasRemain, int resultSetType,
                       int resultSetConcurrency) throws SQLException {
        super();
        this.statement = orStatement;
        this.sql = sql;
        this.valueLens = valueLens;
        this.dataRows = dataRows;
        this.hasRemain = hasRemain;
        this.orFields = fields;
        this.totalRows = dataRows.size();
        this.resultSetType = resultSetType;
        this.resultsetconcurrency = resultSetConcurrency;
        if (orStatement.getConnection() instanceof ORBaseConnection) {
            this.connection = (ORBaseConnection) orStatement.getConnection();
        }
    }

    /**
     * resultSet constructor
     *
     * @param orStatement statement
     * @param fields fields
     * @param valueLens values length
     * @param dataRows data rows
     * @throws SQLException if a database access error occurs
     */
    public ORResultSet(ORStatement orStatement, ORField[] fields, List<int[]> valueLens,
                       List<byte[][]> dataRows) throws SQLException {
        super();
        this.currentRow = 0;
        this.statement = orStatement;
        this.valueLens = valueLens;
        this.dataRows = dataRows;
        this.orFields = fields;
        this.totalRows = dataRows.size();
        this.resultSetType = orStatement.getResultSetType();
        this.resultsetconcurrency = orStatement.getResultSetConcurrency();
        if (orStatement.getConnection() instanceof ORBaseConnection) {
            this.connection = (ORBaseConnection) orStatement.getConnection();
        }
    }

    /**
     * resultSet constructor
     *
     * @param orStatement statement
     * @throws SQLException if a database access error occurs
     */
    public ORResultSet(ORStatement orStatement) throws SQLException {
        this.statement = orStatement;
        this.resultSetType = orStatement.getResultSetType();
        this.resultsetconcurrency = orStatement.getResultSetConcurrency();
        if (orStatement.getConnection() instanceof ORBaseConnection) {
            this.connection = (ORBaseConnection) orStatement.getConnection();
        }
    }

    /**
     * get fields
     *
     * @return fields
     */
    public ORField[] getOrFields() {
        return orFields;
    }

    /**
     * get values length
     *
     * @return values length
     */
    public List<int[]> getValueLens() {
        return valueLens;
    }

    /**
     * update fetch data to the resultSet
     *
     * @param total rows total
     * @param valueLens value length
     * @param dataRows data rows
     * @param hasRemain is there still data available
     */
    public void setFetchInfo(int total, List<int[]> valueLens, List<byte[][]> dataRows, boolean hasRemain) {
        this.totalRows = total;
        this.valueLens = valueLens;
        this.hasRemain = hasRemain;
        this.dataRows = dataRows;
        this.currentRow = -1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkRsType();
        currentRow = -1;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return new ORArray();
        }

        ORArray arr = new ORArray();
        Object[] typeInfo = orFields[columnIndex - 1].getTypeInfo();
        ORField field = orFields[columnIndex - 1];
        Object value = getObject(columnIndex);

        arr.setField(field);
        arr.setType(ORDataType.getType(Integer.parseInt(typeInfo[1].toString())));
        arr.setValue(value);
        return arr;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        String value = getNumber(columnIndex);
        return new BigDecimal(value);
    }

    private int getSegmentSize(int segment) {
        int size = 1;
        while (segment >= SEGMENT_SIZE[size - 1]) {
            size++;
        }
        return size;
    }

    private String getNumber(int columnIndex) {
        byte[] byteValue = getByteValue(columnIndex);
        if ((byte) (byteValue[0] >> 1) == 0) {
            return "0";
        }
        StringBuilder value = new StringBuilder();
        boolean isMinus = byteValue[0] % 2 == 1;
        if (isMinus) {
            value.append('-');
        }
        int[] segments = new int[SEGMENT_LENGTH];
        int segmentCount = byteValue[0] / 2;
        int segment0 = connection.getORStream().bytesToShort(byteValue, 2);
        segments[0] = segment0;
        value.append(segment0);
        if (segmentCount > 1) {
            value.append(".");
        }
        int i = 2;
        while (i <= segmentCount) {
            int segment = connection.getORStream().bytesToShort(byteValue, i * 2);
            segments[i - 1] = segment;
            int size = getSegmentSize(segment);
            int scale = 0;
            while (scale < 4 - size) {
                value.append('0');
                scale++;
            }
            value.append(segment);
            i++;
        }

        int e = byteValue[1] * 4;
        if (byteValue[1] != 0) {
            value.append("E").append(e);
        }

        int round = segments[0];
        int mark = 0;
        if (round >= 10000) {
            mark = 5;
        } else if (round >= 1000) {
            mark = 4;
        } else if (round >= 100) {
            mark = 3;
        } else if (round >= 10) {
            mark = 2;
        } else {
            mark = 1;
        }
        int num = mark + e;
        String valueStr = value.toString();
        if (num <= 1 || num + byteValue[0] % 2 <= 40) {
            return new BigDecimal(valueStr).stripTrailingZeros().toPlainString();
        }

        return new BigDecimal(valueStr).toString();
    }

    @Override
    public boolean first() throws SQLException {
        checkRsType();
        if (totalRows <= 0) {
            return false;
        }
        currentRow = 0;
        return true;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }
        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        PGBlob blob = new PGBlob();
        byte[] byteValue = getByteValue(columnIndex);
        if (byteValue == null) {
            return null;
        }
        byte[] bs = Arrays.copyOf(byteValue, getLen(columnIndex));
        int dataLen = connection.getORStream().bytesToInt(bs);
        byte[] value = new byte[dataLen];
        System.arraycopy(bs, 12, value, 0, dataLen);
        blob.setBytes(1, value);
        return blob;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }
        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        Calendar calendar = cal == null ? getDefaultCalendar() : cal;
        byte[] valueBytes = getByteValue(columnIndex);
        long value = connection.getORStream().bytesToLong(valueBytes);
        calendar = getCalendar(valueBytes, calendar);
        Timestamp result = connection.getTimestampUtils().getTimestamp(value, calendar);
        return new Date(((java.util.Date) result).getTime());
    }

    private LocalTime getLocalTime(int columnIndex) {
        long value = connection.getORStream().bytesToLong(getByteValue(columnIndex));
        Instant instant = Instant.ofEpochMilli(value);
        return instant.atZone(ZoneId.systemDefault()).toLocalTime();
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    /**
     * create resultSetMetaData
     *
     * @return ResultSetMetaData
     */
    protected ResultSetMetaData createMetaData() {
        return new ORResultSetMetaData(this.orFields);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        Calendar calendar = cal == null ? getDefaultCalendar() : cal;
        byte[] valueBytes = getByteValue(columnIndex);
        long value = connection.getORStream().bytesToLong(valueBytes);
        calendar = getCalendar(valueBytes, calendar);
        return connection.getTimestampUtils().getTimestamp(value, calendar);
    }

    private String getTimestampTZ(int columnIndex, Calendar cal) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        Calendar calendar = cal == null ? getDefaultCalendar() : cal;
        byte[] valueBytes = getByteValue(columnIndex);
        long value = connection.getORStream().bytesToLong(valueBytes);
        Timestamp timestamp = connection.getTimestampUtils().getTimestamp(value, calendar);
        if (valueBytes.length >= 10) {
            int timeZoneId = connection.getORStream().bytesToShortOffset(valueBytes, 8);
            String tz = connection.getTimestampUtils().getOffsetTimeZone(timeZoneId);
            return timestamp.toString() + " " + tz;
        }
        return timestamp.toString();
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        if (isInsertRow || hasRemain) {
            return false;
        }

        return currentRow >= totalRows;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRow + 1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkRsType();
        if (totalRows > 0) {
            currentRow = totalRows;
        }
    }

    private void checkRsType() throws SQLException {
        checkClosed();
        if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("resultSetType requires a scrollable ResultSet,"
                    + " but it is ResultSet.TYPE_FORWARD_ONLY");
        }
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkRsType();
        int internalIndex;
        if (row == 0) {
            beforeFirst();
            return false;
        }

        final int rowsSize = totalRows;
        // if index<0, count from the end of the result set, but check
        // to be sure that it is not beyond the first index
        if (row < 0) {
            if (row >= -rowsSize) {
                internalIndex = rowsSize + row;
            } else {
                beforeFirst();
                return false;
            }
        } else {
            // must be the case that index>0,
            // find the correct place, assuming that
            // the index is not too large
            if (row <= rowsSize) {
                internalIndex = row - 1;
            } else {
                afterLast();
                return false;
            }
        }

        currentRow = internalIndex;
        return true;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        if (isInsertRow) {
            return false;
        }
        return currentRow <= -1;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkRsType();
        if (isInsertRow) {
            throw new PSQLException("Can't use relative move methods while on the insert row.",
                    PSQLState.INVALID_CURSOR_STATE);
        }
        return absolute(this.currentRow + 1 + rows);
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        if (isInsertRow || hasRemain) {
            return false;
        }
        return currentRow == totalRows - 1;
    }

    private Calendar getDefaultCalendar() {
        TimestampUtils timestampUtils = connection.getTimestampUtils();
        if (timestampUtils.hasFastDefaultTimeZone()) {
            return timestampUtils.getSharedCalendar(null);
        }
        Calendar sharedCalendar = timestampUtils.getSharedCalendar(defaultTimeZone);
        if (defaultTimeZone == null) {
            defaultTimeZone = sharedCalendar.getTimeZone();
        }
        return sharedCalendar;
    }

    @Override
    public synchronized void cancelRowUpdates() throws SQLException {
        throw new PSQLException("Can't call cancelRowUpdates().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public boolean last() throws SQLException {
        checkRsType();
        isInsertRow = false;
        if (totalRows <= 0) {
            return false;
        }
        currentRow = totalRows - 1;
        return true;
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new PSQLException("Can't call moveToInsertRow().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public synchronized void deleteRow() throws SQLException {
        throw new PSQLException("Can't call deleteRow().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public synchronized void insertRow() throws SQLException {
        throw new PSQLException("Can't call insertRow().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        if (isInsertRow) {
            return false;
        }
        return currentRow == 0;
    }

    @Override
    public int findColumn(String columnName) throws SQLException {
        checkClosed();
        if (columnNameIndexMap == null) {
            columnNameIndexMap = new HashMap<>(orFields.length * 2);
            for (int i = 0; i < orFields.length; i++) {
                columnNameIndexMap.put(orFields[i].getColumnName(), i + 1);
            }
        }

        Integer columnIndex = columnNameIndexMap.get(columnName);
        if (columnIndex != null) {
            return columnIndex;
        }

        columnIndex = columnNameIndexMap.get(columnName.toLowerCase(Locale.US));
        if (columnIndex != null) {
            columnNameIndexMap.put(columnName, columnIndex);
            return columnIndex;
        }

        columnIndex = columnNameIndexMap.get(columnName.toUpperCase(Locale.US));
        if (columnIndex != null) {
            columnNameIndexMap.put(columnName, columnIndex);
            return columnIndex;
        }

        throw new PSQLException(
                GT.tr("The column name {0} was not found in this ResultSet.", columnName),
                PSQLState.UNDEFINED_COLUMN);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        Calendar calendar = cal == null ? getDefaultCalendar() : cal;
        byte[] valueBytes = getByteValue(columnIndex);
        long value = connection.getORStream().bytesToLong(valueBytes);
        calendar = getCalendar(valueBytes, calendar);
        Timestamp result = connection.getTimestampUtils().getTimestamp(value, calendar);
        return new Time(((java.util.Date) result).getTime());
    }

    private Calendar getCalendar(byte[] valueBytes, Calendar calendar) {
        if (calendar.getTimeZone().getRawOffset() == TimeZone.getDefault().getRawOffset()
                && valueBytes.length >= 10) {
            int timeZoneId = connection.getORStream().bytesToShortOffset(valueBytes, 8);
            String tz = "GMT" + connection.getTimestampUtils().getOffsetTimeZone(timeZoneId);
            return Calendar.getInstance(TimeZone.getTimeZone(tz));
        }
        return calendar;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        byte[] value = Arrays.copyOf(getByteValue(columnIndex), valueLen);
        int dataLen = connection.getORStream().bytesToInt(value);
        byte[] data = new byte[dataLen];
        System.arraycopy(value, 12, data, 0, dataLen);
        String str = new String(data, connection.getORStream().getCharset());
        PGClob clob = new PGClob();
        clob.setString(1, str);
        return clob;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new PSQLException("Can't call moveToCurrentRow().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public synchronized void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw new PSQLException("Can't call updateAsciiStream().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public boolean previous() throws SQLException {
        checkRsType();
        if (isInsertRow) {
            throw new PSQLException("Can't use relative move methods while on the insert row.",
                    PSQLState.INVALID_CURSOR_STATE);
        }
        if (currentRow < 1) {
            currentRow = -1;
            return false;
        }
        currentRow--;
        return true;
    }

    @Override
    public synchronized void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException {
        updateValue(columnIndex, x);
    }

    @Override
    public synchronized void updateNull(int columnIndex) throws SQLException {
        updateValue(columnIndex, null);
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new PSQLException("Can't call refreshRow().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public synchronized void updateRow() throws SQLException {
        throw new PSQLException("Can't call updateRow().", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        boolean hasNext = this.hasNext();
        if (!hasNext && hasRemain) {
            try {
                statement.fetch(this, sql);
            } catch (IOException e) {
                throw new PSQLException("fetch more rows failed.", PSQLState.IO_ERROR);
            }
            hasNext = hasNext();
        }
        return hasNext;
    }

    /**
     * has next
     *
     * @return has next
     */
    protected boolean hasNext() {
        boolean hasNext = false;
        if (currentRow + 1 < totalRows) {
            hasNext = true;
            currentRow++;
        }

        if (currentRow > 0) {
            dataRows.set(currentRow - 1, null);
        }
        return hasNext;
    }

    @Override
    public void close() throws SQLException {
        dataRows = null;
        valueLens = null;
        orFields = null;
        isClosed = true;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return 0;
        }
        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return 0;
        }

        long value = handleNum(columnIndex);
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new SQLException("value " + value + " is out of the range of the integer.");
        }
        return (int) value;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag || this.dataRows == null) {
            return null;
        }
        Object[] type = this.orFields[columnIndex - 1].getTypeInfo();
        int sqlType = Integer.parseInt(type[2].toString());
        switch (sqlType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return String.valueOf(getInt(columnIndex));
            case Types.BIGINT:
                return String.valueOf(getLong(columnIndex));
            case Types.NUMERIC:
                return String.valueOf(getBigDecimal(columnIndex));
            case Types.DECIMAL:
                return String.valueOf(getDecimal(columnIndex));
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return String.valueOf(getDouble(columnIndex));
            case Types.DATE:
                return String.valueOf(getDate(columnIndex));
            case Types.TIME:
                return String.valueOf(getTime(columnIndex));
            case Types.TIMESTAMP:
                return String.valueOf(getTimestamp(columnIndex, null));
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return String.valueOf(getTimestampTZ(columnIndex, null));
            case Types.CLOB:
                Clob value = this.getClob(columnIndex);
                return value.getSubString(1, (int) value.length());
            case Types.BLOB:
                Blob blob = getBlob(columnIndex);
                return new String(blob.getBytes(1L, (int) blob.length()), connection.getORStream().getCharset());
            case Types.BOOLEAN:
                Object obj = this.getBoolean(columnIndex);
                return obj.toString();
            case Types.OTHER:
                return getOther(columnIndex, type);
            default:
                int valueLen = getLen(columnIndex);
                if (valueLen < 0) {
                    return null;
                }
                byte[] bs = Arrays.copyOf(getByteValue(columnIndex), valueLen);
                return new String(bs, this.connection.getORStream().getCharset());
        }
    }

    private String getOther(int columnIndex, Object[] type) throws SQLException {
        int dbtype = Integer.parseInt(type[1].toString());
        switch (dbtype) {
            case ORDataType.RAW:
                byte[] rawbs = Arrays.copyOf(getByteValue(columnIndex), getLen(columnIndex));
                return byteToString(rawbs);
            case ORDataType.DATE_YEAR_MONTH:
                byte[] ymbs = getByteValue(columnIndex);
                int ymTime = connection.getORStream().bytesToInt(ymbs);
                return ORTimestampUtils.getDateYearMonth(ymTime);
            case ORDataType.DATE_DAY_HMS:
                byte[] bs = getByteValue(columnIndex);
                long dhTime = connection.getORStream().bytesToLong(bs);
                return ORTimestampUtils.getDateDayHMS(dhTime);
            default:
                throw new SQLException("dbType " + dbtype + " is not supported.");
        }
    }

    private String byteToString(byte[] data) {
        if (data.length == 0) {
            return "";
        }
        StringBuffer value = new StringBuffer();
        for (int i = 0; i < data.length; i++) {
            int flag = data[i] & 255;
            String h1 = Integer.toHexString(flag >>> 4).toUpperCase();
            value.append(h1);
            String h2 = Integer.toHexString(flag & 15).toUpperCase();
            value.append(h2);
        }
        return value.toString();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return 0;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return 0;
        }

        long value = handleNum(columnIndex);
        if (value > Byte.MAX_VALUE || value < Byte.MIN_VALUE) {
            throw new SQLException("value " + value + " is out of the range of the byte.");
        }
        return (byte) value;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return 0;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return 0L;
        }
        return handleNum(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return false;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return false;
        }
        int value = connection.getORStream().bytesToInt(getByteValue(columnIndex));
        BigDecimal number = new BigDecimal(value);
        return number.floatValue() != 0.0F;
    }

    private long handleNum(int columnIndex) throws SQLException {
        ORField fieldDef = this.orFields[columnIndex - 1];
        int dbType = Integer.parseInt(fieldDef.getTypeInfo()[1].toString());
        byte[] bv = getByteValue(columnIndex);
        switch (dbType) {
            case ORDataType.INT:
                return connection.getORStream().bytesToInt(bv);
            case ORDataType.BIGINT:
            case ORDataType.REF_CURSOR:
                return connection.getORStream().bytesToLong(bv);
            case ORDataType.UINT:
                return connection.getORStream().bytesToUint(bv);
            case ORDataType.REAL:
                double dv = getReal(columnIndex);
                return Integer.parseInt(String.valueOf(dv));
            case ORDataType.NUMBER2:
                String decimal = getDecimal(columnIndex);
                return Integer.parseInt(decimal);
            case ORDataType.NUMERIC:
            case ORDataType.DECIMAL:
                String num = getNumber(columnIndex);
                return Integer.parseInt(num);
            default:
                throw new SQLException("conversion to int type from " + fieldDef.getTypeInfo()[0]
                        + " is not supported.");
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return 0;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return 0.0;
        }
        Object[] type = this.orFields[columnIndex - 1].getTypeInfo();
        int sqlType = Integer.parseInt(type[2].toString());
        switch (sqlType) {
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                return getReal(columnIndex);
            case Types.DECIMAL:
                return Double.valueOf(getDecimal(columnIndex));
            case Types.NUMERIC:
                String num = getNumber(columnIndex);
                return Double.valueOf(num);
            default:
                throw new SQLException("conversion to double type from " + type[0] + " is not supported.");
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return 0;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return 0;
        }

        long value = handleNum(columnIndex);
        if (value > Short.MAX_VALUE || value < Short.MIN_VALUE) {
            throw new SQLException("value " + value + " is out of the range of the short.");
        }
        return (short) value;
    }

    private double getReal(int columnIndex) {
        byte[] bs = getByteValue(columnIndex);
        long value = connection.getORStream().bytesToLong(bs);
        return Double.longBitsToDouble(value);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        Object[] type = this.orFields[columnIndex - 1].getTypeInfo();
        int sqltype = Integer.parseInt(type[2].toString());
        switch (sqltype) {
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                double value = getReal(columnIndex);
                return new BigDecimal(String.valueOf(value));
            case Types.DECIMAL:
                String decimal = getDecimal(columnIndex);
                return new BigDecimal(decimal);
            case Types.NUMERIC:
                String num = getNumber(columnIndex);
                return new BigDecimal(num);
            default:
                throw new SQLException("conversion to double type from " + type[0] + " is not supported.");
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        Object[] type = this.orFields[columnIndex - 1].getTypeInfo();
        int sqlType = Integer.parseInt(type[1].toString());
        switch (sqlType) {
            case ORDataType.BLOB:
                Blob blob = getBlob(columnIndex);
                return blob.getBytes(1, (int) blob.length());
            case ORDataType.CLOB:
                Clob clob = getClob(columnIndex);
                String value = clob.getSubString(1, (int) clob.length());
                return value.getBytes(connection.getORStream().getCharset());
            case ORDataType.IMAGE:
            case ORDataType.BINARY:
            case ORDataType.VARBINARY:
            case ORDataType.RAW:
            case ORDataType.TEXT:
            case ORDataType.CHAR:
            case ORDataType.VARCHAR:
                return Arrays.copyOf(getByteValue(columnIndex), getLen(columnIndex));
            default:
                throw new PSQLException("conversion to bytes value from " + type[3] + " is not supported.",
                        PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
        }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        Object[] type = this.orFields[columnIndex - 1].getTypeInfo();
        return getObject(columnIndex, type);
    }

    private Object getObject(int columnIndex, Object[] type) throws SQLException {
        int sqlType = Integer.parseInt(type[2].toString());
        switch (sqlType) {
            case Types.SQLXML:
                return getSQLXML(columnIndex);
            case Types.BOOLEAN:
                return getBoolean(columnIndex);
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return getInt(columnIndex);
            case Types.BIGINT:
                return getLong(columnIndex);
            case Types.NUMERIC:
                return getBigDecimal(columnIndex);
            case Types.DECIMAL:
                return getDecimal(columnIndex);
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return getDouble(columnIndex);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return getString(columnIndex);
            case Types.TIME:
                return getTime(columnIndex);
            case Types.DATE:
            case Types.TIMESTAMP:
                return getTimestamp(columnIndex);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return getBytes(columnIndex);
            case Types.CLOB:
                return getClob(columnIndex);
            case Types.BLOB:
                return getBlob(columnIndex);
            default:
                return getString(columnIndex);
        }
    }

    private String getDecimal(int columnIndex) throws SQLException {
        byte[] valueBytes = getByteValue(columnIndex);
        if (valueBytes.length <= 1) {
            return "0";
        }
        int flag = valueBytes[0] & 0xff;
        int valueLen = getLen(columnIndex);
        if (valueLen <= 1) {
            return "0";
        }
        if (valueLen > 22) {
            throw new SQLException("value length: " + valueLen + "  is out of range.");
        }

        boolean isMinus = (((flag >> 7) & 1) == 0);
        StringBuilder value = getBaseValue(isMinus, valueBytes, valueLen);
        int zeroExp;
        if (isMinus) {
            zeroExp = 0x3e - flag;
        } else {
            zeroExp = flag - 0xc1;
        }
        int mul = zeroExp * 2;
        value.append('E');
        if (mul > 0) {
            value.append('+');
        }
        value.append(mul);

        int mark = mul;
        if (valueBytes[1] >= 10) {
            mark += 1;
        }
        int precision = getPrecision(valueLen, valueBytes);
        int sign = isMinus ? 0 : 1;
        int lenFactor = mark - sign + 2;
        int lenMark = precision - mark - sign + 1;
        if ((mark >= -6 || lenMark <= 40) && (mark <= 0 || lenFactor <= 40)) {
            return new BigDecimal(value.toString()).stripTrailingZeros().toPlainString();
        }
        return new BigDecimal(value.toString()).toString();
    }

    private StringBuilder getBaseValue(boolean isMinus, byte[] valueBytes, int valueLen) {
        StringBuilder value = new StringBuilder();
        if (isMinus) {
            value.append('-');
        }

        int i = 1;
        value.append(valueBytes[i++]);
        if (i < valueLen) {
            value.append('.');
        }
        while (i < valueLen) {
            if (valueBytes[i] < 10) {
                value.append(0).append(valueBytes[i]);
            } else {
                value.append(valueBytes[i]);
            }
            i++;
        }
        return value;
    }

    private int getPrecision(int valueLen, byte[] valueBytes) {
        int result = 0;
        int i = valueLen - 1;
        while (i > 1) {
            if (valueBytes[i] > 0) {
                result = result + 2 * i;
                if (valueBytes[i] % 10 == 0) {
                    result -= 1;
                }
                break;
            }
            i--;
        }

        if (i > 0) {
            if (valueBytes[1] >= 10) {
                return result + 2;
            } else {
                return result + 1;
            }
        }
        if (valueBytes[1] < 10 || valueBytes[1] % 10 == 0) {
            return 1;
        } else {
            return 2;
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return 0;
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return 0.0F;
        }
        String num = String.valueOf(getReal(columnIndex));
        return Float.valueOf(num);
    }

    private byte[] getByteValue(int columnIndex) {
        if (this.dataRows == null) {
            return null;
        }
        return this.dataRows.get(this.currentRow)[columnIndex - 1];
    }

    private int getLen(int columnIndex) {
        if (this.dataRows == null) {
            return 0;
        }
        return this.valueLens.get(currentRow)[columnIndex - 1];
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex, null);
    }

    private String getORType(int column) {
        return this.orFields[column - 1].getTypeInfo()[0].toString();
    }

    @Override
    protected int getSQLType(int column) {
        Object type = this.orFields[column - 1].getTypeInfo()[2];
        return Integer.parseInt(type.toString());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex, null);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getSQLXML(int)");
    }

    @Override
    protected void updateValue(int columnIndex, Object value) throws SQLException {
        throw new PSQLException("Can‘t update the ResultSet.", PSQLState.INVALID_CURSOR_STATE);
    }

    @Override
    protected void checkClosed() throws SQLException {
        if (isClosed) {
            throw new PSQLException("This ResultSet is closed.", PSQLState.OBJECT_NOT_IN_STATE);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex, null);
    }

    @Override
    protected void checkColumnIndex(int column) throws SQLException {
        if (column < 1 || column > this.orFields.length) {
            throw new PSQLException(
                    GT.tr("The column index is out of range: {0}, number of columns: {1}.",
                            column, this.orFields.length),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw Driver.notImplemented(this.getClass(), "getURL(int)");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }
        return new ByteArrayInputStream(getString(columnIndex).getBytes(connection.getORStream().getCharset()));
    }

    @Override
    protected void checkResultSet(int column) throws SQLException {
        checkClosed();
        if (this.dataRows == null) {
            throw new PSQLException("ResultSet not positioned properly, perhaps you need to call next.",
                    PSQLState.INVALID_CURSOR_STATE);
        }
        checkColumnIndex(column);
        wasNullFlag = (dataRows.get(currentRow)[column - 1] == null);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }

        byte[] bs = getBytes(columnIndex);
        if (bs != null) {
            return new ByteArrayInputStream(bs);
        }
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkResultSet(columnIndex);
        if (wasNullFlag) {
            return null;
        }
        try {
            return new ByteArrayInputStream(getString(columnIndex).getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new PSQLException(GT.tr("The JVM claims not to support the encoding: {0}", "UTF-8"),
                    PSQLState.UNEXPECTED_ERROR, e);
        }
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw org.postgresql.Driver.notImplemented(this.getClass(), "getNCharacterStream(int)");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("type is null");
        }

        int valueLen = getLen(columnIndex);
        if (valueLen < 0) {
            return null;
        }
        int sqlType = getSQLType(columnIndex);
        if (type == BigDecimal.class) {
            return getBigDecimal(type, sqlType, columnIndex);
        } else if (type == String.class) {
            return getString(type, sqlType, columnIndex);
        } else if (type == Boolean.class) {
            return getBoolean(type, sqlType, columnIndex);
        } else if (type == Short.class) {
            return getShort(type, sqlType, columnIndex);
        } else if (type == Integer.class) {
            return getInt(type, sqlType, columnIndex);
        } else if (type == Long.class) {
            return getLong(type, sqlType, columnIndex);
        } else if (type == BigInteger.class) {
            return getBigInteger(type, sqlType, columnIndex);
        } else if (type == Float.class) {
            return getFloat(type, sqlType, columnIndex);
        } else if (type == Double.class) {
            return getDouble(type, sqlType, columnIndex);
        } else if (type == Date.class) {
            return getDate(type, sqlType, columnIndex);
        } else if (type == Time.class) {
            return getTime(type, sqlType, columnIndex);
        } else if (type == Timestamp.class) {
            return getTimestamp(type, sqlType, columnIndex);
        } else if (type == Calendar.class) {
            return getCalendar(type, sqlType, columnIndex);
        } else if (type == Blob.class) {
            return getBlob(type, sqlType, columnIndex);
        } else if (type == Clob.class) {
            return getClob(type, sqlType, columnIndex);
        } else if (type == java.util.Date.class) {
            return getUtilDate(type, sqlType, columnIndex);
        } else if (type == Array.class) {
            return getArray(type, sqlType, columnIndex);
        } else if (type == SQLXML.class) {
            return getSQLXML(type, sqlType, columnIndex);
        } else if (type == UUID.class) {
            return type.cast(getObject(columnIndex));
        } else if (type == LocalDate.class) {
            return getLocalDate(type, sqlType, columnIndex);
        } else if (type == LocalTime.class) {
            return getLocalTime(type, sqlType, columnIndex);
        } else if (type == LocalDateTime.class) {
            return getLocalDateTime(type, sqlType, columnIndex);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getBigDecimal(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.NUMERIC || sqlType == Types.DECIMAL) {
            return type.cast(getBigDecimal(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getString(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.CHAR || sqlType == Types.VARCHAR) {
            return type.cast(getString(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getBoolean(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.BOOLEAN || sqlType == Types.BIT) {
            boolean booleanValue = getBoolean(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(booleanValue);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getShort(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.SMALLINT) {
            short shortValue = getShort(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(shortValue);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getInt(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.INTEGER || sqlType == Types.SMALLINT) {
            int intValue = getInt(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(intValue);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getLong(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.BIGINT) {
            long longValue = getLong(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(longValue);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getBigInteger(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.BIGINT) {
            long longValue = getLong(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(BigInteger.valueOf(longValue));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getFloat(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
            float floatValue = getFloat(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(floatValue);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getDouble(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.REAL || sqlType == Types.FLOAT || sqlType == Types.DOUBLE) {
            double doubleValue = getDouble(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(doubleValue);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getDate(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.DATE) {
            return type.cast(getDate(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getTime(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.TIME) {
            return type.cast(getTime(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getTimestamp(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.TIMESTAMP || sqlType == Types.TIME_WITH_TIMEZONE) {
            return type.cast(getTimestamp(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getCalendar(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.TIMESTAMP
                || sqlType == Types.TIME_WITH_TIMEZONE) {
            Timestamp timestampValue = getTimestamp(columnIndex);
            if (wasNull()) {
                return null;
            }
            Calendar calendar = Calendar.getInstance(getDefaultCalendar().getTimeZone());
            calendar.setTimeInMillis(timestampValue.getTime());
            return type.cast(calendar);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getBlob(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.BIGINT) {
            return type.cast(getBlob(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getClob(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.CLOB || sqlType == Types.BIGINT) {
            return type.cast(getClob(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getUtilDate(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.TIMESTAMP) {
            Timestamp timestamp = getTimestamp(columnIndex);
            if (wasNull()) {
                return null;
            }
            return type.cast(new java.util.Date(timestamp.getTime()));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getArray(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.ARRAY) {
            return type.cast(getArray(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getSQLXML(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.SQLXML) {
            return type.cast(getSQLXML(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, sqlType),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getLocalDate(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.DATE) {
            Date dateValue = getDate(columnIndex);
            if (dateValue == null) {
                return null;
            }
            long time = dateValue.getTime();
            if (time == PGStatement.DATE_POSITIVE_INFINITY) {
                return type.cast(LocalDate.MAX);
            }
            if (time == PGStatement.DATE_NEGATIVE_INFINITY) {
                return type.cast(LocalDate.MIN);
            }
            return type.cast(dateValue.toLocalDate());
        } else if (sqlType == Types.TIMESTAMP) {
            Timestamp value = getTimestamp(columnIndex);
            if (value == null) {
                return null;
            }
            return type.cast(value);
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, getORType(columnIndex)),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getLocalTime(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.TIME) {
            return type.cast(getLocalTime(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, getORType(columnIndex)),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    private <T> T getLocalDateTime(Class<T> type, int sqlType, int columnIndex) throws SQLException {
        if (sqlType == Types.TIMESTAMP) {
            return type.cast(getTimestamp(columnIndex));
        } else {
            throw new PSQLException(GT.tr("conversion to {0} from {1} not supported", type, getORType(columnIndex)),
                    PSQLState.INVALID_PARAMETER_VALUE);
        }
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw org.postgresql.Driver.notImplemented(this.getClass(), "getNClob(int)");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw org.postgresql.Driver.notImplemented(this.getClass(), "getRowId(int)");
    }
}