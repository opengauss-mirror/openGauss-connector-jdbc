/*
 * Copyright (c) 2004, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.Oid;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.csv.CSVReader;
import org.postgresql.util.csv.CSVReaderBuilder;
import org.postgresql.util.csv.CSVReaderNullFieldIndicator;
import org.postgresql.util.csv.CSVWriter;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @Projecet pgjdbc
 * @Package org.postgresql.jdbc
 * @Class PGStructAttrsConverter
 * @Description: Utility class for converting attributes properties in PGStruct objects
 * <p>
 * Convert Object[] attributes to String sttrsValue,
 * and convert String attrsValue to Object[] attributes
 * </p>
 */
public class PGStructAttrsConverter {
    private static final String QUOTES = "\"";
    private static final char BACKSLASH_CHAR = '\\';
    private static final String BACKSLASH_STRING = "\\";
    private static final String DOUBLE_BACKSLASH_STRING = "\\\\";

    /**
     * Convert Object[] attributes to String sttrsValue, taking into account the configuration switches of standardConformingStrings and supportsESringSyntax.
     *
     * @param attributes object array of attribute
     * @throws SQLException if something wrong happens
     */
    public static String convertAttributes(Object[] attributes) throws SQLException {
        if (attributes == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        sb.append("(");

        String[] dataArray = new String[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            Object attrObj = attributes[i];
            if (attrObj instanceof Struct) {
                Struct struct = (Struct) attrObj;
                String attrValue = struct.toString();
                dataArray[i] = attrValue;
            } else if (attributes[i] instanceof Array) {
                String attrsValue = attributes[i].toString();
                dataArray[i] = attrsValue;
            } else {
                // Handle common attributes
                String attrValue = attributes[i].toString();
                dataArray[i] = attrValue;
            }
        }

        try {
            // Format the attributeValue object using open csv.
            StringWriter writer = new StringWriter();
            CSVWriter csvWriter = new CSVWriter(writer);
            csvWriter.writeNext(dataArray);
            csvWriter.flush();
            String attributeStr = writer.toString().trim();
            // Handling backslash characters
            attributeStr = processBackslashChar(attributeStr);
            sb.append(attributeStr);
        } catch (IOException ioe) {
            throw new PSQLException("Invalid character data was found. CSVWriter write text error, ", PSQLState.DATA_ERROR, ioe);
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Parse the attribute list in the Struct object according to the literal value of attributes.
     * To convert String attributes into Object[] attributes, it needs to be encapsulated in combination with att_type_id in the system view.
     *
     * @param conn             a database connection
     * @param elementOIDs      the list of  datatype
     * @param attributesString the string value of attribute
     * @throws SQLException if something wrong happens
     */
    public static Object[] parseAttributes(BaseConnection conn, List<Integer> elementOIDs, String attributesString) throws SQLException {
        String quote = QUOTES;
        List<Object> attributeList = new ArrayList();
        // Remove the surrounding brackets and process the attribute value within the brackets.
        if (attributesString.startsWith("(") && attributesString.endsWith(")")) {
            attributesString = attributesString.substring(1, attributesString.length() - 1);
        }
        // Parse attributesString into attributeValueArray.
        String[] attributeValueArray = parseAttributeValueArray(attributesString);
        for (int i = 0; i < attributeValueArray.length; i++) {
            String attributeValue = attributeValueArray[i];
            int oid = elementOIDs.get(i);
            int sqlType = conn.getTypeInfo().getSQLType(oid);
            if (sqlType == Types.STRUCT) {
                String objectValue = attributeValue;
                // Handle double backslashes in attributeValue.
                objectValue = processDoubleBackslashStr(objectValue);
                attributeList.add(new PGStruct(conn, oid, objectValue));
            } else if (sqlType == Types.ARRAY) {
                String objectValue = attributeValue;
                attributeList.add(new PgArray(conn, oid, objectValue));
            } else {
                String objectValue = attributeValue;
                if (!objectValue.isEmpty() && objectValue.length() > 1
                        && objectValue.startsWith(quote) && objectValue.endsWith(quote)) {
                    objectValue = objectValue.substring(1, objectValue.length() - 1);
                }
                Object obj = getObject(conn, objectValue, oid, sqlType);
                attributeList.add(obj);
            }
        }
        return attributeList.toArray();
    }

    /**
     * Use the open CSV tool to parse the attribute object array from attributesString
     *
     * @param attributesString the string value of attribute
     * @throws PSQLException if something wrong happens
     */
    public static String[] parseAttributeValueArray(String attributesString) throws PSQLException {
        if (attributesString.isEmpty()) {
            return new String[0];
        }
        try {
            StringReader strReader = new StringReader(attributesString);
            CSVReader csvReader = new CSVReaderBuilder(strReader)
                    .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS).build();
            return csvReader.readNext();
        } catch (IOException ioe) {
            throw new PSQLException("Invalid character data was found. csvReader read text error, ", PSQLState.DATA_ERROR, ioe);
        }
    }

    /**
     * Convert to Object object of corresponding type according to oid and att_type_id.
     *
     * @param connection a database connection
     * @param text       the value of data
     * @param oid        type oid
     * @param type       the jdbc type
     * @throws SQLException if something wrong happens
     */
    private static Object getObject(BaseConnection connection, String text, int oid, int type) throws SQLException {
        switch (type) {
            case Types.INTEGER:
            case Types.SMALLINT:
                return PgResultSet.toInt(text);
            case Types.BIGINT:
                return PgResultSet.toLong(text);
            case Types.NUMERIC:
                return PgResultSet.toBigDecimal(text);
            case Types.REAL:
                return PgResultSet.toFloat(text);
            case Types.DOUBLE:
                return PgResultSet.toDouble(text);
            case Types.DATE:
                if (oid == Oid.JSONB_ARRAY) {
                    return text;
                }
                return connection.getTimestampUtils().toDate(null, text);
            case Types.TIME:
                if (oid == Oid.JSONB_ARRAY) {
                    return text;
                }
                return connection.getTimestampUtils().toTime(null, text);
            case Types.TIMESTAMP:
                if (oid == Oid.JSONB_ARRAY) {
                    return text;
                }
                return connection.getTimestampUtils().toTimestamp(null, text);
            default:
                return text;
        }
    }

    /**
     * Handle backslashes, replace 1 backslashes with 2 backslashes
     *
     * @param attributeValue the string value of attribute
     */
    private static String processBackslashChar(String attributeValue) {
        if (attributeValue.contains(BACKSLASH_STRING)) {
            StringBuilder sb = new StringBuilder();
            char[] charArray = attributeValue.toCharArray();
            for (char ch : charArray) {
                if (ch == BACKSLASH_CHAR) {
                    sb.append(DOUBLE_BACKSLASH_STRING);
                } else {
                    sb.append(ch);
                }
            }
            return sb.toString();
        }
        return attributeValue;
    }

    /**
     * Handle backslashes, replace 2 backslashes with 1 backslashes
     *
     * @param attributeValue the string value of attribute
     */
    private static String processDoubleBackslashStr(String attributeValue) {
        if (attributeValue.contains(DOUBLE_BACKSLASH_STRING)) {
            attributeValue = attributeValue.replace(DOUBLE_BACKSLASH_STRING, BACKSLASH_STRING);
        }
        return attributeValue;
    }
}
