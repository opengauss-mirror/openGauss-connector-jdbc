/*
 * Copyright (c) 2004, openGauss Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.Oid;
import org.postgresql.core.Utils;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final char BACKSLASH_CHARACTER = '\\';
    private static final char QUOTES_CHARACTER = '"';
    private static final char COMMA_CHARACTER = ',';
    private static final String COMMA = ",";

    /**
     * @Params
     * @Return
     * @Exception
     * @Description: Convert Object[] attributes to String sttrsValue, taking into account the configuration switches of standardConformingStrings and supportsESringSyntax.
     */
    public static String convertAttributes(Object[] attributes, boolean standardConformingStrings, boolean supportsEStringSyntax) throws SQLException {
        if (attributes == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        sb.append("(");

        for (int i = 0; i < attributes.length; i++) {
            if (i > 0) {
                sb.append(COMMA);
            }

            if (attributes[i] == null) {
                continue;
            }

            if (attributes[i] instanceof Number) {
                sb.append(attributes[i]);
                continue;
            }

            Object attrObj = attributes[i];
            String formattedAttrValue = null;
            if (attrObj instanceof Struct) {
                Struct struct = (Struct) attrObj;
                String attrValue = struct.toString();
                formattedAttrValue = formatterComplexityAttribute(attrValue, QUOTES);
            } else if (attributes[i] instanceof Array) {
                String attrsValue = attributes[i].toString();
                formattedAttrValue = formatterComplexityAttribute(attrsValue, QUOTES);
            } else {
                // Handle common attributes
                String attrValue = attributes[i].toString();
                formattedAttrValue = formatterNormalAttribute(attrValue, QUOTES, standardConformingStrings, supportsEStringSyntax);
            }
            sb.append(QUOTES).append(formattedAttrValue).append(QUOTES);
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * @Params
     * @Return
     * @Exception
     * @Description: Methods for dealing with ordinary Object objects
     */
    private static String formatterNormalAttribute(String attrValue, String quote, boolean standardConformingStrings, boolean supportsEStringSyntax) throws SQLException {
        // Handle \ character
        boolean hasBackslash = attrValue.indexOf(BACKSLASH_CHARACTER) != -1;
        StringBuilder sb = new StringBuilder();
        if (hasBackslash && !standardConformingStrings && supportsEStringSyntax) {
            sb.append('E');
        }

        // escape codes
        // No E'..' here since escapeLiteral escapes all things and it does not use \123 kind of
        sb = Utils.escapeLiteral(sb, attrValue, standardConformingStrings);

        // If the escaped string contains " , replace it with ""
        String formattedValue = sb.toString();
        if (formattedValue.contains(QUOTES)) {
            String doubleQuotes = getFormatterQuotesByLevel(2, quote);
            formattedValue.replaceAll(quote, doubleQuotes);
        }
        return formattedValue;
    }

    /**
     * @MethodName:
     * @Params
     * @Return
     * @Exception
     * @Description: Methods for handling complex objects, such as PGStruct, PgArray
     */
    private static String formatterComplexityAttribute(String attrValue, String quote) {
        if (attrValue.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        char[] attrCharArray = attrValue.toCharArray();

        List<Integer> startStack = new ArrayList<>();
        List<Integer> endStack = new ArrayList<>();
        for (int i = 0; i < attrCharArray.length; i++) {
            char ch = attrCharArray[i];
            if (ch == '(' || ch == '{') {
                startStack.add(i);
            } else if (ch == ')' || ch == '}') {
                endStack.add(i);
            }
        }
        String formattedValue = null;
        if (startStack.size() <= 1) {
            // There are only two type nestings, just format them directly.
            formattedValue = formattedQuotesByLevel(1, attrValue, quote);
        } else {
            // Types used more than twice need to be processed in order from the inside to the outside.
            Map<Integer, List<String>> structDataMap = new LinkedHashMap<>();
            for (int i = 0; i < startStack.size(); i++) {
                int level = i + 1;

                int begin = startStack.get(i);
                int end = endStack.get(endStack.size() - 1 - i);
                String subString = attrValue.substring(begin, end + 1);
                if (structDataMap.containsKey(level)) {
                    structDataMap.get(level).add(subString);
                } else {
                    List<String> tmpList = new ArrayList<>();
                    tmpList.add(subString);
                    structDataMap.put(level, tmpList);
                }
            }
            // Handle formatted attribute value
            formattedValue = structDataMap.get(1).get(0);
            formattedValue = formattedQuotesByLevel(1, formattedValue, quote);
            int maxLevel = structDataMap.keySet().size();
            for (int i = maxLevel; i > 1; i--) {
                List<String> tmpList = structDataMap.get(i);
                for (int j = 0; j < tmpList.size(); j++) {
                    String srcStr = tmpList.get(j);
                    String destStr = formattedQuotesByLevel(i, srcStr, quote);
                    formattedValue = formattedValue.replace(srcStr, destStr);
                }
            }
        }
        sb.append(formattedValue);
        return sb.toString();
    }

    /**
     * @MethodName:
     * @Params
     * @Return
     * @Exception
     * @Description: According to the number of layers, increase the quote exponentially by 2 to the String value.
     */
    public static String formattedQuotesByLevel(int level, String value, String quote) {
        String replaceQuote = getFormatterQuotesByLevel(level + 1, quote);
        return value.replaceAll(quote, replaceQuote);
    }

    /**
     * @MethodName:
     * @Params
     * @Return
     * @Exception
     * @Description: Copy quote exponentially by 2 according to level. When it is 0, return nothing.
     */
    private static String getFormatterQuotesByLevel(int level, String quote) {
        if (level <= 0) {
            return "";
        } else {
            StringBuilder sb = new StringBuilder();
            int total = (int) Math.pow(2, level - 1);
            for (int i = 0; i < total; ++i) {
                sb.append(quote);
            }
            return sb.toString();
        }
    }

    /**
     * @MethodName:
     * @Params
     * @Return
     * @Exception
     * @Description: To convert String sttrsValue into Object[] attributes, it needs to be encapsulated in combination with att_type_id in the system view.
     */
    public static Object[] parseAttributes(BaseConnection conn, List<Integer> elementOIDs, String attrsValue) throws SQLException {
        String quote = QUOTES;
        List<Object> list = new ArrayList();

        if (!attrsValue.startsWith("(") && !attrsValue.endsWith(")")) {
            throw new RuntimeException("Not a valid construct value for a Row");
        } else {
            // Remove the left and right brackets and process the attrs attribute.
            attrsValue = attrsValue.substring(1, attrsValue.length() - 1);

            char[] attrsCharArray = attrsValue.toCharArray();
            // Handle the PGStruct object contained in attrsValue
            String innerStructValue = null;
            String innerFormattedAttrsValue = null;
            String innerReplaceValue = null;
            if (attrsValue.contains("(") && attrsValue.contains(")")) {
                String patternStr = ("\\((.+)\\)");
                Pattern pattern = Pattern.compile(patternStr);
                Matcher matcher = pattern.matcher(attrsValue);
                if (matcher.find()) {
                    // Find what's inside the brackets
                    innerStructValue = matcher.group();
                    int hashCode = innerStructValue.hashCode();
                    innerReplaceValue = Math.random() + "" + hashCode;

                    int i = attrsValue.indexOf(innerStructValue);
                    int level = 0;
                    while (i < attrsCharArray.length && i >= 0) {
                        char levelCh = attrsCharArray[i - 1];
                        if (levelCh == QUOTES_CHARACTER) {
                            level++;
                            i--;
                        } else if (levelCh == COMMA_CHARACTER) {
                            break;
                        }
                    }
                    String formattedQuotes = getFormatterQuotesByLevel(level + 1, quote);
                    innerFormattedAttrsValue = innerStructValue.replace(formattedQuotes, quote);

                    // Replace placeholders to prevent misparsing
                    attrsValue = attrsValue.replace(innerStructValue, innerReplaceValue);
                }
            }

            String[] attrsValueArray = attrsValue.split(COMMA);
            // According to , number comes out the attrs value array
            for (int i = 0; i < attrsValueArray.length; i++) {
                String attrsObjectValue = attrsValueArray[i];
                int oid = elementOIDs.get(i);
                int sqlType = conn.getTypeInfo().getSQLType(oid);
                if (sqlType == Types.STRUCT) {
                    attrsObjectValue = innerFormattedAttrsValue;
                    list.add(new PGStruct(conn, oid, attrsObjectValue));
                } else if (sqlType == Types.ARRAY) {
                    String objectValue = attrsObjectValue;
                    list.add(new PgArray(conn, oid, objectValue));
                } else {
                    String objectValue = attrsObjectValue;
                    if (!objectValue.isEmpty() && objectValue.startsWith(quote) && objectValue.endsWith(quote)) {
                        objectValue = objectValue.substring(1, objectValue.length() - 1);
                    }
                    Object obj = getObject(conn, objectValue, oid, sqlType);
                    list.add(obj);
                }
            }
        }
        return list.toArray();
    }

    /**
     * @MethodName:
     * @Params
     * @Return
     * @Exception
     * @Description: Convert to Object object of corresponding type according to oid and att_type_id.
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
}
