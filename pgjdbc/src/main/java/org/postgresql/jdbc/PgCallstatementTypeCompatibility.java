/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import java.sql.Types;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class for deal with compatibility type in PgCallableStatement.
 */
public class PgCallstatementTypeCompatibility {

    private static TypeConvert noneConvert = new TypeConvert() {
        @Override
        public Object convert(Object input) {
            return input;
        }

        @Override
        public boolean needConvert() {
            return false;
        }
    };

    private static TypeConvert stringConvert = input -> input.toString();

    private static TypeConvert double2FloatConvert = input -> ((Double) input).floatValue();

    private static TypeConvert numeric2integer = input -> {
        String str = input.toString();
        int idx = str.lastIndexOf(".");
        if(idx > 0){
            String strNum = str.substring(0,idx);
            return Integer.valueOf(strNum);
        } else{
            return Integer.valueOf(str);
        }
    };

    private static TypeConvert numeric2Float = input -> Double.parseDouble(input.toString());

    private static TypeConvert smallint2Tinyint = input -> {
        int bit;
        int result = 0;
        for (int i = 0; i < 8; i++) {
            bit = ((int)input >> i) & 0x01;
            if (bit == 1) {
                result += 1 << i;
            }
        }
        return result;
    };

    private static Map<String, TypeConvert> typeConvertMap = new ConcurrentHashMap<>();

    static {
        addConvert(Types.DOUBLE, Types.REAL, double2FloatConvert);
        addConvert(Types.INTEGER, Types.VARCHAR, stringConvert);
        addConvert(Types.NUMERIC, Types.VARCHAR, stringConvert);
        addConvert(Types.VARCHAR, Types.CLOB, stringConvert);
        addConvert(Types.NUMERIC, Types.INTEGER, numeric2integer);
        addConvert(Types.INTEGER, Types.NUMERIC, noneConvert);
        addConvert(Types.OTHER, -10, noneConvert);
        addConvert(Types.OTHER, Types.BLOB, noneConvert);
        addConvert(Types.BLOB, Types.OTHER, noneConvert);
        addConvert(Types.REF_CURSOR, Types.OTHER, noneConvert);
        addConvert(Types.SMALLINT, Types.TINYINT, smallint2Tinyint);
        addConvert(Types.NUMERIC, Types.DOUBLE, numeric2Float);
    }

    private int actualType;
    private int parameterType;
    private TypeConvert convert;

    public PgCallstatementTypeCompatibility(int actualType, int parameterType) {
        this.actualType = actualType;
        this.parameterType = parameterType;
        this.convert = typeConvertMap.getOrDefault(generateUniqueKey(actualType, parameterType), null);
    }

    /**
     * Check if two type is compatibility type, actualType and parameterType is single direct convert.
     * @return true if compatibility
     */
    public boolean isCompatibilityType() {
        return this.convert != null;
    }

    /**
     * Check if need convert input value.
     * @return true if need
     */
    public boolean needConvert() {
        return this.convert.needConvert();
    }

    /**
     * Convert value to another
     * @param input the input value
     * @return the converted value
     */
    public Object convert(Object input) {
        return this.convert.convert(input);
    }

    private static void addConvert(int actualType, int parameterType, TypeConvert convert) {
        typeConvertMap.put(generateUniqueKey(actualType, parameterType), convert);
    }

    private static String generateUniqueKey(int firstKey, int secondKey) {
        return "" + firstKey + "|" + secondKey;
    }

    private interface TypeConvert {
        Object convert(Object input);
        default boolean needConvert() {
            return true;
        };
    }
}
