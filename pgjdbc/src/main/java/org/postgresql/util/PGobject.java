/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */


package org.postgresql.util;

import java.io.Serializable;
import java.sql.SQLException;

/**
 * PGobject is a class used to describe unknown types An unknown type is any type that is unknown by
 * JDBC Standards.
 */
public class PGobject implements Serializable, Cloneable {
    protected String type;
    protected String value;

    /**
     * when the object is a custom type is used to save its structure
     */
    protected Object[] struct;

    /**
     * Cache result
     */
    private String[] arrayValue;

    /**
     * This is called by org.postgresql.Connection.getObject() to create the object.
     */
    public PGobject() {
    }

    /**
     * <p>This method sets the type of this object.</p>
     *
     * <p>It should not be extended by subclasses, hence it is final</p>
     *
     * @param type a string describing the type of the object
     */
    public final void setType(String type) {
        this.type = type;
    }

    /**
     * This method sets the value of this object. It must be overridden.
     *
     * @param value a string representation of the value of the object
     * @throws SQLException thrown if value is invalid for this type
     */
    public void setValue(String value) throws SQLException {
        this.value = value;
    }

    /**
     * As this cannot change during the life of the object, it's final.
     *
     * @return the type name of this object
     */
    public final String getType() {
        return type;
    }

    /**
     * This must be overidden, to return the value of the object, in the form required by
     * org.postgresql.
     *
     * @return the value of this object
     */
    public String getValue() {
        return value;
    }

    /**
     * <p>This method sets the struct of this object.</p>
     * <p>It should not be extended by subclasses, hence it is final</p>
     *
     * @param struct struct a Object[] describing the struct of the object
     */
    public final void setStruct(Object[] struct) {
        this.struct = struct;
    }

    /**
     * As this cannot change during the life of the object, it's final.
     *
     * @return the struct of this object
     */
    public final Object[] getStruct() {
        return struct;
    }

    /**
     * parse string to array
     *
     * @return custom type array structure value
     */
    public String[] getArrayValue() {
        if (arrayValue != null) {
            return arrayValue;
        }
        if (struct != null && struct.length > 0 && this.value.length() > 2) {
            arrayValue = new String[struct.length];
            // remove the parentheses.
            char[] chars = this.value.toCharArray();
            int noBeginAndEndBracketLen = chars.length - 1;
            int begin = 1;
            int end = 1;
            int index = 0;
            for (int i = 1; i < noBeginAndEndBracketLen; i++) {
                // If the character starts with a double quote character, this value contains the following special
                // characters , or " or ( or ) or \ or blank.
                if (chars[i] == '"') {
                    while (i + 2 <= noBeginAndEndBracketLen) {
                        if (chars[i + 1] == '"') {
                            if ((i + 2 == noBeginAndEndBracketLen) || (chars[i + 2] == ',')) {
                                i = i + 2;
                                end = i;
                                arrayValue[index] = delimitedCompositeTypeValue(begin, end, this.value);
                                index++;
                                begin = end + 1;
                                break;
                            } else if (chars[i + 2] != '"') { // String format error,return undisassembled value.
                                arrayValue = new String[]{this.value};
                                return new String[]{this.value};
                            }
                            i = i + 2;
                        } else {
                            i++;
                        }
                    }
                } else if (chars[i] == ',') { // If it is a comma, intercept the initial position to the current
                    // position.
                    end = i;
                    arrayValue[index] = delimitedCompositeTypeValue(begin, end, this.value);
                    index++;
                    begin = end + 1;
                }
            }
            if (end != noBeginAndEndBracketLen) {
                arrayValue[index] = delimitedCompositeTypeValue(begin, noBeginAndEndBracketLen, this.value);
            }
        } else {
            arrayValue = new String[]{this.value};
        }
        return arrayValue;
    }

    /**
     * @param begin        start index
     * @param end          finish index
     * @param originalChar raw data
     * @return specify the subscript data
     */
    private String delimitedCompositeTypeValue(int begin, int end, String originalChar) {
        String attribute = originalChar.substring(begin, end);
        return (attribute != null && attribute.length() > 0) ? attribute : null;
    }

    /**
     * This must be overidden to allow comparisons of objects.
     *
     * @param obj Object to compare with
     * @return true if the two boxes are identical
     */
    public boolean equals(Object obj) {
        if (obj instanceof PGobject) {
            final Object otherValue = ((PGobject) obj).getValue();

            if (otherValue == null) {
                return getValue() == null;
            }
            return otherValue.equals(getValue());
        }
        return false;
    }

    /**
     * This must be overidden to allow the object to be cloned.
     */
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * This is defined here, so user code need not overide it.
     *
     * @return the value of this object, in the syntax expected by org.postgresql
     */
    public String toString() {
        return getValue();
    }

    /**
     * Compute hash. As equals() use only value. Return the same hash for the same value.
     *
     * @return Value hashcode, 0 if value is null {@link java.util.Objects#hashCode(Object)}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;

    }
}
