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

/**
 * column info
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORField {
    private String columnName;
    private int labelLen;
    private Object[] typeInfo;
    private int length;
    private int nullableFlag;
    private boolean isAutoIncrement;
    private int precision;
    private int scale;
    private int inOutFlag;

    /**
     * set columnName
     *
     * @param columnName columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * set type info
     *
     * @param typeInfo type info
     */
    public void setTypeInfo(Object[] typeInfo) {
        this.typeInfo = typeInfo;
    }

    /**
     * get type info
     *
     * @return type info
     */
    public Object[] getTypeInfo() {
        return typeInfo;
    }

    /**
     * set inOut flag
     *
     * @param inOutFlag inOut flag
     */
    public void setInOutFlag(int inOutFlag) {
        this.inOutFlag = inOutFlag;
    }

    /**
     * set column length
     *
     * @param length column length
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * set scale
     *
     * @param scale scale
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * get scale
     *
     * @return scale
     */
    public int getScale() {
        return scale;
    }

    /**
     * set label length
     *
     * @param labelLen labelLen
     */
    public void setLabelLen(int labelLen) {
        this.labelLen = labelLen;
    }

    /**
     * set nullableFlag
     *
     * @param nullableFlag nullableFlag
     */
    public void setNullableFlag(int nullableFlag) {
        this.nullableFlag = nullableFlag;
    }

    /**
     * get nullableFlag
     *
     * @return nullableFlag
     */
    public int getNullableFlag() {
        return nullableFlag;
    }

    /**
     * set isAutoIncrement
     *
     * @param isAutoIncrement autoIncrement
     */
    public void setAutoIncrement(boolean isAutoIncrement) {
        this.isAutoIncrement = isAutoIncrement;
    }

    /**
     * get columnName
     *
     * @return columnName
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * get isAutoIncrement
     *
     * @return isAutoIncrement
     */
    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    /**
     * get column length
     *
     * @return column length
     */
    public int getLength() {
        return length;
    }

    /**
     * set precision
     *
     * @param precision precision
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * get precision
     *
     * @return precision
     */
    public int getPrecision() {
        return precision;
    }
}
