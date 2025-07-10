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
