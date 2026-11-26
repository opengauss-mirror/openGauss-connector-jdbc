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
