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

package org.postgresql.jdbc;

import org.postgresql.core.ORCachedQuery;
import org.postgresql.core.ORParameterList;
import org.postgresql.core.ORDataType;

import java.sql.SQLException;
import java.util.List;

/**
 * prepare statement param info.
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORParameterMetaData extends PgParameterMetaData {
    private ORCachedQuery preparedQuery;

    /**
     * preparedQuery constructor
     *
     * @param preparedQuery prepare query
     */
    public ORParameterMetaData(ORCachedQuery preparedQuery) {
        super(null, null);
        this.preparedQuery = preparedQuery;
    }

    @Override
    public int getParameterCount() {
        return this.preparedQuery.getParamCount();
    }

    private void paramVerify(int param) throws SQLException {
        if (param < 1 || param > preparedQuery.getParamCount()) {
            throw new SQLException("the param index out of bounds.");
        }
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        paramVerify(param);
        int[] types = getTypes();
        return ORDataType.getDataType(types[param - 1])[0].toString();
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        paramVerify(param);
        return false;
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        paramVerify(param);
        int[] types = getTypes();
        return ORDataType.getDataType(types[param - 1])[3].toString();
    }

    private int[] getTypes() throws SQLException {
        List<ORParameterList> parameterList = preparedQuery.getStatement().getParametersList();
        if (parameterList.isEmpty()) {
            throw new SQLException("the parameter list is empty.");
        }
        return parameterList.get(parameterList.size() - 1).getDbTypes();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        paramVerify(param);
        int[] types = getTypes();
        Object paramType = ORDataType.getDataType(types[param - 1])[2];
        return Integer.parseInt(paramType.toString());
    }
}
