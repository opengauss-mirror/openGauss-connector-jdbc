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
        List<ORParameterList> parameterList = preparedQuery.getCtStatement().getParametersList();
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
