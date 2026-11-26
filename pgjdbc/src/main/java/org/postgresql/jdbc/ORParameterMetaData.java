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
