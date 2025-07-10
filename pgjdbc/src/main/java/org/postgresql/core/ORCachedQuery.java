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

import org.postgresql.jdbc.ORResultSet;
import org.postgresql.jdbc.ORStatement;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * query cache
 *
 * @author zhangting
 * @since  2025-06-29
 */
public class ORCachedQuery {
    private ORResultSet rs;
    private ORStatement ctStatement;
    private ORBaseConnection conn;
    private String sql;
    private String parsedSql;
    private int paramCount;
    private boolean isPrepare;

    /**
     * cached query constructor
     *
     * @param conn connection
     * @param ctStatement statement
     * @param sql sql
     * @param isPrepare is prepare
     */
    public ORCachedQuery(ORBaseConnection conn, ORStatement ctStatement, String sql, boolean isPrepare) {
        this.conn = conn;
        this.ctStatement = ctStatement;
        this.sql = sql;
        this.paramCount = parseSql(sql);
        this.isPrepare = isPrepare;
    }

    /**
     * get connection
     *
     * @return connection
     */
    public ORBaseConnection getConn() {
        return conn;
    }

    /**
     * if is prepare
     *
     * @return is prepare
     */
    public boolean isPrepare() {
        return isPrepare;
    }

    /**
     * get param count
     *
     * @return param count
     */
    public int getParamCount() {
        return paramCount;
    }

    private int parseSql(String sql) {
        char[] cs = sql.toCharArray();
        StringBuilder sqlSb = new StringBuilder(cs.length + 10);
        int index = 0;
        int params = 0;
        while (index < cs.length) {
            if (cs[index] == '\'') {
                index = parseQuotes(sqlSb, cs, index, '\'');
            } else if (cs[index] == '"') {
                index = parseQuotes(sqlSb, cs, index, '"');
            } else if (cs[index] == '-') {
                index = parseLine(sqlSb, cs, index);
            } else if (cs[index] == '?') {
                params++;
                index = parseParam(sqlSb, index, params);
            } else if (cs[index] == '/') {
                index = parseLines(sqlSb, cs, index);
            } else {
                sqlSb.append(cs[index]);
                index++;
            }
        }

        this.parsedSql = sqlSb.toString();
        return params;
    }

    private int parseQuotes(StringBuilder sqlSb, char[] cs, int index, char target) {
        int p = index;
        sqlSb.append(cs[p++]);
        while (p < cs.length) {
            sqlSb.append(cs[p]);
            if (cs[p] == target) {
                p++;
                return p;
            }
            p++;
        }
        return p;
    }

    private int parseLine(StringBuilder sqlSb, char[] cs, int index) {
        int p = index;
        sqlSb.append(cs[p++]);
        if (p >= cs.length || cs[p] != '-') {
            return p;
        }
        sqlSb.append(cs[p++]);
        while (p < cs.length) {
            if (cs[p] == '\r' || cs[p] == '\n') {
                return p;
            }
            sqlSb.append(cs[p++]);
        }
        return p;
    }

    private int parseParam(StringBuilder sqlSb, int index, int paramCount) {
        int p = index;
        sqlSb.append(":p").append(paramCount);
        return ++p;
    }

    private int parseLines(StringBuilder sqlSb, char[] cs, int index) {
        int p = index;
        sqlSb.append(cs[p++]);
        if (p < cs.length && cs[p] == '*') {
            sqlSb.append(cs[p++]);
            while (p < cs.length) {
                sqlSb.append(cs[p]);
                char c = cs[p];
                p++;
                if (c == '*' && cs[p] == '/' && p < cs.length) {
                    sqlSb.append(cs[p++]);
                    break;
                }
            }
        }
        return p;
    }

    /**
     * get statement
     *
     * @return statement
     */
    public ORStatement getCtStatement() {
        return ctStatement;
    }

    /**
     * get parsed sql
     *
     * @return parsed sql
     */
    public String getParsedSql() {
        return parsedSql;
    }

    /**
     * get resultSet
     *
     * @return resultSet
     */
    public ResultSet getRs() {
        return rs;
    }

    /**
     * set data rows to the resultSet
     *
     * @param fields column fields
     * @param valueLens value length
     * @param rows data rows
     * @param hasRemain is there still data available
     * @throws SQLException if a database access error occurs
     */
    public void handleResultRows(ORField[] fields, List<int[]> valueLens, List<byte[][]> rows,
                                 boolean hasRemain) throws SQLException {
        this.rs = new ORResultSet(this.ctStatement, sql, fields, valueLens, rows, hasRemain);
    }

    /**
     * set resultSet
     *
     * @param rs resultSet
     */
    public void setRs(ORResultSet rs) {
        this.rs = rs;
    }

    /**
     * update fetch data to the resultSet
     *
     * @param total rows total
     * @param valueLens value length
     * @param rows data rows
     * @param hasRemain is there still data available
     */
    public void setNewData(int total, List<int[]> valueLens, List<byte[][]> rows, boolean hasRemain) {
        rs.setFetchInfo(total, valueLens, rows, hasRemain);
    }

    /**
     * set sql query
     *
     * @param sql sql query
     */
    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * get sql
     *
     * @return sql
     */
    public String getSql() {
        return sql;
    }
}