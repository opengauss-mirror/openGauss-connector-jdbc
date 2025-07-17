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

package org.postgresql.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * parallel search
 *
 * @author taoying
 * @since  2025-07-25
 */
public class ParallelSearch {
    private static final Pattern SELECT_PATTERN = Pattern.compile(
        "^\\s*(?:--.*\\s*)*SELECT\\s+",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern COMMENT_PATTERN = Pattern.compile(
        "--.*$",
        Pattern.MULTILINE
    );
    private static final Pattern VECTOR_OP_PATTERN = Pattern.compile(
        "<->|<=>|<#>|<+>|<~>|<%>"
    );

    private HikariDataSource dataSource;

    private ExecutorService executorService;

    /**
     * init connection pool
     *
     * @param jdbcUrl url of jdbc
     * @param username user name
     * @param auth database password
     * @param maxworkers max thread workers
     */
    public void initConnectionPool(String jdbcUrl, String username, String auth, int maxworkers) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(auth);
        config.setMaximumPoolSize(maxworkers);
        dataSource = new HikariDataSource(config);

        executorService = new ThreadPoolExecutor(maxworkers, maxworkers, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());
    }

    /**
     * execute multi search
     *
     * @param dbConfig database configuration
     * @param sqlTemplate sql
     * @param parameters search data
     * @param scanParams set params
     * @param threadCount max thread workers
     * @return search results
     * @throws InterruptedException InterruptedException
     * @throws ExecutionException ExecutionException
     */
    public List<List<Map<String, Object>>> executeMultiSearch(Map<String, String> dbConfig,
        String sqlTemplate, List<List<Object>> parameters, Map<String, Object> scanParams, int threadCount)
        throws InterruptedException, ExecutionException {
        Logger logger = Logger.getLogger("org.opengauss.core.v3.ConnectionFactoryImpl");
        logger.setLevel(Level.WARNING);

        validateSqlTemplate(sqlTemplate);

        if (parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Parameters list must not be null or empty");
        }

        if (threadCount <= 0) {
            throw new IllegalArgumentException("threadcount must be greater than 0");
        }

        String jdbcUrl = dbConfig.get("jdbcUrl");
        String username = dbConfig.get("username");
        String auth = dbConfig.get("auth");

        Boolean isInit = false;
        if (dataSource == null || dataSource.isClosed()) {
            initConnectionPool(jdbcUrl, username, auth, threadCount);
            isInit = true;
        }

        List<Future<List<Map<String, Object>>>> futures = new ArrayList<>();
        for (List<Object> param : parameters) {
            QueryTask task = new QueryTask(dataSource, sqlTemplate, param, scanParams);
            futures.add(executorService.submit(task));
        }

        List<List<Map<String, Object>>> results = new ArrayList<>();
        for (Future<List<Map<String, Object>>> future : futures) {
            results.add(future.get());
        }

        if (isInit) {
            closeConnectionPool();
        }
        return results;
    }

    private void validateSqlTemplate(String sqlTemplate) {
        if (sqlTemplate == null || sqlTemplate.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid SQL template: must be a non-empty string");
        }

        String trimmedSql = sqlTemplate.trim();
        String processedSql = trimmedSql;

        Matcher selectMatcher = SELECT_PATTERN.matcher(trimmedSql);
        if (!selectMatcher.find()) {
            throw new IllegalArgumentException("Invalid SQL template: must be a SELECT statement");
        }

        Matcher commentMatcher = COMMENT_PATTERN.matcher(processedSql);
        processedSql = commentMatcher.replaceAll("").trim();

        String[] statements = processedSql.split(";");
        int validStatementCount = 0;
        for (String stmt : statements) {
            if (!stmt.trim().isEmpty()) {
                validStatementCount++;
            }
        }

        if (validStatementCount != 1) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid SQL template: must contain exactly one query, found %d",
                    validStatementCount)
            );
        }

        Matcher vectorOpMatcher = VECTOR_OP_PATTERN.matcher(trimmedSql);
        if (!vectorOpMatcher.find()) {
            throw new IllegalArgumentException(
                "Invalid SQL template: must contain vector operator <->, <=>, <+>, <~>, <%> or <#>"
            );
        }
    }

    /**
     * close connection pool
     */
    public void closeConnectionPool() {
        if (executorService != null) {
            executorService.shutdown();
            executorService = null;
        }
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            dataSource = null;
        }
    }
}

class QueryTask implements Callable<List<Map<String, Object>>> {
    private final HikariDataSource dataSource;
    private final String sqlTemplate;
    private final List<Object> parameters;
    private final Map<String, Object> scanParams;

    public QueryTask(HikariDataSource dataSource, String sqlTemplate, List<Object> parameters,
        Map<String, Object> scanParams) {
        this.dataSource = dataSource;
        this.sqlTemplate = sqlTemplate;
        this.parameters = parameters;
        this.scanParams = scanParams;
    }

    @Override
    public List<Map<String, Object>> call() throws SQLException {
        try (Connection connection = dataSource.getConnection();
            Statement st = connection.createStatement()) {
            StringBuilder sqlBuilder = new StringBuilder();
            for (Map.Entry<String, Object> entry : scanParams.entrySet()) {
                sqlBuilder.append("SET ")
                    .append(entry.getKey())
                    .append(" = ")
                    .append(entry.getValue())
                    .append("; ");
            }
            String configSql = sqlBuilder.toString().trim();
            st.execute(configSql);

            String sql = generateSql(sqlTemplate, parameters);
            ResultSet rs = st.executeQuery(sql);

            return resultSetToMapList(rs);
        }
    }

    private static String generateSql(String sqlTemplate, List<Object> parameters) {
        String sql = sqlTemplate;
        for (Object param : parameters) {
            sql = sql.replaceFirst("\\?", param.toString());
        }
        return sql;
    }

    private List<Map<String, Object>> resultSetToMapList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnName(i), rs.getObject(i));
            }
            result.add(row);
        }
        return result;
    }
}