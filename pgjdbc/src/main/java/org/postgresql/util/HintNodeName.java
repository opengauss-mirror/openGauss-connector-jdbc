package org.postgresql.util;

import org.postgresql.core.Parser;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.SetupQueryRunner;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Add node name for single slice query
 *
 * @since 2021-12-28
 */
public class HintNodeName {
    private static Log LOGGER = Logger.getLogger(HintNodeName.class.getName());

    // Last update time of allnode
    private static long lastUpdated = System.currentTimeMillis();

    // Minimum time interval for continuous update of allnode
    private static final long MIN_UPDATAED = 10000L;
    private static List<String> allNode = new ArrayList<>();
    private static char[] specialChar = new char[]{'+', '-', '*', '/', ' ', '\n', '\r', '\t'};
    private static char[] jumpChar = new char[]{')', '/', '\'', '"', '(', '-'};

    /**
     * Add a single slice query statement to the SQL statement
     *
     * @param query         SQL statement
     * @param nodeName      DN name
     * @param queryExecutor queryExecutor
     * @return String
     * @throws SQLException Throw SQLException
     */
    public static String addNodeName(final String query,
                                     final String nodeName,
                                     QueryExecutor queryExecutor) throws SQLException {
        if (nodeName == null || nodeName.isEmpty()) {
            return query;
        }
        containsSqlInjection(nodeName, queryExecutor);

        char[] tempSql = query.toCharArray();
        // Used to determine whether the currently scanned select is a select keyword
        boolean isSelect = false;
        // Is the current statement a select statement
        boolean isSelectStatement = false;
        // parentheses number
        int parenthesesClose = 0;

        for (int i = 0; i < tempSql.length; i++) {
            if (tempSql[i] == '/' && i + 2 < tempSql.length && tempSql[i + 1] == '*') {
                // There are comments after select
                if (isSelect) {
                    return addHint(i, nodeName, tempSql, query);
                }
                i = Parser.parseBlockComment(tempSql, i);
            } else if (tempSql[i] == '-' && i + 1 < tempSql.length && tempSql[i + 1] == '-') {
                i = Parser.parseLineComment(tempSql, i);
            } else if (Parser.isSpecialCharacters(tempSql[i])) {
                continue;
            } else if (tempSql[i] == '(') {
                if (!isSelectStatement) {
                    // scene:(select 1)
                    continue;
                } else {
                    if (isSelect) {
                        return addHint(i, nodeName, tempSql, query);
                    }
                    parenthesesClose++;
                }
            } else if (tempSql[i] == ')') {
                parenthesesClose--;
            } else if (tempSql[i] == '"') {
                i = Parser.parseDoubleQuotes(tempSql, i);
            } else if (tempSql[i] == '\'') {
                i = Parser.parseSingleQuotes(tempSql, i, true);
            } else if ((tempSql[i] == 's' || tempSql[i] == 'S') && i + 5 < tempSql.length
                    && !isSelect) {
                if (parenthesesClose == 0 &&
                        "select".equalsIgnoreCase(String.valueOf(tempSql[i])
                                + tempSql[i + 1] + tempSql[i + 2] + tempSql[i + 3]
                                + tempSql[i + 4] + tempSql[i + 5])) {
                    // Judge whether select is a keyword
                    isSelect = isSpecialCharacters(i + 6, tempSql);
                    if (isSelect) {
                        // Judge whether it is a select statement
                        isSelectStatement = true;
                    }
                    i = i + 5;
                }
            } else if ((tempSql[i] == 'w' || tempSql[i] == 'W') && i + 3 < tempSql.length
                    && !isSelectStatement) {
                if ("with".equalsIgnoreCase(String.valueOf(tempSql[i])
                        + tempSql[i + 1] + tempSql[i + 2] + tempSql[i + 3])) {
                    isSelectStatement = isSpecialCharacters(i + 4, tempSql);
                    i = i + 3;
                }
            } else {
                if (isSelect) {
                    return addHint(i, nodeName, tempSql, query);
                }
                // After crossing the comments and parentheses at the beginning of the statement,
                // the first keyword is not select and with, and the execution is terminated
                if (!isSelectStatement) {
                    return query;
                }
                // scene: with tempSelect as ()
                i = crossThisString(i, tempSql);
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Add a single slice query statement to the SQL statement:" + String.valueOf(tempSql));
        }
        return query;
    }

    private static String addHint(final int offSet, final String nodeName, final char[] sqlChars, String query) {
        int tempOffSet = offSet;
        String newSql;
        if (sqlChars[tempOffSet] == '/' && tempOffSet + 2 < sqlChars.length
                && sqlChars[tempOffSet + 1] == '*' && sqlChars[tempOffSet + 2] == '+') {
            tempOffSet = Parser.parseBlockComment(sqlChars, tempOffSet) - 1;
            newSql = query.substring(0, tempOffSet) + " set(node_name " + nodeName + ") "
                    + query.substring(tempOffSet);
        } else {
            newSql = query.substring(0, tempOffSet) + "/*+ set(node_name " + nodeName + ") */ "
                    + query.substring(tempOffSet);
        }
        return newSql;
    }

    /**
     * Used to determine whether there are illegal characters
     *
     * @param nodeName      DN name
     * @param queryExecutor Type parameter of QueryExecutor
     * @return null
     * @throws SQLException Throw SQLException
     */
    public static void containsSqlInjection(String nodeName,
                                            QueryExecutor queryExecutor) throws SQLException {
        if (nodeName.contains(";") || nodeName.contains("/*") || nodeName.contains("*/")) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.warn("Illegal node name:" + nodeName);
            }
            throw new PSQLException(
                    GT.tr("Illegal node name:" + nodeName + "."),
                    PSQLState.DATA_ERROR);
        }
        nodeNameCheck(nodeName, queryExecutor);
    }

    private static void updateAllNode(QueryExecutor queryExecutor) throws SQLException {
        String query = "select node_name from pgxc_node where node_type='D' and nodeis_active='t'";
        List<byte[][]> results = SetupQueryRunner.runForList(queryExecutor, query, true);
        synchronized (allNode) {
            try {
                long nowTime = System.currentTimeMillis();
                if (allNode.size() != 0 && (nowTime - lastUpdated < MIN_UPDATAED)) {
                    return;
                }
                allNode.clear();
                for (byte[][] result : results) {
                    allNode.add(queryExecutor.getEncoding().decode(result[0]));
                }
                lastUpdated = nowTime;
            } catch (IOException e) {
                allNode.clear();
                throw new SQLException("Fail to check pgxc_node." + e.getMessage());
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Currently available DN nodes:" + allNode);
        }
    }

    private static boolean isSpecialCharacters(final int offSet, char[] tempSql) {
        if (offSet >= tempSql.length) {
            return true;
        }
        char tempChar = tempSql[offSet];
        for (char specialChar : specialChar) {
            if (specialChar == tempChar) {
                return true;
            }
        }
        return false;
    }

    private static boolean jumpCharacters(final char specialChar) {
        for (char jump : jumpChar) {
            if (jump == specialChar) {
                return true;
            }
        }
        return false;
    }

    private static int crossThisString(final int offSet, char[] tempSql) {
        for (int i = offSet + 1; i < tempSql.length; i++) {
            if (Parser.isSpecialCharacters(tempSql[i])) {
                return i - 1;
            } else if (jumpCharacters(tempSql[i])) {
                // scene:with w1 as  (select id /*A  ) select */ from test)
                return i - 1;
            } else {
                continue;
            }
        }
        return tempSql.length - 1;
    }

    private static void nodeNameCheck(final String nodeName, final QueryExecutor queryExecutor) throws SQLException {
        if (!allNode.contains(nodeName)) {
            updateAllNode(queryExecutor);
            if (!allNode.contains(nodeName)) {
                throw new PSQLException(
                        GT.tr("Node name " + nodeName + " does not exist."),
                        PSQLState.DATA_ERROR);
            }
        }
    }
}
