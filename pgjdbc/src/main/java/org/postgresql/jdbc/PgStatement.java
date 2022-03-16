/*$$$!!Warning: Huawei key information asset. No spread without permission.$$$*/
/*CODEMARK:WQcKBKQLgyHkXlCLiGIlSvGdiRVlDYpIJLLeBa8CyrNJleUkOpTtwkJAoYct18IqCBpJnKNz
xHjEu+uXc8LQFhsoI8WrCaQSzs7IbOufhA/Trab/bBrhL6CdcNh/R9gRt8WN0nikxwJmRwS3
ilXKnoNfko5Zw3qFNnUc0ADHXFHeGX4P3ONBXnw0j6DKIdX8KaWsr2L7stjeVKz+oqR+Z8zf
bshdEshqyIK6a6DPitClTLK1TsHgkvRGx14SlPiKTlFg6HsD0Ere3u78qZaAfA==#*/
/*$$$!!Warning: Deleting or modifying the preceding information is prohibited.$$$*/
/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.core.*;
import org.postgresql.util.GT;
import org.postgresql.util.PGbytea;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import org.postgresql.util.HintNodeName;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class PgStatement implements Statement, BaseStatement {
  private static final String[] NO_RETURNING_COLUMNS = new String[0];
  private static Log LOGGER = Logger.getLogger(PgStatement.class.getName());
  /**
   * Default state for use or not binary transfers. Can use only for testing purposes
   */
  private static final boolean DEFAULT_FORCE_BINARY_TRANSFERS =
      Boolean.getBoolean("org.postgresql.forceBinary");
  // only for testing purposes. even single shot statements will use binary transfers
  private boolean forceBinaryTransfers = DEFAULT_FORCE_BINARY_TRANSFERS;
  
  protected ArrayList<Query> batchStatements = null;
  protected ArrayList<ParameterList> batchParameters = null;
  protected final int resultsettype; // the resultset type to return (ResultSet.TYPE_xxx)
  protected final int concurrency; // is it updateable or not? (ResultSet.CONCUR_xxx)
  private final int rsHoldability;
  private boolean poolable;
  private boolean closeOnCompletion = false;
  protected int fetchdirection = ResultSet.FETCH_FORWARD;
  // fetch direction hint (currently ignored)
  ArrayList noticeListenerlist = new ArrayList();
  /**set
   * Protects current statement from cancelTask starting, waiting for a bit, and waking up exactly
   * on subsequent query execution. The idea is to atomically compare and swap the reference to the
   * task, so the task can detect that statement executes different query than the one the
   * cancelTask was created. Note: the field must be set/get/compareAndSet via
   * {@link #CANCEL_TIMER_UPDATER} as per {@link AtomicReferenceFieldUpdater} javadoc.
   */
  private volatile TimerTask cancelTimerTask = null;
  private static final AtomicReferenceFieldUpdater<PgStatement, TimerTask> CANCEL_TIMER_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(PgStatement.class, TimerTask.class, "cancelTimerTask");

  /**
   * Protects statement from out-of-order cancels. It protects from both
   * {@link #setQueryTimeout(int)} and {@link #cancel()} induced ones.
   *
   * {@link #execute(String)} and friends change the field to
   * {@link StatementCancelState#IN_QUERY} during execute. {@link #cancel()}
   * ignores cancel request if state is {@link StatementCancelState#IDLE}.
   * In case {@link #execute(String)} observes non-{@link StatementCancelState#IDLE} state as it
   * completes the query, it waits till {@link StatementCancelState#CANCELLED}. Note: the field must be
   * set/get/compareAndSet via {@link #STATE_UPDATER} as per {@link AtomicIntegerFieldUpdater}
   * javadoc.
   */
  private volatile StatementCancelState statementState = StatementCancelState.IDLE;

  private static final AtomicReferenceFieldUpdater<PgStatement, StatementCancelState> STATE_UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(PgStatement.class, StatementCancelState.class, "statementState");

  /**
   * Does the caller of execute/executeUpdate want generated keys for this execution? This is set by
   * Statement methods that have generated keys arguments and cleared after execution is complete.
   */
  protected boolean wantsGeneratedKeysOnce = false;

  /**
   * Was this PreparedStatement created to return generated keys for every execution? This is set at
   * creation time and never cleared by execution.
   */
  public boolean wantsGeneratedKeysAlways = false;

  // The connection who created us
  protected final BaseConnection connection;

  /**
   * The warnings chain.
   */
  protected volatile PSQLWarningWrapper warnings = null;

  /**
   * Maximum number of rows to return, 0 = unlimited.
   */
  protected int maxrows = 0;

  /**
   * Number of rows to get in a batch.
   */
  protected int fetchSize = 0;

  /**
   * Timeout (in milliseconds) for a query.
   */
  protected long timeout = 0;

  protected boolean replaceProcessingEnabled = true;

  /**
   * The current results.
   */
  protected ResultWrapper result = null;

  /**
   * The first unclosed result.
   */
  protected ResultWrapper firstUnclosedResult = null;

  /**
   * Results returned by a statement that wants generated keys.
   */
  protected ResultWrapper generatedKeys = null;

  protected int m_prepareThreshold; // Reuse threshold to enable use of PREPARE

  protected int maxfieldSize = 0;

  /**
   * Used for client encryption to identify the statement name in libpq/clientlogic
   */
  protected String statementName = "";

    /**
     * Flag to determine if client logic requires to reload the cache and try again,
     * required when the cache is not in sync
     */
    protected Boolean shouldClientLogicRetry = false;

  private Boolean didRunPreQuery = false;

    /**
     * if the statement was created to fetch data for client logic cache via JNI using the method executeQueryWithNoCL
     * Important to set up, so the obtained resultset can ignore CL in that case
     */
    private boolean isStatamentUsedForClientLogicCache = false;

  PgStatement(PgConnection c, int rsType, int rsConcurrency, int rsHoldability)
      throws SQLException {
    this.connection = c;
    forceBinaryTransfers |= c.getForceBinary();
    resultsettype = rsType;
    concurrency = rsConcurrency;
    setFetchSize(c.getDefaultFetchSize());
	  if(c.getFetchSize() >= 0){
    	this.fetchSize = c.getFetchSize();
    }

    setPrepareThreshold(c.getPrepareThreshold());
    this.rsHoldability = rsHoldability;

    if (c.getClientLogic() != null) {
      this.statementName = c.getClientLogic().getStatementName();
    }
  }

  public ResultSet createResultSet(Query originalQuery, Field[] fields, List<byte[][]> tuples,
      ResultCursor cursor) throws SQLException {
    PgResultSet newResult = new PgResultSet(originalQuery, this, fields, tuples, cursor,
        getMaxRows(), getMaxFieldSize(), getResultSetType(), getResultSetConcurrency(),
        getResultSetHoldability());
    newResult.setFetchSize(getFetchSize());
    newResult.setFetchDirection(getFetchDirection());
    return newResult;
  }

  public BaseConnection getPGConnection() {
    return connection;
  }

  public String getFetchingCursorName() {
    return null;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  protected boolean wantsScrollableResultSet() {
    return resultsettype != ResultSet.TYPE_FORWARD_ONLY;
  }

  protected boolean wantsHoldableResultSet() {
    // FIXME: false if not supported
    return rsHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  public ResultWrapper getUpdateResult() {
    return this.result;
  }

  /**
   * ResultHandler implementations for updates, queries, and either-or.
   */
  public class StatementResultHandler extends ResultHandlerBase {
    private ResultWrapper results;
    private ResultWrapper lastResult;

    ResultWrapper getResults() {
      return results;
    }

    private void append(ResultWrapper newResult) {
      if (results == null) {
        lastResult = results = newResult;
      } else {
        lastResult.append(newResult);
      }
    }

    @Override
    public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples,
        ResultCursor cursor) {
      try {
        ResultSet rs = PgStatement.this.createResultSet(fromQuery, fields, tuples, cursor);
        append(new ResultWrapper(rs));
      } catch (SQLException e) {
        handleError(e);
      }
    }

    @Override
    public void handleCommandStatus(String status, long updateCount, long insertOID) {
      append(new ResultWrapper(updateCount, insertOID));
    }

    @Override
    public void handleWarning(SQLWarning warning) {
      PgStatement.this.addWarning(warning);
    }

  }

  public java.sql.ResultSet executeQuery(String p_sql) throws SQLException {
    p_sql = HintNodeName.addNodeName(p_sql, this.connection.getClientInfo("nodeName"),
            this.connection.getQueryExecutor());

    ClientLogic clientLogic = this.connection.getClientLogic();
    String exception = "No results were returned by the query.";
    if (clientLogic != null) {
      exception = "";
    }
    if (!executeWithFlags(p_sql, 0)) {
      throw new PSQLException(GT.tr(exception), PSQLState.NO_DATA);
    }

    return getSingleResultSet();
  }

    /*
     * @return true if the statement was created to get data for client logic cache Via JNI using executeQueryWithNoCL
     */
    @Override
    public boolean getIsStatamentUsedForClientLogicCache() {
        return isStatamentUsedForClientLogicCache;
    }

  /**
   * This method was added for supporting fetchDataFromQuery method in the ClientLogicImpl class
   * executes query and bypass client logic to get client logic data 
   * @param p_sql the query to run
   * @return ResultSet with the data
   * @throws SQLException
   */
  java.sql.ResultSet executeQueryWithNoCL(String p_sql) throws SQLException {
    isStatamentUsedForClientLogicCache = true;
	  if (!executeWithFlags(p_sql, QueryExecutor.QUERY_EXECUTE_BYPASS_CLIENT_LOGIC)) {
		  throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
	  }
	  return getSingleResultSet();
  }
  
  protected java.sql.ResultSet getSingleResultSet() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result.getNext() != null) {
        throw new PSQLException(GT.tr("Multiple ResultSets were returned by the query."),
            PSQLState.TOO_MANY_RESULTS);
      }

      return result.getResultSet();
    }
  }

  public int executeUpdate(String p_sql) throws SQLException {
    executeWithFlags(p_sql, QueryExecutor.QUERY_NO_RESULTS);
    checkNoResultUpdate();
    return getUpdateCount();
  }

  protected final void checkNoResultUpdate() throws SQLException {
    synchronized (this) {
      checkClosed();
      ResultWrapper iter = result;
      while (iter != null) {
        if (iter.getResultSet() != null) {
          throw new PSQLException(GT.tr("A result was returned when none was expected."),
              PSQLState.TOO_MANY_RESULTS);

        }
        iter = iter.getNext();
      }


    }
  }

  public boolean execute(String p_sql) throws SQLException {
    p_sql = HintNodeName.addNodeName(p_sql, this.connection.getClientInfo("nodeName"),
            this.connection.getQueryExecutor());

    return executeWithFlags(p_sql, 0);
  }

  public boolean executeWithFlags(String sql, int flags) throws SQLException {
    return executeCachedSql(sql, flags, NO_RETURNING_COLUMNS);
  }

  private boolean executeCachedSql(String sql, int flags, String[] columnNames) throws SQLException {
    PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
    // Simple statements should not replace ?, ? with $1, $2
    boolean shouldUseParameterized = false;
    QueryExecutor queryExecutor = connection.getQueryExecutor();
    Object key = queryExecutor
        .createQueryKey(sql, replaceProcessingEnabled, shouldUseParameterized, columnNames);
    CachedQuery cachedQuery;
    boolean shouldCache = preferQueryMode == PreferQueryMode.EXTENDED_CACHE_EVERYTHING;
    if (shouldCache) {
      cachedQuery = queryExecutor.borrowQueryByKey(key);
    } else {
      cachedQuery = queryExecutor.createQueryByKey(key);
    }
    if (wantsGeneratedKeysOnce) {
      SqlCommand sqlCommand = cachedQuery.query.getSqlCommand();
      wantsGeneratedKeysOnce = sqlCommand != null && sqlCommand.isReturningKeywordPresent();
    }
    boolean res;
    try {
      res = executeWithFlags(cachedQuery, flags);
    } finally {
      if (shouldCache) {
        queryExecutor.releaseQuery(cachedQuery);
      }
    }
    return res;
  }

  public boolean executeWithFlags(CachedQuery simpleQuery, int flags) throws SQLException {
    checkClosed();
    if (connection.getPreferQueryMode().compareTo(PreferQueryMode.EXTENDED) < 0) {
      flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    }
    execute(simpleQuery, null, flags);
    synchronized (this) {
      checkClosed();
      return (result != null && result.getResultSet() != null);
    }
  }

  public boolean executeWithFlags(int flags) throws SQLException {
    checkClosed();
    throw new PSQLException(GT.tr("Can''t use executeWithFlags(int) on a Statement."),
        PSQLState.WRONG_OBJECT_TYPE);
  }

  protected void closeForNextExecution() throws SQLException {
    // Every statement execution clears any previous warnings.
    clearWarnings();

    // Close any existing resultsets associated with this statement.
    synchronized (this) {
      while (firstUnclosedResult != null) {
        ResultSet rs = firstUnclosedResult.getResultSet();
        if (rs != null) {
          rs.close();
        }
        firstUnclosedResult = firstUnclosedResult.getNext();
      }
      result = null;

      if (generatedKeys != null) {
        if (generatedKeys.getResultSet() != null) {
          generatedKeys.getResultSet().close();
        }
        generatedKeys = null;
      }
    }
  }

  /**
   * Returns true if query is unlikely to be reused.
   *
   * @param cachedQuery to check (null if current query)
   * @return true if query is unlikely to be reused
   */
  protected boolean isOneShotQuery(CachedQuery cachedQuery) {
    if (cachedQuery == null) {
      return true;
    }
    cachedQuery.increaseExecuteCount();
    if ((m_prepareThreshold == 0 || cachedQuery.getExecuteCount() < m_prepareThreshold)
        && !getForceBinaryTransfer()) {
      return true;
    }
    return false;
  }

  protected final void execute(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
      throws SQLException {
    try {
      executeInternal(cachedQuery, queryParameters, flags);
    } catch (SQLException e) {
      // Don't retry composite queries as it might get partially executed
      if (cachedQuery.query.getSubqueries() != null
          || !connection.getQueryExecutor().willHealOnRetry(e)) {
        throw e;
      }
      cachedQuery.query.close();
      // Execute the query one more time
      executeInternal(cachedQuery, queryParameters, flags);
    }
  }

  /**
   * Sets parameter value of prepared statement to client logic binary format
   * @param queryParameters the query parameters
   * @param parameterIndex the index to change
   * @param x bytes data
   * @param customOid the field oid
   * @throws SQLException
   */
  private void setClientLogicBytea(ParameterList queryParameters, int parameterIndex, byte[] x, int customOid) throws SQLException {
    checkClosed();

    if (null == x) {
      queryParameters.setNull(parameterIndex, customOid);
      return;
    }
    byte[] copy = new byte[x.length];
    System.arraycopy(x, 0, copy, 0, x.length);
    queryParameters.setClientLogicBytea(parameterIndex, copy, 0, x.length, customOid);
  }
  /**
   * For prepared statements, replace client logic parameters from user input value to binary client logic value
   * @param statementName statement name that was used by preQuery on libpq
   * @param queryParameters the query parameters
   * @throws SQLException
   */
  private void replaceClientLogicParameters(String statementName, ParameterList queryParameters) throws SQLException {
    if (queryParameters.getInParameterCount() > 0) {
      ClientLogic clientLogic = this.connection.getClientLogic();
      if (clientLogic != null) {
        List<String> listParameterValuesBeeforeCL = new ArrayList<>();
        // Getting the string values of all parameters
        String[] arrParameterValuesBeforeCL = queryParameters.getLiteralValues();
        if (arrParameterValuesBeforeCL != null){
          for(int i = 0; i < queryParameters.getInParameterCount(); ++i) {
            String valueBeforCL = arrParameterValuesBeforeCL[i];
            if (valueBeforCL == null) {
              valueBeforCL = "";
            }
            listParameterValuesBeeforeCL.add(valueBeforCL);
          }
          List<String> modifiedParameters = new ArrayList<>();
          List<Integer> resultTypeOids = new ArrayList<>();
          try {
            // Getting the client logic binary value from the back-end
            clientLogic.replaceStatementParams(statementName, listParameterValuesBeeforeCL, modifiedParameters,
		      	    resultTypeOids);
          }
          catch (ClientLogicException e) {
            LOGGER.error("Errror: '" + e.getErrorText() + "' while running client logic to change parameters");
            throw new SQLException("Errror: '" + e.getErrorText() + "' while running client logic to change parameters");
          }
          int indexParam = 1;
          for (String modifiedParam : modifiedParameters) {
              int clientLogicTypeOid = resultTypeOids.get(indexParam-1);
              if (modifiedParam != null) {
                  // Convert the data to binary
                  byte[] dataInBytes = PGbytea.toBytes(modifiedParam.getBytes());
                  this.setClientLogicBytea(queryParameters, indexParam, dataInBytes, clientLogicTypeOid);
              } else if (clientLogicTypeOid != 0) {
                  // column with client logic, but empty input
                  this.setClientLogicBytea(queryParameters, indexParam, null, clientLogicTypeOid);
              } else {
                // nothing here
              }
              ++indexParam;
          }
        }
      }
    }
  }

  private void executeInternal(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
      throws SQLException {
    closeForNextExecution();
    // Replace the query for client logic case
    ClientLogic clientLogic = replaceQueryForClientLogic(cachedQuery, queryParameters, flags);
    // Enable cursor-based resultset if possible.
    if (fetchSize > 0 && !wantsScrollableResultSet() && !connection.getAutoCommit()
        && !wantsHoldableResultSet()) {
      flags |= QueryExecutor.QUERY_FORWARD_CURSOR;
    }

    if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
      flags |= QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS;

      // If the no results flag is set (from executeUpdate)
      // clear it so we get the generated keys results.
      if ((flags & QueryExecutor.QUERY_NO_RESULTS) != 0) {
        flags &= ~(QueryExecutor.QUERY_NO_RESULTS);
      }
    }

    if (isOneShotQuery(cachedQuery)) {
      flags |= QueryExecutor.QUERY_ONESHOT;
    }
    // Only use named statements after we hit the threshold. Note that only
    // named statements can be transferred in binary format.

    if (connection.getAutoCommit()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }

    // updateable result sets do not yet support binary updates
    if (concurrency != ResultSet.CONCUR_READ_ONLY) {
      flags |= QueryExecutor.QUERY_NO_BINARY_TRANSFER;
    }

    Query queryToExecute = cachedQuery.query;

    if (queryToExecute.isEmpty()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }
    shouldClientLogicRetry = false;
    if (!queryToExecute.isStatementDescribed() && forceBinaryTransfers
        && (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
      // Simple 'Q' execution does not need to know parameter types
      // When binaryTransfer is forced, then we need to know resulting parameter and column types,
      // thus sending a describe request.
      int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
      StatementResultHandler handler2 = new StatementResultHandler();
      if (clientLogic != null) {
        runQueryExecutorForClientLogic(queryParameters, clientLogic, queryToExecute, flags2, handler2, 0, 0);
      }
      else {
        connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler2, 0, 0,
                flags2);
      }
      ResultWrapper result2 = handler2.getResults();
      if (result2 != null) {
        result2.getResultSet().close();
      }
    }

    StatementResultHandler handler = new StatementResultHandler();
    synchronized (this) {
      result = null;
    }
    runQueryExecutor(queryParameters, flags, clientLogic, queryToExecute, handler);
    /* check if the resultset had any issues with handling client logic fields and issue a retry if it did */
    retryClientLogicExtract(flags, clientLogic, handler);
    if (shouldClientLogicRetry) {
        LOGGER.debug("query failed due to missing client logic cache - reloading the cache and trying again...");
        connection.getClientLogic().reloadCache();
        // So it would not happen again and again ...
        flags = flags | QueryExecutor.QUERY_RETRY_WITH_CLIENT_LOGIC_CACHE_RELOADS;
        executeInternal(cachedQuery, queryParameters, flags);
        return;
      }
    updateGeneratedKeyStatus(clientLogic, handler);
    runQueryPostProcess(clientLogic);
  }

  private void runQueryPostProcess(ClientLogic clientLogic) {
    try {
      if (clientLogic != null) {
        clientLogic.runQueryPostProcess();
      }
    }
    catch(ClientLogicException e) {
      LOGGER.error("Failed running runQueryPostProcess Error: " + e.getErrorCode() + ":" + e.getErrorText());
    }
  }

  private void runQueryExecutor(ParameterList queryParameters, int flags, ClientLogic clientLogic, Query queryToExecute, org.postgresql.jdbc.PgStatement.StatementResultHandler handler) throws SQLException {
    try {
      startTimer();
      if (clientLogic != null) {
        runQueryExecutorForClientLogic(queryParameters, clientLogic, queryToExecute, flags, handler, maxrows, fetchSize);
      }
      else {
        connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler, maxrows,
                fetchSize, flags);
      }
    } finally {
      killTimerTask();
    }
  }

  private void updateGeneratedKeyStatus(ClientLogic clientLogic, org.postgresql.jdbc.PgStatement.StatementResultHandler handler) throws SQLException {
    synchronized (this) {
      checkClosed();
      result = firstUnclosedResult = handler.getResults();

      if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
        generatedKeys = result;
        result = result.getNext();

        if (wantsGeneratedKeysOnce) {
          wantsGeneratedKeysOnce = false;
        }
      }
    }
  }

  private void runQueryExecutorForClientLogic(ParameterList queryParameters, ClientLogic clientLogic, Query queryToExecute, int flags2, org.postgresql.jdbc.PgStatement.StatementResultHandler handler2, int i, int i2) throws SQLException {
    try {
      connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler2, i, i2,
              flags2);
    } catch (SQLException sqlException) {
        if ((flags2 & QueryExecutor.QUERY_RETRY_WITH_CLIENT_LOGIC_CACHE_RELOADS) == 0 &&
            ClientLogic.checkIfReloadCache(sqlException)) {
            shouldClientLogicRetry = true;
        } else {
            // Client logic should be able to change the error message back to use user input
            String updatedMessage = clientLogic.clientLogicMessage(sqlException.getMessage());
            throw new SQLException(updatedMessage);
        }
    }
  }

  private ClientLogic replaceQueryForClientLogic(CachedQuery cachedQuery, ParameterList queryParameters, int flags) throws SQLException {
  	ClientLogic clientLogic = null;
    if ((flags & QueryExecutor.QUERY_EXECUTE_BYPASS_CLIENT_LOGIC) == 0) {
  		clientLogic = this.connection.getClientLogic();
    }
    if (clientLogic != null) {
      // we use subqueires to check multiple statements scenario return null if query is simpleQuery
      if (cachedQuery.query.getSubqueries() != null) {
        LOGGER.error("multiple statements is not allowed under client logic routine.");
        throw new SQLException("multiple statements is not allowed under client logic routine, please split it up into simpleQuery per statement.");
      }
      String modifiedQuery = cachedQuery.query.getNativeSql();
      try {
        if (this instanceof PgPreparedStatement) {
          if (!didRunPreQuery) {
            modifiedQuery = clientLogic.prepareQuery(modifiedQuery, statementName);
            didRunPreQuery = true;
          }
          replaceClientLogicParameters(statementName, queryParameters);
        }
        else {
          modifiedQuery = clientLogic.runQueryPreProcess(cachedQuery.query.getNativeSql());
        }
        cachedQuery.query.replaceNativeSqlForClientLogic(modifiedQuery);
      }
      catch(ClientLogicException e) {
        if (e.isParsingError()) {
          /*
           * we should not block bad queries to be sent to the server
    	     * PgConnection.isValid is based on error that is not parsed correctly
    	     */
    	    LOGGER.debug("pre query failed for parsing error, moving on");
        } else {
          LOGGER.debug("Failed running runQueryPreProcess on executeInternal " + e.getErrorCode() + ":" + e.getErrorText());
          throw new SQLException(e.getErrorText());
        }
      }
    }
    return clientLogic;
  }

    /**
     * retry client logic value parsing when failed
     *
     * @param flags query flags
     * @param clientLogic client logic object
     * @param handler results handler
     */
    private void retryClientLogicExtract(int flags, ClientLogic clientLogic, StatementResultHandler handler) {
        if (clientLogic == null) {
            return;
        }
        if ((flags & QueryExecutor.QUERY_NO_RESULTS) != 0) {
            // If no results flag is on, no need to check the result sets
            return;
        }
        if ((flags & QueryExecutor.QUERY_EXECUTE_BYPASS_CLIENT_LOGIC) != 0) {
            // If client logic flag is on, no need to check the result sets
            return;
        }

        ResultWrapper resultWrapper = handler.getResults();
        boolean isCacheReloaded = false;
        while (resultWrapper != null) {
            if (resultWrapper.getResultSet() != null) {
                if (resultWrapper.getResultSet() instanceof PgResultSet) {
                    PgResultSet pgRs = (PgResultSet) resultWrapper.getResultSet();
                    if (pgRs.getDidClientLogicFail()) {
                        LOGGER.debug("Failed to parse client logic value - reloading the cache and trying again...");
                        if (!isCacheReloaded) {
                            clientLogic.reloadCache();
                            isCacheReloaded = true;
                        }
                        try {
                            pgRs.clientLogicGetData(true);
                        } catch (SQLException e) {
                            LOGGER.error("Failed retryClientLogicExtract, error is: " + e.getMessage());
                        }
                    }
                }
            }
            resultWrapper = resultWrapper.getNext();
        }
    }

  public void setCursorName(String name) throws SQLException {
    checkClosed();
    // No-op.
  }

  private volatile boolean isClosed = false;

  @Override
  public int getUpdateCount() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null || result.getResultSet() != null) {
        return -1;
      }

      long count = result.getUpdateCount();
      return count > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : (int) count;
    }
  }

  public boolean getMoreResults() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null) {
        return false;
      }

      result = result.getNext();

      // Close preceding resultsets.
      while (firstUnclosedResult != result) {
        if (firstUnclosedResult.getResultSet() != null) {
          firstUnclosedResult.getResultSet().close();
        }
        firstUnclosedResult = firstUnclosedResult.getNext();
      }

      return (result != null && result.getResultSet() != null);
    }
  }

  public int getMaxRows() throws SQLException {
    checkClosed();
    return maxrows;
  }

  public void setMaxRows(int max) throws SQLException {
    checkClosed();
    if (max < 0) {
      throw new PSQLException(
          GT.tr("Maximum number of rows must be a value grater than or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    maxrows = max;
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    checkClosed();
    replaceProcessingEnabled = enable;
  }

  public int getQueryTimeout() throws SQLException {
    checkClosed();
    long seconds = timeout / 1000;
    if (seconds >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) seconds;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    setQueryTimeoutMs(seconds * 1000L);
  }

  /**
   * The queryTimeout limit is the number of milliseconds the driver will wait for a Statement to
   * execute. If the limit is exceeded, a SQLException is thrown.
   *
   * @return the current query timeout limit in milliseconds; 0 = unlimited
   * @throws SQLException if a database access error occurs
   */
  public long getQueryTimeoutMs() throws SQLException {
    checkClosed();
    return timeout;
  }

  /**
   * Sets the queryTimeout limit.
   *
   * @param millis - the new query timeout limit in milliseconds
   * @throws SQLException if a database access error occurs
   */
  public void setQueryTimeoutMs(long millis) throws SQLException {
    checkClosed();

    if (millis < 0) {
      throw new PSQLException(GT.tr("Query timeout must be a value greater than or equals to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    timeout = millis;
  }

  /**
   * <p>Either initializes new warning wrapper, or adds warning onto the chain.</p>
   *
   * <p>Although warnings are expected to be added sequentially, the warnings chain may be cleared
   * concurrently at any time via {@link #clearWarnings()}, therefore it is possible that a warning
   * added via this method is placed onto the end of the previous warning chain</p>
   *
   * @param warn warning to add
   */
  public void addWarning(SQLWarning warn) {
    NoticeListener listener;

    //copy reference to avoid NPE from concurrent modification of this.warnings
    final PSQLWarningWrapper warnWrap = this.warnings;
    if (warnWrap == null) {
      this.warnings = new PSQLWarningWrapper(warn);
    } else {
      warnWrap.addWarning(warn);
    }	    
    for(int i = 0; i < noticeListenerlist.size() ; i++)
	{
		listener = (NoticeListener)noticeListenerlist.get(i);
	    try{
	    	listener.noticeReceived(warn);
		}catch(Exception e){
        LOGGER.trace("noticeReceived failed. " + e.toString());
        } 
    }

  }

  public SQLWarning getWarnings() throws SQLException {
    checkClosed();
    //copy reference to avoid NPE from concurrent modification of this.warnings
    final PSQLWarningWrapper warnWrap = this.warnings;
    return warnWrap != null ? warnWrap.getFirstWarning() : null;
  }

  public int getMaxFieldSize() throws SQLException {
    return maxfieldSize;
  }

  public void setMaxFieldSize(int max) throws SQLException {
    checkClosed();
    if (max < 0) {
      throw new PSQLException(
          GT.tr("The maximum field size must be a value greater than or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    maxfieldSize = max;
  }

  /**
   * <p>Clears the warning chain.</p>
   * <p>Note that while it is safe to clear warnings while the query is executing, warnings that are
   * added between calls to {@link #getWarnings()} and #clearWarnings() may be missed.
   * Therefore you should hold a reference to the tail of the previous warning chain
   * and verify if its {@link SQLWarning#getNextWarning()} value is holds any new value.</p>
   */
  public void clearWarnings() throws SQLException {
    warnings = null;
  }

  public java.sql.ResultSet getResultSet() throws SQLException {
    synchronized (this) {
      checkClosed();

      if (result == null) {
        return null;
      }

      return result.getResultSet();
    }
  }

  /**
   * <B>Note:</B> even though {@code Statement} is automatically closed when it is garbage
   * collected, it is better to close it explicitly to lower resource consumption.
   *
   * {@inheritDoc}
   */
  public final void close() throws SQLException {
    // closing an already closed Statement is a no-op.
    synchronized (this) {
      if (isClosed) {
        return;
      }
      isClosed = true;
    }

    cancel();

    closeForNextExecution();

    closeImpl();
  }

  /**
   * This is guaranteed to be called exactly once even in case of concurrent {@link #close()} calls.
   * @throws SQLException in case of error
   */
  protected void closeImpl() throws SQLException {
  }

  /*
   *
   * The following methods are postgres extensions and are defined in the interface BaseStatement
   *
   */

  public long getLastOID() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null) {
        return 0;
      }
      return result.getInsertOID();
    }
  }

  public void setPrepareThreshold(int newThreshold) throws SQLException {
    checkClosed();

    if (newThreshold < 0) {
      forceBinaryTransfers = true;
      newThreshold = 1;
    }

    this.m_prepareThreshold = newThreshold;
  }

  public int getPrepareThreshold() {
    return m_prepareThreshold;
  }

  public void setUseServerPrepare(boolean flag) throws SQLException {
    setPrepareThreshold(flag ? 1 : 0);
  }

  public boolean isUseServerPrepare() {
    return false;
  }

  protected void checkClosed() throws SQLException {
    if (isClosed()) {
      throw new PSQLException(GT.tr("This statement has been closed."),
          PSQLState.OBJECT_NOT_IN_STATE);
    }
  }

  // ** JDBC 2 Extensions **

  public void addBatch(String p_sql) throws SQLException {
    checkClosed();

    if (batchStatements == null) {
      batchStatements = new ArrayList<Query>();
      batchParameters = new ArrayList<ParameterList>();
    }

    // Simple statements should not replace ?, ? with $1, $2
    boolean shouldUseParameterized = false;
    CachedQuery cachedQuery = connection.createQuery(p_sql, replaceProcessingEnabled, shouldUseParameterized);
    batchStatements.add(cachedQuery.query);
    batchParameters.add(null);
  }

  public void clearBatch() throws SQLException {
    if (batchStatements != null) {
      batchStatements.clear();
      batchParameters.clear();
    }
  }

  protected BatchResultHandler createBatchHandler(Query[] queries,
      ParameterList[] parameterLists) {
    return new BatchResultHandler(this, queries, parameterLists,
        wantsGeneratedKeysAlways);
  }

  private BatchResultHandler internalExecuteBatch() throws SQLException {

    // Construct query/parameter arrays.
    transformQueriesAndParameters();
    // Empty arrays should be passed to toArray
    // see http://shipilev.net/blog/2016/arrays-wisdom-ancients/
    Query[] queries = batchStatements.toArray(new Query[0]);
    ParameterList[] parameterLists = batchParameters.toArray(new ParameterList[0]);
    batchStatements.clear();
    batchParameters.clear();

    int flags;

    // Force a Describe before any execution? We need to do this if we're going
    // to send anything dependent on the Describe results, e.g. binary parameters.
    boolean preDescribe = false;

    if (wantsGeneratedKeysAlways) {
      /*
       * This batch will return generated keys, tell the executor to expect result rows. We also
       * force a Describe later so we know the size of the results to expect.
       *
       * If the parameter type(s) change between batch entries and the default binary-mode changes
       * we might get mixed binary and text in a single result set column, which we cannot handle.
       * To prevent this, disable binary transfer mode in batches that return generated keys. See
       * GitHub issue #267
       */
      flags = QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS | QueryExecutor.QUERY_NO_BINARY_TRANSFER;
    } else {
      // If a batch hasn't specified that it wants generated keys, using the appropriate
      // Connection.createStatement(...) interfaces, disallow any result set.
      flags = QueryExecutor.QUERY_NO_RESULTS;
    }

    PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
    if (preferQueryMode == PreferQueryMode.SIMPLE
        || (preferQueryMode == PreferQueryMode.EXTENDED_FOR_PREPARED
        && parameterLists[0] == null)) {
      flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
    }

    boolean sameQueryAhead = queries.length > 1 && queries[0] == queries[1];

    if (!sameQueryAhead
        // If executing the same query twice in a batch, make sure the statement
        // is server-prepared. In other words, "oneshot" only if the query is one in the batch
        // or the queries are different
        || isOneShotQuery(null)) {
      flags |= QueryExecutor.QUERY_ONESHOT;
    } else {
      // If a batch requests generated keys and isn't already described,
      // force a Describe of the query before proceeding. That way we can
      // determine the appropriate size of each batch by estimating the
      // maximum data returned. Without that, we don't know how many queries
      // we'll be able to queue up before we risk a deadlock.
      // (see v3.QueryExecutorImpl's MAX_BUFFERED_RECV_BYTES)

      // SameQueryAhead is just a quick way to issue pre-describe for batch execution
      // TODO: It should be reworked into "pre-describe if query has unknown parameter
      // types and same query is ahead".
      preDescribe = (wantsGeneratedKeysAlways || sameQueryAhead)
          && !queries[0].isStatementDescribed();
      /*
       * It's also necessary to force a Describe on the first execution of the new statement, even
       * though we already described it, to work around bug #267.
       */
      flags |= QueryExecutor.QUERY_FORCE_DESCRIBE_PORTAL;
    }

    if (connection.getAutoCommit()) {
      flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
    }

    //Client logic case - handle the queries
    handleQueriesForClientLogic(queries, parameterLists);

    BatchResultHandler handler;
    handler = createBatchHandler(queries, parameterLists);

    if ((preDescribe || forceBinaryTransfers)
        && (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
      // Do a client-server round trip, parsing and describing the query so we
      // can determine its result types for use in binary parameters, batch sizing,
      // etc.
      int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
      StatementResultHandler handler2 = new StatementResultHandler();
      try {
        connection.getQueryExecutor().execute(queries[0], parameterLists[0], handler2, 0, 0, flags2);
      } catch (SQLException e) {
        // Unable to parse the first statement -> throw BatchUpdateException
        handler.handleError(e);
        handler.handleCompletion();
        // Will not reach here (see above)
      }
      ResultWrapper result2 = handler2.getResults();
      if (result2 != null) {
        result2.getResultSet().close();
      }
    }

    synchronized (this) {
      result = null;
    }

    if(((PgConnection)this.connection).isBatchInsert() && parameterLists != null && checkParameterList(parameterLists)) {
    	try {
    		startTimer();
			connection.getQueryExecutor().executeBatch(queries,
					parameterLists, handler, maxrows, fetchSize, flags);

    	}finally {
    		 killTimerTask();
    	     synchronized (this) {
   	          checkClosed();
   	          if (wantsGeneratedKeysAlways) {
   	            generatedKeys = new ResultWrapper(handler.getGeneratedKeys());
   	          }
   	        }
    	}
    }else {
    	try {
    	      startTimer();
    	      connection.getQueryExecutor().execute(queries, parameterLists, handler, maxrows, fetchSize,
    	          flags);
    	    } finally {
    	      killTimerTask();
    	      synchronized (this) {
    	          checkClosed();
    	          if (wantsGeneratedKeysAlways) {
    	            generatedKeys = new ResultWrapper(handler.getGeneratedKeys());
    	          }
    	        }
    	    }
    }

    return handler;
  }

  private void handleQueriesForClientLogic(Query[] queries, ParameterList[] parameterLists) throws SQLException {
    ClientLogic clientLogic = this.connection.getClientLogic();

    if (clientLogic != null) {
      for (int queriesCounter = 0; queriesCounter < queries.length; ++queriesCounter) {
        String modifiedQuery = queries[queriesCounter].getNativeSql();
        try {
          if (this instanceof PgPreparedStatement) {
            if (!didRunPreQuery) {
              clientLogic.prepareQuery(modifiedQuery, statementName);
              didRunPreQuery = true;
            }

            if (parameterLists != null && parameterLists.length  > queriesCounter) {
              if (parameterLists[queriesCounter] != null) {
                replaceClientLogicParameters(statementName, parameterLists[queriesCounter]);
              }
            }
          }
          else {
            modifiedQuery = clientLogic.runQueryPreProcess(modifiedQuery);
            queries[queriesCounter].replaceNativeSqlForClientLogic(modifiedQuery);
          }
        }
        catch(ClientLogicException e) {
          LOGGER.debug("Failed running runQueryPreProcess on executeBatch " + e.getErrorCode() + ":" + e.getErrorText());
          throw new SQLException(e.getErrorText());
        }
      }
    }
  }

  public int[] executeBatch() throws SQLException {
    checkClosed();
    closeForNextExecution();

    if (batchStatements == null || batchStatements.isEmpty()) {
      return new int[0];
    }

    return internalExecuteBatch().getUpdateCount();
  }

	public boolean checkParameterList(ParameterList[] paramlist){
		for(int i=0; i< paramlist.length; i++){
			if(paramlist[i] == null){
				return false;
			}
		}
		return true;
	}

  public void cancel() throws SQLException {
    if (statementState == StatementCancelState.IDLE) {
      return;
    }
    if (!STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY,
        StatementCancelState.CANCELING)) {
      // Not in query, there's nothing to cancel
      return;
    }
    // Synchronize on connection to avoid spinning in killTimerTask
    synchronized (connection) {
      try {
        connection.cancelQuery();
      } finally {
        STATE_UPDATER.set(this, StatementCancelState.CANCELLED);
        connection.notifyAll(); // wake-up killTimerTask
      }
    }
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public int getFetchDirection() {
    return fetchdirection;
  }

  public int getResultSetConcurrency() {
    return concurrency;
  }

  public int getResultSetType() {
    return resultsettype;
  }

  public void setFetchDirection(int direction) throws SQLException {
    switch (direction) {
      case ResultSet.FETCH_FORWARD:
      case ResultSet.FETCH_REVERSE:
      case ResultSet.FETCH_UNKNOWN:
        fetchdirection = direction;
        break;
      default:
        throw new PSQLException(GT.tr("Invalid fetch direction constant: {0}.", direction),
            PSQLState.INVALID_PARAMETER_VALUE);
    }
  }

  public void setFetchSize(int rows) throws SQLException {
    checkClosed();
    if (rows < 0) {
      throw new PSQLException(GT.tr("Fetch size must be a value greater to or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    fetchSize = rows;
  }

  private void startTimer() {
    /*
     * there shouldn't be any previous timer active, but better safe than sorry.
     */
    cleanupTimer();

    STATE_UPDATER.set(this, StatementCancelState.IN_QUERY);

    if (timeout == 0) {
      return;
    }

    TimerTask cancelTask = new TimerTask() {
      public void run() {
        try {
          if (!CANCEL_TIMER_UPDATER.compareAndSet(PgStatement.this, this, null)) {
            // Nothing to do here, statement has already finished and cleared
            // cancelTimerTask reference
            return;
          }
          PgStatement.this.cancel();
        } catch (SQLException e) {
            LOGGER.trace("Catch SQLException while cancel this Statement. ", e);
        }
      }
    };

    CANCEL_TIMER_UPDATER.set(this, cancelTask);
    connection.addTimerTask(cancelTask, timeout);
  }

  /**
   * Clears {@link #cancelTimerTask} if any. Returns true if and only if "cancel" timer task would
   * never invoke {@link #cancel()}.
   */
  private boolean cleanupTimer() {
    TimerTask timerTask = CANCEL_TIMER_UPDATER.get(this);
    if (timerTask == null) {
      // If timeout is zero, then timer task did not exist, so we safely report "all clear"
      return timeout == 0;
    }
    if (!CANCEL_TIMER_UPDATER.compareAndSet(this, timerTask, null)) {
      // Failed to update reference -> timer has just fired, so we must wait for the query state to
      // become "cancelling".
      return false;
    }
    timerTask.cancel();
    connection.purgeTimerTasks();
    // All clear
    return true;
  }

  private void killTimerTask() {
    boolean timerTaskIsClear = cleanupTimer();
    // The order is important here: in case we need to wait for the cancel task, the state must be
    // kept StatementCancelState.IN_QUERY, so cancelTask would be able to cancel the query.
    // It is believed that this case is very rare, so "additional cancel and wait below" would not
    // harm it.
    if (timerTaskIsClear && STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY, StatementCancelState.IDLE)) {
      return;
    }

    // Being here means someone managed to call .cancel() and our connection did not receive
    // "timeout error"
    // We wait till state becomes "cancelled"
    boolean interrupted = false;
    synchronized (connection) {
      // state check is performed under synchronized so it detects "cancelled" state faster
      // In other words, it prevents unnecessary ".wait()" call
      while (!STATE_UPDATER.compareAndSet(this, StatementCancelState.CANCELLED, StatementCancelState.IDLE)) {
        try {
          // Note: wait timeout here is irrelevant since synchronized(connection) would block until
          // .cancel finishes
          connection.wait(10);
        } catch (InterruptedException e) { // NOSONAR
          // Either re-interrupt this method or rethrow the "InterruptedException"
          interrupted = true;
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  protected boolean getForceBinaryTransfer() {
    return forceBinaryTransfers;
  }

  @Override
  public long getLargeUpdateCount() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (result == null || result.getResultSet() != null) {
        return -1;
      }

      return result.getUpdateCount();
    }
  }

  public void setLargeMaxRows(long max) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setLargeMaxRows");
  }

  public long getLargeMaxRows() throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getLargeMaxRows");
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    checkClosed();
    closeForNextExecution();

    if (batchStatements == null || batchStatements.isEmpty()) {
      return new long[0];
    }

    return internalExecuteBatch().getLargeUpdateCount();
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    executeWithFlags(sql, QueryExecutor.QUERY_NO_RESULTS);
    checkNoResultUpdate();
    return getLargeUpdateCount();
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeLargeUpdate(sql);
    }

    return executeLargeUpdate(sql, (String[]) null);
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    if (columnIndexes == null || columnIndexes.length == 0) {
      return executeLargeUpdate(sql);
    }

    throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
            PSQLState.NOT_IMPLEMENTED);
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    if (columnNames != null && columnNames.length == 0) {
      return executeLargeUpdate(sql);
    }

    wantsGeneratedKeysOnce = true;
    executeCachedSql(sql, 0, columnNames);

    return getLargeUpdateCount();
  }

  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public void setPoolable(boolean poolable) throws SQLException {
    checkClosed();
    this.poolable = poolable;
  }

  public boolean isPoolable() throws SQLException {
    checkClosed();
    return poolable;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  public void closeOnCompletion() throws SQLException {
    closeOnCompletion = true;
  }

  public boolean isCloseOnCompletion() throws SQLException {
    return closeOnCompletion;
  }

  protected void checkCompletion() throws SQLException {
    if (!closeOnCompletion) {
      return;
    }

    synchronized (this) {
      ResultWrapper resultWrapper = firstUnclosedResult;
      while (resultWrapper != null) {
        if (resultWrapper.getResultSet() != null && !resultWrapper.getResultSet().isClosed()) {
          return;
        }
        resultWrapper = resultWrapper.getNext();
      }
    }

    // prevent all ResultSet.close arising from Statement.close to loop here
    closeOnCompletion = false;
    try {
      close();
    } finally {
      // restore the status if one rely on isCloseOnCompletion
      closeOnCompletion = true;
    }
  }

  public boolean getMoreResults(int current) throws SQLException {
    synchronized (this) {
      checkClosed();
      // CLOSE_CURRENT_RESULT
      if (current == Statement.CLOSE_CURRENT_RESULT && result != null
          && result.getResultSet() != null) {
        result.getResultSet().close();
      }

      // Advance resultset.
      if (result != null) {
        result = result.getNext();
      }

      // CLOSE_ALL_RESULTS
      if (current == Statement.CLOSE_ALL_RESULTS) {
        // Close preceding resultsets.
        while (firstUnclosedResult != result) {
          if (firstUnclosedResult.getResultSet() != null) {
            firstUnclosedResult.getResultSet().close();
          }
          firstUnclosedResult = firstUnclosedResult.getNext();
        }
      }

      // Done.
      return (result != null && result.getResultSet() != null);
    }
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    synchronized (this) {
      checkClosed();
      if (generatedKeys == null || generatedKeys.getResultSet() == null) {
        return createDriverResultSet(new Field[0], new ArrayList<byte[][]>());
      }

      return generatedKeys.getResultSet();
    }
  }

  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeUpdate(sql);
    }

    return executeUpdate(sql, (String[]) null);
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    if (columnIndexes == null || columnIndexes.length == 0) {
      return executeUpdate(sql);
    }

    throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
        PSQLState.NOT_IMPLEMENTED);
  }

  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    if (columnNames != null && columnNames.length == 0) {
      return executeUpdate(sql);
    }

    wantsGeneratedKeysOnce = true;
    if (!executeCachedSql(sql, 0, columnNames)) {
      // no resultset returned. What's a pity!
    }
    return getUpdateCount();
  }

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    }
    return execute(sql, (String[]) null);
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    if (columnIndexes != null && columnIndexes.length == 0) {
      return execute(sql);
    }

    throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
        PSQLState.NOT_IMPLEMENTED);
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException {
    if (columnNames != null && columnNames.length == 0) {
      return execute(sql);
    }

    wantsGeneratedKeysOnce = true;
    return executeCachedSql(sql, 0, columnNames);
  }

  public int getResultSetHoldability() throws SQLException {
    return rsHoldability;
  }

  public ResultSet createDriverResultSet(Field[] fields, List<byte[][]> tuples)
      throws SQLException {
    return createResultSet(null, fields, tuples, null);
  }

  protected void transformQueriesAndParameters() throws SQLException {
  }

  public void addNoticeListener(NoticeListener listener) throws SQLException {
	  noticeListenerlist.add(listener);
  }

}
