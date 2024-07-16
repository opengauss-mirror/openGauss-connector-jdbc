/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.PGNotification;
import org.postgresql.PGProperty;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.CachedQuery;
import org.postgresql.core.ConnectionFactory;
import org.postgresql.core.Encoding;
import org.postgresql.core.Oid;
import org.postgresql.core.Provider;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ReplicationProtocol;
import org.postgresql.core.ResultHandlerBase;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.SqlCommand;
import org.postgresql.core.TransactionState;
import org.postgresql.core.TypeInfo;
import org.postgresql.core.Utils;
import org.postgresql.core.Version;
import org.postgresql.fastpath.Fastpath;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationConnectionImpl;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.LruCache;
import org.postgresql.util.PGBinaryObject;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.xml.DefaultPGXmlFactoryFactory;
import org.postgresql.xml.LegacyInsecurePGXmlFactoryFactory;
import org.postgresql.xml.PGXmlFactoryFactory;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.io.File;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.postgresql.core.types.PGClob;
import org.postgresql.core.types.PGBlob;


public class PgConnection implements BaseConnection {

  private static Log LOGGER = Logger.getLogger(PgConnection.class.getName());

  private static final SQLPermission SQL_PERMISSION_ABORT = new SQLPermission("callAbort");
  private static final SQLPermission SQL_PERMISSION_NETWORK_TIMEOUT = new SQLPermission("setNetworkTimeout");
  private static final Map<String,String> CONNECTION_INFO_REPORT_BLACK_LIST;
  static {
      CONNECTION_INFO_REPORT_BLACK_LIST = new HashMap<>();
      CONNECTION_INFO_REPORT_BLACK_LIST.put("user","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("sslcert","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("password","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("sslkey","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("sslpassword","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("PGHOSTURL","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("PGPORTURL","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("PGPORT","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("PGHOST","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("PGDBNAME","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("sslenccert","");
      CONNECTION_INFO_REPORT_BLACK_LIST.put("sslenckey","");
  }

    /**set
     * Protects current statement from cancelTask starting, waiting for a bit, and waking up exactly
     * on subsequent query execution. The idea is to atomically compare and swap the reference to the
     * task, so the task can detect that statement executes different query than the one the
     * cancelTask was created. Note: the field must be set/get/compareAndSet via
     * {@link #CANCEL_TIMER_UPDATER} as per {@link AtomicReferenceFieldUpdater} javadoc.
     */
  private AtomicReferenceFieldUpdater<PgStatement, TimerTask> CANCEL_TIMER_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PgStatement.class, TimerTask.class, "cancelTimerTask");

  //
  // Data initialized on construction:
  //
  private final Properties _clientInfo;

  /* URL we were created via */
  private final String creatingURL;

  private Throwable openStackTrace;

  /* Actual network handler */
  private final QueryExecutor queryExecutor;

  /* Query that runs COMMIT */
  private final Query commitQuery;
  /* Query that runs ROLLBACK */
  private final Query rollbackQuery;

  private ClientLogic clientLogic = null;

  private final TypeInfo _typeCache;

  private boolean disableColumnSanitiser = false;

  // Default statement prepare threshold.
  protected int prepareThreshold;

  /**
   * Default fetch size for statement.
   *
   * @see PGProperty#DEFAULT_ROW_FETCH_SIZE
   */
  protected int defaultFetchSize;

  // Default forcebinary option.
  protected boolean forcebinary = false;

  private int rsHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
  private int savepointId = 0;
  // Connection's autocommit state.
  private boolean autoCommit = true;
  // Connection's readonly state.
  private boolean readOnly = false;
  
  //Connection allow readonly or not
  private boolean allowReadOnly = true;
  
  public boolean batchInsert = true;
	
  public boolean blobmode = true;
  
  //Default statement fetchsize
  private int fetchSize = -1;

  // Bind String to UNSPECIFIED or VARCHAR?
  private final boolean bindStringAsVarchar;

  // Current warnings; there might be more on queryExecutor too.
  private SQLWarning firstWarning = null;

  // True if bit to string else bit to boolean.
  private boolean bitToString = false;

  // Timer for scheduling TimerTasks for this connection.
  // Only instantiated if a task is actually scheduled.
  private volatile Timer cancelTimer = null;

  private PreparedStatement checkConnectionQuery;
  /**
   * Replication protocol in current version postgresql(10devel) supports a limited number of
   * commands.
   */
  private final boolean replicationConnection;

  private final LruCache<FieldMetadata.Key, FieldMetadata> fieldMetadataCache;

  private final String xmlFactoryFactoryClass;
  private PGXmlFactoryFactory xmlFactoryFactory;
  private String socketAddress;
  private String secSocketAddress;
  private boolean adaptiveSetSQLType = false;
  private boolean isDolphinCmpt = false;
  private PgDatabase pgDatabase;

  final CachedQuery borrowQuery(String sql) throws SQLException {
    return queryExecutor.borrowQuery(sql);
  }

  final CachedQuery borrowCallableQuery(String sql) throws SQLException {
    return queryExecutor.borrowCallableQuery(sql);
  }

  private CachedQuery borrowReturningQuery(String sql, String[] columnNames) throws SQLException {
    return queryExecutor.borrowReturningQuery(sql, columnNames);
  }

  @Override
  public CachedQuery createQuery(String sql, boolean escapeProcessing, boolean isParameterized,
      String... columnNames)
      throws SQLException {
    return queryExecutor.createQuery(sql, escapeProcessing, isParameterized, columnNames);
  }

  void releaseQuery(CachedQuery cachedQuery) {
    queryExecutor.releaseQuery(cachedQuery);
  }

  @Override
  public void setFlushCacheOnDeallocate(boolean flushCacheOnDeallocate) {
    queryExecutor.setFlushCacheOnDeallocate(flushCacheOnDeallocate);
    LOGGER.debug("  setFlushCacheOnDeallocate = " + flushCacheOnDeallocate);
  }

  public PgDatabase getPgDatabase() {
    return pgDatabase;
  }

  //
  // Ctor.
  //
  public PgConnection(HostSpec[] hostSpecs,
                      String user,
                      String database,
                      Properties info,
                      String url) throws SQLException {
    // Print out the driver version number
    LOGGER.debug(org.postgresql.util.DriverInfo.DRIVER_FULL_NAME);
    try {
        if (info.getProperty("fetchsize") != null) {
            fetchSize = Integer.parseInt(info.getProperty("fetchsize"));
            if (fetchSize < 0) {
                fetchSize = -1;
            }
        }
    } catch (Exception e) {
        LOGGER.trace("Catch Exception while transfor fetchsize to integer. ", e);
    }
    try {
        String allow = info.getProperty("allowReadOnly");
        if (allow != null && allow.trim().equals("") == false) {
            if (allow.equalsIgnoreCase("FALSE")) {
                allowReadOnly = false;
            }
        }
    } catch (Exception e) {
        LOGGER.trace("Catch Exception while compare allow and FALSE. ", e);
    }

    this.creatingURL = url;

    bitToString = PGProperty.BIT_TO_STRING.getBoolean(info);
    setDefaultFetchSize(PGProperty.DEFAULT_ROW_FETCH_SIZE.getInt(info));

    setPrepareThreshold(PGProperty.PREPARE_THRESHOLD.getInt(info));
    if (prepareThreshold == -1) {
      setForceBinary(true);
    }

    // Now make the initial connection and set up local state
    this.queryExecutor = ConnectionFactory.openConnection(hostSpecs, user, database, info);
    this.socketAddress = this.queryExecutor.getSocketAddress();
    this.secSocketAddress = this.queryExecutor.getSecSocketAddress();
    // WARNING for unsupported servers (8.1 and lower are not supported)
    if (LOGGER.isWarnEnabled() && !haveMinimumServerVersion(ServerVersion.v8_2)) {
      LOGGER.warn("Unsupported Server Version: " + queryExecutor.getServerVersion());
    }
    
    Set<Integer> binaryOids = getBinaryOids(info);
    // split for receive and send for better control
    Set<Integer> useBinarySendForOids = new HashSet<Integer>(binaryOids);

    Set<Integer> useBinaryReceiveForOids = new HashSet<Integer>(binaryOids);

    /*
     * Does not pass unit tests because unit tests expect setDate to have millisecond accuracy
     * whereas the binary transfer only supports date accuracy.
     */
    useBinarySendForOids.remove(Oid.DATE);

    queryExecutor.setBinaryReceiveOids(useBinaryReceiveForOids);
    queryExecutor.setBinarySendOids(useBinarySendForOids);

    //
    // String -> text or unknown?
    //

    String stringType = PGProperty.STRING_TYPE.get(info);
    if (stringType != null) {
      if (stringType.equalsIgnoreCase("unspecified")) {
        bindStringAsVarchar = false;
      } else if (stringType.equalsIgnoreCase("varchar")) {
        bindStringAsVarchar = true;
      } else {
        throw new PSQLException(
            GT.tr("Unsupported value for stringtype parameter: " + stringType),
            PSQLState.INVALID_PARAMETER_VALUE);
      }
    } else {
      bindStringAsVarchar = true;
    }

    /* set dolphin.b_compatibility_mode to the value of PGProperty.B_CMPT_MODE */
    this.setDolphinCmpt(PGProperty.B_CMPT_MODE.getBoolean(info));

    int unknownLength = PGProperty.UNKNOWN_LENGTH.getInt(info);

    // Initialize object handling
    _typeCache = createTypeInfo(this, unknownLength);
    _typeCache.setPGTypes();
    initObjectTypes(info);

    pgDatabase = new PgDatabase(this);
    pgDatabase.setDolphin();

    // Initialize timestamp stuff
    timestampUtils = new TimestampUtils(!queryExecutor.getIntegerDateTimes(), new Provider<TimeZone>() {
      @Override
      public TimeZone get() {
        return queryExecutor.getTimeZone();
      }
    });
    timestampUtils.setTimestampNanoFormat(PGProperty.TIMESTAMP_NANO_FORMAT.getInteger(info));
    timestampUtils.setDolphin(pgDatabase.isDolphin());

    // Initialize common queries.
    // isParameterized==true so full parse is performed and the engine knows the query
    // is not a compound query with ; inside, so it could use parse/bind/exec messages
    commitQuery = createQuery("COMMIT", false, true).query;
    rollbackQuery = createQuery("ROLLBACK", false, true).query;

    if (PGProperty.LOG_UNCLOSED_CONNECTIONS.getBoolean(info)) {
      openStackTrace = new Throwable("Connection was created at this point:");
    }
    this.disableColumnSanitiser = PGProperty.DISABLE_COLUMN_SANITISER.getBoolean(info);

    if (haveMinimumServerVersion(ServerVersion.v8_3)) {
      _typeCache.addCoreType("uuid", Oid.UUID, Types.OTHER, "java.util.UUID", Oid.UUID_ARRAY);
      _typeCache.addCoreType("xml", Oid.XML, Types.SQLXML, "java.sql.SQLXML", Oid.XML_ARRAY);
    }
    _typeCache.addCoreType("clob", Oid.CLOB, Types.CLOB, "java.sql.CLOB", Oid.UNSPECIFIED);
    _typeCache.addCoreType("blob", Oid.BLOB, Types.BLOB, "java.sql.BLOB", Oid.UNSPECIFIED);

    this._clientInfo = new Properties();
    if (haveMinimumServerVersion(ServerVersion.v9_0)) {
      String appName = PGProperty.APPLICATION_NAME.get(info);
      if (appName == null) {
        appName = "";
      }
      this._clientInfo.put("ApplicationName", appName);
    }
    String appType = PGProperty.APPLICATION_TYPE.get(info);
    if(appType == null)
        appType = "";
    this._clientInfo.put("ApplicationType", appType);
    fieldMetadataCache = new LruCache<FieldMetadata.Key, FieldMetadata>(
            Math.max(0, PGProperty.DATABASE_METADATA_CACHE_FIELDS.getInt(info)),
            Math.max(0, PGProperty.DATABASE_METADATA_CACHE_FIELDS_MIB.getInt(info) * 1024 * 1024),
        false);

    xmlFactoryFactoryClass = PGProperty.XML_FACTORY_FACTORY.get(info);

    replicationConnection = PGProperty.REPLICATION.get(info) != null;
    if(replicationConnection) {
      return;
    }

    /* Get Database GUC parameters when connection established. */
    Statement stmtGetGuc = null;
    ResultSet rsGetGuc = null;
    Statement stmtSetGuc = null;
    
    try {
        String connectionExtraInfo = info.getProperty("connectionExtraInfo");

        // If you need to do something as initialization, do it here.
        String getGucSQL = "select name, setting from pg_settings where name in ('connection_info')";
        stmtGetGuc = createStatement();
        rsGetGuc = stmtGetGuc.executeQuery(getGucSQL);
        boolean useConnectionInfo = false;
        boolean useConnectionExtraInfo = false;
        while (rsGetGuc.next()) {
            if (rsGetGuc.getString(1).equalsIgnoreCase("connection_info")) {
                useConnectionInfo = true;
                useConnectionExtraInfo = Boolean.valueOf(connectionExtraInfo);
            }
        }

        // Done to scan all GUC results here, and begin to do some initialization.
        if (useConnectionInfo) {
            String connectionInfo = getConnectionInfo(useConnectionExtraInfo, info);
            String setConnectionInfoSql = "set connection_info = '" + connectionInfo.replace("'", "''") + "'";
            if (!setConnectionInfoSql.contains(";")) {
                stmtSetGuc = createStatement();
                stmtSetGuc.executeUpdate(setConnectionInfoSql);
            } else {
                LOGGER.debug("connection_info contains \";\", which is not allowed.");
            }
        }
    } catch (SQLException e) {
        // The connection dialing should not be interrupted whatever happens.
        LOGGER.trace("Catch SQLException while connection. ", e);
    } finally {
        if (stmtGetGuc != null) stmtGetGuc.close();
        if (rsGetGuc != null) rsGetGuc.close();
        if (stmtSetGuc != null) stmtSetGuc.close();
    }

    String blobString = info.getProperty("blobMode");

    if (blobString != null && blobString.equalsIgnoreCase("OFF")) {
        blobmode = false;
    }

    String batchString = info.getProperty("batchMode");
    if (batchString == null || batchString.equalsIgnoreCase("AUTO")) {
        if (batchInsert) {
            final String sql = "select count(*) from pg_settings where name = 'support_batch_bind' and setting = 'on';";
            boolean flag = false;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = createStatement();
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    if (rs.getInt(1) == 1) {
                        flag = true;
                    }
                }
            } catch (SQLException e) {
                LOGGER.trace("Failed to create statement or execute query, Error: " + e.getMessage());
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (SQLException e) {
                    LOGGER.trace("Failed to close resultset,Error:" + e.getMessage());
                }
                try {
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    LOGGER.trace("Failed to close statement,Error:" + e.getMessage());
                }
            }
            if (flag == false) {
                LOGGER.trace("WARNING, client suggest to use batch mode while the server is not supported");
                batchInsert = false;
            }
        }
    } else if (batchString.equalsIgnoreCase("OFF")) {
        batchInsert = false;
    } else if (batchString.equalsIgnoreCase("ON")) {
        batchInsert = true;
    } else {
        LOGGER.trace("WARNING, unrecognized batchmode type");
        batchInsert = false;
    }

    adaptiveSetSQLType = PGProperty.ADAPTIVE_SET_SQL_TYPE.getBoolean(info);

    initClientLogic(info);
  }

  /**
   * Link the client logic JNI
   * @param info connection settings
   * @throws SQLException
   */
  private void initClientLogic(Properties info) throws SQLException {
    //Connecting the Client Logic so
    if (PGProperty.PG_CLIENT_LOGIC.get(info) != null && PGProperty.PG_CLIENT_LOGIC.get(info).equals("1")) {
      String autoBalance = info.getProperty("autoBalance");
      String targetType = info.getProperty("targetServerType");
      if ((autoBalance != null && !autoBalance.equals("false")) || (targetType != null)) {
        LOGGER.error("[client encryption] Failed connecting to client logic as autobalance or targetType is set");
        clientLogic = null;
        throw new PSQLException(
           GT.tr("Failed connecting to client logic"),
           PSQLState.INVALID_PARAMETER_VALUE);
      }
    	LOGGER.trace("Initiating client logic");
      try {
      	clientLogic = new ClientLogic();
    		String databaseName = PGProperty.PG_DBNAME.get(info);
    		clientLogic.linkClientLogic(databaseName, this);
      } 
      catch (ClientLogicException e) {
      	clientLogic = null;
      	LOGGER.error("Failed connecting to client logic");
        throw new PSQLException(
            GT.tr("Failed connecting to client logic" + e.getMessage()),
            PSQLState.INVALID_PARAMETER_VALUE);
      }
    }
    else {
    	LOGGER.trace("Client logic is off");
    }
  }

  private static Set<Integer> getBinaryOids(Properties info) throws PSQLException {
	 boolean binaryTransfer =  false;
     binaryTransfer = PGProperty.BINARY_TRANSFER.getBoolean(info);
    // Formats that currently have binary protocol support
    Set<Integer> binaryOids = new HashSet<Integer>(32);
    if (binaryTransfer) {
      binaryOids.add(Oid.BYTEA);
      binaryOids.add(Oid.INT2);
      binaryOids.add(Oid.INT4);
      binaryOids.add(Oid.INT8);
      binaryOids.add(Oid.FLOAT4);
      binaryOids.add(Oid.FLOAT8);
      binaryOids.add(Oid.TIME);
      binaryOids.add(Oid.DATE);
      binaryOids.add(Oid.TIMETZ);
      binaryOids.add(Oid.TIMESTAMP);
      binaryOids.add(Oid.TIMESTAMPTZ);
      binaryOids.add(Oid.INT2_ARRAY);
      binaryOids.add(Oid.INT4_ARRAY);
      binaryOids.add(Oid.INT8_ARRAY);
      binaryOids.add(Oid.FLOAT4_ARRAY);
      binaryOids.add(Oid.FLOAT8_ARRAY);
      binaryOids.add(Oid.VARCHAR_ARRAY);
      binaryOids.add(Oid.TEXT_ARRAY);
      binaryOids.add(Oid.POINT);
      binaryOids.add(Oid.BOX);
      binaryOids.add(Oid.UUID);
    }

    binaryOids.addAll(getOidSet(PGProperty.BINARY_TRANSFER_ENABLE.get(info)));
    binaryOids.removeAll(getOidSet(PGProperty.BINARY_TRANSFER_DISABLE.get(info)));
    return binaryOids;
  }

  private static Set<Integer> getOidSet(String oidList) throws PSQLException {
    Set<Integer> oids = new HashSet<Integer>();
    StringTokenizer tokenizer = new StringTokenizer(oidList, ",");
    while (tokenizer.hasMoreTokens()) {
      String oid = tokenizer.nextToken();
      oids.add(Oid.valueOf(oid));
    }
    return oids;
  }

  private String oidsToString(Set<Integer> oids) {
    StringBuilder sb = new StringBuilder();
    for (Integer oid : oids) {
      sb.append(Oid.toString(oid));
      sb.append(',');
    }
    if (sb.length() > 0) {
      sb.setLength(sb.length() - 1);
    } else {
      sb.append(" <none>");
    }
    return sb.toString();
  }

    private String getConnectionInfo(boolean withExtraInfo, Properties info) {
        String connectionInfo = "";
        String gsVersion = null;
        String driverPath = null;
        String OSUser = null;
        String urlConfiguration = null;
        gsVersion = Driver.getGSVersion();
        if (withExtraInfo) {
            OSUser = System.getProperty("user.name");
            try {
                File jarDir = new File(Driver.class.getProtectionDomain().getCodeSource().getLocation().toURI());
                driverPath = jarDir.getCanonicalPath();
            } catch (URISyntaxException | IOException | IllegalArgumentException e) {
                driverPath = "";
                LOGGER.trace("Failed to make connection_info as there is an exception: " + e.getMessage());
            }
            urlConfiguration = reassembleUrl(info);
        }
        connectionInfo = "{" + "\"driver_name\":\"JDBC\"," + "\"driver_version\":\""
                + gsVersion.replace("\"", "\\\"") + "\"";
        if (withExtraInfo && driverPath != null) {
            connectionInfo += ",\"driver_path\":\"" + driverPath.replace("\\", "\\\\").replace("\"", "\\\"") + "\","
                    + "\"os_user\":\"" + OSUser.replace("\"", "\\\"") + "\","
                    + "\"urlConfiguration\":\"" + urlConfiguration.replace("\"", "\\\"") + "\"";

        }
        connectionInfo += "}";
        return connectionInfo;
    }

    private String reassembleUrl(Properties info) {
        StringBuffer urlConfiguration = new StringBuffer();
        if (creatingURL.startsWith("jdbc:postgresql:")) {
            urlConfiguration.append("jdbc:postgresql://");
        } else if (creatingURL.startsWith("jdbc:dws:iam:")) {
            urlConfiguration.append("jdbc:dws:iam://");
        } else if (creatingURL.startsWith("jdbc:opengauss:")) {
            urlConfiguration.append("jdbc:opengauss://");
        } else {
            urlConfiguration.append("jdbc:gaussdb://");
        }

        String[] ports = info.getProperty("PGPORTURL").split(",");
        String[] hosts = info.getProperty("PGHOSTURL").split(",", ports.length);
        for (int i = 0; i < hosts.length; ++i) {
            urlConfiguration.append(hosts[i] + ":" + ports[i] + ",");
        }
        urlConfiguration.deleteCharAt(urlConfiguration.length() - 1);

        urlConfiguration.append("/" + info.getProperty("PGDBNAME") + "?");
        for (String propertyName : info.stringPropertyNames()) {
            if(CONNECTION_INFO_REPORT_BLACK_LIST.get(propertyName) == null){
                urlConfiguration.append(propertyName+"="+info.getProperty(propertyName)+"&");
            }
        }
        urlConfiguration.deleteCharAt(urlConfiguration.length() - 1);
        return urlConfiguration.toString();
    }
  private final TimestampUtils timestampUtils;

  public TimestampUtils getTimestampUtils() {
    return timestampUtils;
  }

  /**
   * The current type mappings.
   */
  protected Map<String, Class<?>> typemap;

  @Override
  public Statement createStatement() throws SQLException {
    // We now follow the spec and default to TYPE_FORWARD_ONLY.
    return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkClosed();
    return typemap;
  }

  public QueryExecutor getQueryExecutor() {
    return queryExecutor;
  }

  public ReplicationProtocol getReplicationProtocol() {
    return queryExecutor.getReplicationProtocol();
  }

  /**
   * This adds a warning to the warning chain.
   *
   * @param warn warning to add
   */
  public void addWarning(SQLWarning warn) {
    // Add the warning to the chain
    if (firstWarning != null) {
      firstWarning.setNextWarning(warn);
    } else {
      firstWarning = warn;
    }

  }

  @Override
  public ResultSet execSQLQuery(String s) throws SQLException {
    return execSQLQuery(s, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public ResultSet execSQLQuery(String s, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    BaseStatement stat = (BaseStatement) createStatement(resultSetType, resultSetConcurrency);
    boolean hasResultSet = stat.executeWithFlags(s, QueryExecutor.QUERY_SUPPRESS_BEGIN);

    while (!hasResultSet && stat.getUpdateCount() != -1) {
      hasResultSet = stat.getMoreResults();
    }

    if (!hasResultSet) {
      throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
    }

    // Transfer warnings to the connection, since the user never
    // has a chance to see the statement itself.
    SQLWarning warnings = stat.getWarnings();
    if (warnings != null) {
      addWarning(warnings);
    }

    return stat.getResultSet();
  }

  @Override
  public void execSQLUpdate(String s) throws SQLException {
    BaseStatement stmt = (BaseStatement) createStatement();
    if (stmt.executeWithFlags(s, QueryExecutor.QUERY_NO_METADATA | QueryExecutor.QUERY_NO_RESULTS
        | QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
      throw new PSQLException(GT.tr("A result was returned when none was expected."),
          PSQLState.TOO_MANY_RESULTS);
    }

    // Transfer warnings to the connection, since the user never
    // has a chance to see the statement itself.
    SQLWarning warnings = stmt.getWarnings();
    if (warnings != null) {
      addWarning(warnings);
    }

    stmt.close();
  }

  /**
   * <p>In SQL, a result table can be retrieved through a cursor that is named. The current row of a
   * result can be updated or deleted using a positioned update/delete statement that references the
   * cursor name.</p>
   *
   * <p>We do not support positioned update/delete, so this is a no-op.</p>
   *
   * @param cursor the cursor name
   * @throws SQLException if a database access error occurs
   */
  public void setCursorName(String cursor) throws SQLException {
    checkClosed();
    // No-op.
  }

  /**
   * getCursorName gets the cursor name.
   *
   * @return the current cursor name
   * @throws SQLException if a database access error occurs
   */
  public String getCursorName() throws SQLException {
    checkClosed();
    return null;
  }

  /**
   * <p>We are required to bring back certain information by the DatabaseMetaData class. These
   * functions do that.</p>
   *
   * <p>Method getURL() brings back the URL (good job we saved it)</p>
   *
   * @return the url
   * @throws SQLException just in case...
   */
  public String getURL() throws SQLException {
    return creatingURL;
  }

  /**
   * Method getUserName() brings back the User Name (again, we saved it).
   *
   * @return the user name
   * @throws SQLException just in case...
   */
  public String getUserName() throws SQLException {
    return queryExecutor.getUser();
  }

  public Fastpath getFastpathAPI() throws SQLException {
    checkClosed();
    if (fastpath == null) {
      fastpath = new Fastpath(this);
    }
    return fastpath;
  }

  // This holds a reference to the Fastpath API if already open
  private Fastpath fastpath = null;

  public LargeObjectManager getLargeObjectAPI() throws SQLException {
    checkClosed();
    if (largeobject == null) {
      largeobject = new LargeObjectManager(this);
    }
    return largeobject;
  }

  // This holds a reference to the LargeObject API if already open
  private LargeObjectManager largeobject = null;

  /*
   * This method is used internally to return an object based around org.postgresql's more unique
   * data types.
   *
   * <p>It uses an internal HashMap to get the handling class. If the type is not supported, then an
   * instance of org.postgresql.util.PGobject is returned.
   *
   * You can use the getValue() or setValue() methods to handle the returned object. Custom objects
   * can have their own methods.
   *
   * @return PGobject for this type, and set to value
   *
   * @exception SQLException if value is not correct for this type
   */
  @Override
  public Object getObject(String type, String value, byte[] byteValue) throws SQLException {
    if (typemap != null) {
      Class<?> c = typemap.get(type);
      if (c != null) {
        // Handle the type (requires SQLInput & SQLOutput classes to be implemented)
        throw new PSQLException(GT.tr("Custom type maps are not supported."),
            PSQLState.NOT_IMPLEMENTED);
      }
    }

    PGobject obj = null;

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Constructing object from type=" + type + " value=<" + value + ">");
    }

    try {
      Class<? extends PGobject> klass = _typeCache.getPGobject(type);

      // If className is not null, then try to instantiate it,
      // It must be basetype PGobject

      // This is used to implement the org.postgresql unique types (like lseg,
      // point, etc).

      if (klass != null) {
        obj = klass.newInstance();
        obj.setType(type);
        if (byteValue != null && obj instanceof PGBinaryObject) {
          PGBinaryObject binObj = (PGBinaryObject) obj;
          binObj.setByteValue(byteValue, 0);
        } else {
          obj.setValue(value);
        }
      } else {
        // If className is null, then the type is unknown.
        // so return a PGobject with the type set, and the value set
        obj = new PGobject();
        obj.setType(type);
        obj.setValue(value);
      }

      return obj;
    } catch (SQLException sx) {
      // rethrow the exception. Done because we capture any others next
      throw sx;
    } catch (Exception ex) {
      throw new PSQLException(GT.tr("Failed to create object for: {0}.", type),
          PSQLState.CONNECTION_FAILURE, ex);
    }
  }

  protected TypeInfo createTypeInfo(BaseConnection conn, int unknownLength) {
    return new TypeInfoCache(conn, unknownLength);
  }

  public TypeInfo getTypeInfo() {
    return _typeCache;
  }

  @Override
  public void addDataType(String type, String name) {
    try {
      addDataType(type, Class.forName(name).asSubclass(PGobject.class));
    } catch (Exception e) {
      throw new RuntimeException("Cannot register new type: " + e);
    }
  }

  @Override
  public void addDataType(String type, Class<? extends PGobject> klass) throws SQLException {
    checkClosed();
    _typeCache.addDataType(type, klass);
  }

  // This initialises the objectTypes hash map
  private void initObjectTypes(Properties info) throws SQLException {
    // Add in the types that come packaged with the driver.
    // These can be overridden later if desired.
    addDataType("box", org.postgresql.geometric.PGbox.class);
    addDataType("circle", org.postgresql.geometric.PGcircle.class);
    addDataType("line", org.postgresql.geometric.PGline.class);
    addDataType("lseg", org.postgresql.geometric.PGlseg.class);
    addDataType("path", org.postgresql.geometric.PGpath.class);
    addDataType("point", org.postgresql.geometric.PGpoint.class);
    addDataType("polygon", org.postgresql.geometric.PGpolygon.class);
    addDataType("money", org.postgresql.util.PGmoney.class);
    addDataType("interval", org.postgresql.util.PGInterval.class);

    Enumeration<?> e = info.propertyNames();
    while (e.hasMoreElements()) {
      String propertyName = (String) e.nextElement();
      if (propertyName.startsWith("datatype.")) {
        String typeName = propertyName.substring(9);
        String className = info.getProperty(propertyName);
        Class<?> klass;

        try {
          klass = Class.forName(className);
        } catch (ClassNotFoundException cnfe) {
          throw new PSQLException(
              GT.tr("Unable to load the class {0} responsible for the datatype {1}",
                  className, typeName),
              PSQLState.SYSTEM_ERROR, cnfe);
        }

        addDataType(typeName, klass.asSubclass(PGobject.class));
      }
    }
  }

    @Override
    /**
     * Returns reffrence to the client logic object, if the feature is off null is returned
     */
    public ClientLogic getClientLogic() {
        return clientLogic;
    }

  /**
   * <B>Note:</B> even though {@code Statement} is automatically closed when it is garbage
   * collected, it is better to close it explicitly to lower resource consumption.
   *
   * {@inheritDoc}
   */
  @Override
  public void close() throws SQLException {
    if (clientLogic != null) {
      clientLogic.close();
      clientLogic = null;
    }
    if (queryExecutor == null) {
      // This might happen in case constructor throws an exception (e.g. host being not available).
      // When that happens the connection is still registered in the finalizer queue, so it gets finalized
      return;
    }
    releaseTimer();
    queryExecutor.close();
    openStackTrace = null;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    checkClosed();
    CachedQuery cachedQuery = queryExecutor.createQuery(sql, false, true);

    return cachedQuery.query.getNativeSql();
  }

  @Override
  public synchronized SQLWarning getWarnings() throws SQLException {
    checkClosed();
    SQLWarning newWarnings = queryExecutor.getWarnings(); // NB: also clears them.
    if (firstWarning == null) {
      firstWarning = newWarnings;
    } else {
      firstWarning.setNextWarning(newWarnings); // Chain them on.
    }

    return firstWarning;
  }

  @Override
  public synchronized void clearWarnings() throws SQLException {
    checkClosed();
    queryExecutor.getWarnings(); // Clear and discard.
    firstWarning = null;
  }


  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkClosed();
    if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
      throw new PSQLException(
          GT.tr("Cannot change transaction read-only property in the middle of a transaction."),
          PSQLState.ACTIVE_SQL_TRANSACTION);
    }
    if(allowReadOnly) {
        if (readOnly != this.readOnly) {
            String readOnlySql
                   = "SET SESSION CHARACTERISTICS AS TRANSACTION " + (readOnly ? "READ ONLY" : "READ WRITE");
            execSQLUpdate(readOnlySql); // nb: no BEGIN triggered.
          }
        this.readOnly = readOnly;
        LOGGER.debug("  setReadOnly = " + readOnly);
    }else {
    	LOGGER.debug("Cannot change transaction read-only property when the property allowReadOnly is set to false");
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkClosed();
    return readOnly;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkClosed();

    if (this.autoCommit == autoCommit) {
      return;
    }

    if (!this.autoCommit) {
      commit();
    }

    this.autoCommit = autoCommit;
    LOGGER.debug("  setAutoCommit = " + autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    checkClosed();
    return this.autoCommit;
  }

  private void executeTransactionCommand(Query query) throws SQLException {


    try {
      getQueryExecutor().execute(query, null, new TransactionCommandHandler(), 0, 0, QueryExecutor.QUERY_NO_METADATA | QueryExecutor.QUERY_NO_RESULTS | QueryExecutor.QUERY_SUPPRESS_BEGIN);
    } catch (SQLException e) {
      // Don't retry composite queries as it might get partially executed
      if (query.getSubqueries() != null || !queryExecutor.willHealOnRetry(e)) {
        throw e;
      }
      query.close();
      // retry
      getQueryExecutor().execute(query, null, new TransactionCommandHandler(), 0, 0, QueryExecutor.QUERY_NO_METADATA | QueryExecutor.QUERY_NO_RESULTS | QueryExecutor.QUERY_SUPPRESS_BEGIN);
    }
  }

  @Override
  public void commit() throws SQLException {
    checkClosed();

    if (autoCommit) {
      throw new PSQLException(GT.tr("Cannot commit when autoCommit is enabled."),
          PSQLState.NO_ACTIVE_SQL_TRANSACTION);
    }

    if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
      executeTransactionCommand(commitQuery);
    }
  }

  protected void checkClosed() throws SQLException {
    if (isClosed()) {
      throw new PSQLException(GT.tr("This connection has been closed."),
          PSQLState.CONNECTION_DOES_NOT_EXIST);
    }
  }


  @Override
  public void rollback() throws SQLException {
    checkClosed();

    if (autoCommit) {
      throw new PSQLException(GT.tr("Cannot rollback when autoCommit is enabled."),
          PSQLState.NO_ACTIVE_SQL_TRANSACTION);
    }

    if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
      executeTransactionCommand(rollbackQuery);
    }
  }

  public TransactionState getTransactionState() {
    return queryExecutor.getTransactionState();
  }

  public int getTransactionIsolation() throws SQLException {
    checkClosed();

    String level = null;
    final ResultSet rs = execSQLQuery("SHOW TRANSACTION ISOLATION LEVEL"); // nb: no BEGIN triggered
    if (rs.next()) {
      level = rs.getString(1);
    }
    rs.close();

    // TODO revisit: throw exception instead of silently eating the error in unknown cases?
    if (level == null) {
      return Connection.TRANSACTION_READ_COMMITTED; // Best guess.
    }

    level = level.toUpperCase(Locale.US);
    if (level.equals("READ COMMITTED")) {
      return Connection.TRANSACTION_READ_COMMITTED;
    }
    if (level.equals("READ UNCOMMITTED")) {
      return Connection.TRANSACTION_READ_UNCOMMITTED;
    }
    if (level.equals("REPEATABLE READ")) {
      return Connection.TRANSACTION_REPEATABLE_READ;
    }
    if (level.equals("SERIALIZABLE")) {
      return Connection.TRANSACTION_SERIALIZABLE;
    }

    return Connection.TRANSACTION_READ_COMMITTED; // Best guess.
  }

  public void setTransactionIsolation(int level) throws SQLException {
    checkClosed();

    if (queryExecutor.getTransactionState() != TransactionState.IDLE) {
      throw new PSQLException(
          GT.tr("Cannot change transaction isolation level in the middle of a transaction."),
          PSQLState.ACTIVE_SQL_TRANSACTION);
    }

    String isolationLevelName = getIsolationLevelName(level);
    if (isolationLevelName == null) {
      throw new PSQLException(GT.tr("Transaction isolation level {0} not supported.", level),
          PSQLState.NOT_IMPLEMENTED);
    }

    String isolationLevelSQL =
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + isolationLevelName;
    execSQLUpdate(isolationLevelSQL); // nb: no BEGIN triggered
    LOGGER.debug("  setTransactionIsolation = " + isolationLevelName);
  }

  protected String getIsolationLevelName(int level) {
    switch (level) {
      case Connection.TRANSACTION_READ_COMMITTED:
        return "READ COMMITTED";
      case Connection.TRANSACTION_SERIALIZABLE:
        return "SERIALIZABLE";
      case Connection.TRANSACTION_READ_UNCOMMITTED:
        return "READ UNCOMMITTED";
      case Connection.TRANSACTION_REPEATABLE_READ:
        return "REPEATABLE READ";
      default:
        return null;
    }
  }

  public void setCatalog(String catalog) throws SQLException {
    checkClosed();
    // no-op
  }

  public String getCatalog() throws SQLException {
    checkClosed();
    return queryExecutor.getDatabase();
  }

  /**
   * <p>Overrides finalize(). If called, it closes the connection.</p>
   *
   * <p>This was done at the request of <a href="mailto:rachel@enlarion.demon.co.uk">Rachel
   * Greenham</a> who hit a problem where multiple clients didn't close the connection, and once a
   * fortnight enough clients were open to kill the postgres server.</p>
   */
  protected void finalize() throws Throwable {
      if (openStackTrace != null) {
        LOGGER.warn(GT.tr("Finalizing a Connection that was never closed:"), openStackTrace);
      }

      close();
   
  }

  /**
   * Get server version number.
   *
   * @return server version number
   */
  public String getDBVersionNumber() {
    return queryExecutor.getServerVersion();
  }

  /**
   * Get server major version.
   *
   * @return server major version
   */
  public int getServerMajorVersion() {
    try {
      StringTokenizer versionTokens = new StringTokenizer(queryExecutor.getServerVersion(), "."); // aaXbb.ccYdd
      return integerPart(versionTokens.nextToken()); // return X
    } catch (NoSuchElementException e) {
      return 0;
    }
  }

  /**
   * Get server minor version.
   *
   * @return server minor version
   */
  public int getServerMinorVersion() {
    try {
      StringTokenizer versionTokens = new StringTokenizer(queryExecutor.getServerVersion(), "."); // aaXbb.ccYdd
      versionTokens.nextToken(); // Skip aaXbb
      return integerPart(versionTokens.nextToken()); // return Y
    } catch (NoSuchElementException e) {
      return 0;
    }
  }

  @Override
  public boolean haveMinimumServerVersion(int ver) {
    return queryExecutor.getServerVersionNum() >= ver;
  }

  @Override
  public boolean haveMinimumServerVersion(Version ver) {
    return haveMinimumServerVersion(ver.getVersionNum());
  }

  @Override
  public Encoding getEncoding() {
    return queryExecutor.getEncoding();
  }

  @Override
  public byte[] encodeString(String str) throws SQLException {
    try {
      return getEncoding().encode(str);
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Unable to translate data into the desired encoding."),
          PSQLState.DATA_ERROR, ioe);
    }
  }

  @Override
  public String escapeString(String str) throws SQLException {
    return Utils.escapeLiteral(null, str, queryExecutor.getStandardConformingStrings())
        .toString();
  }

  @Override
  public boolean getStandardConformingStrings() {
    return queryExecutor.getStandardConformingStrings();
  }

  // This is a cache of the DatabaseMetaData instance for this connection
  protected java.sql.DatabaseMetaData metadata;

  @Override
  public boolean isClosed() throws SQLException {
    return queryExecutor.isClosed();
  }

  @Override
  public void cancelQuery() throws SQLException {
    checkClosed();
    try {
      queryExecutor.sendQueryCancel();
    } catch (SQLException e) {
      queryExecutor.close();
    }
  }

  @Override
  public PGNotification[] getNotifications() throws SQLException {
    return getNotifications(-1);
  }

  @Override
  public PGNotification[] getNotifications(int timeoutMillis) throws SQLException {
    checkClosed();
    getQueryExecutor().processNotifies(timeoutMillis);
    // Backwards-compatibility hand-holding.
    PGNotification[] notifications = queryExecutor.getNotifications();
    return (notifications.length == 0 ? null : notifications);
  }

  /**
   * Handler for transaction queries.
   */
  private class TransactionCommandHandler extends ResultHandlerBase {
    public void handleCompletion() throws SQLException {
      SQLWarning warning = getWarning();
      if (warning != null) {
        PgConnection.this.addWarning(warning);
      }
      super.handleCompletion();
    }
  }

  @Override
  public boolean getBitToString() {
      return bitToString;
  }

  public int getPrepareThreshold() {
    return prepareThreshold;
  }

  public void setDefaultFetchSize(int fetchSize) throws SQLException {
    if (fetchSize < 0) {
      throw new PSQLException(GT.tr("Fetch size must be a value greater to or equal to 0."),
          PSQLState.INVALID_PARAMETER_VALUE);
    }

    this.defaultFetchSize = fetchSize;
  }

  public int getDefaultFetchSize() {
    return defaultFetchSize;
  }

  public void setPrepareThreshold(int newThreshold) {
    this.prepareThreshold = newThreshold;
  }

  public boolean getForceBinary() {
    return forcebinary;
  }

  public void setForceBinary(boolean newValue) {
    this.forcebinary = newValue;
  }

  public void setTypeMapImpl(Map<String, Class<?>> map) throws SQLException {
    typemap = map;
  }

  public Log getLogger() {
    return LOGGER;
  }

  public int getProtocolVersion() {
    return queryExecutor.getProtocolVersion();
  }

  public boolean getStringVarcharFlag() {
    return bindStringAsVarchar;
  }

  private CopyManager copyManager = null;

  public CopyManager getCopyAPI() throws SQLException {
    checkClosed();
    if (copyManager == null) {
      copyManager = new CopyManager(this);
    }
    return copyManager;
  }

  public boolean binaryTransferSend(int oid) {
    return queryExecutor.useBinaryForSend(oid);
  }

  public boolean isColumnSanitiserDisabled() {
    return this.disableColumnSanitiser;
  }

  public void setDisableColumnSanitiser(boolean disableColumnSanitiser) {
    this.disableColumnSanitiser = disableColumnSanitiser;
    LOGGER.debug("  setDisableColumnSanitiser = " + disableColumnSanitiser);
  }

  @Override
  public PreferQueryMode getPreferQueryMode() {
    return queryExecutor.getPreferQueryMode();
  }

  @Override
  public AutoSave getAutosave() {
    return queryExecutor.getAutoSave();
  }

  @Override
  public void setAutosave(AutoSave autoSave) {
    queryExecutor.setAutoSave(autoSave);
    LOGGER.debug("  setAutosave = " + autoSave.value());
  }

  protected void abort() {
    queryExecutor.abort();
  }

  private synchronized Timer getTimer() {
    if (cancelTimer == null) {
      cancelTimer = Driver.getSharedTimer().getTimer();
    }
    return cancelTimer;
  }

  private synchronized void releaseTimer() {
    if (cancelTimer != null) {
      cancelTimer = null;
      Driver.getSharedTimer().releaseTimer();
    }
  }

  @Override
  public void addTimerTask(TimerTask timerTask, long milliSeconds) {
    Timer timer = getTimer();
    timer.schedule(timerTask, milliSeconds);
  }

  @Override
  public void purgeTimerTasks() {
    Timer timer = cancelTimer;
    if (timer != null) {
      timer.purge();
    }
  }

  @Override
  public String escapeIdentifier(String identifier) throws SQLException {
    return Utils.escapeIdentifier(null, identifier).toString();
  }

  @Override
  public String escapeLiteral(String literal) throws SQLException {
    return Utils.escapeLiteral(null, literal, queryExecutor.getStandardConformingStrings())
        .toString();
  }

  @Override
  public LruCache<FieldMetadata.Key, FieldMetadata> getFieldMetadataCache() {
    return fieldMetadataCache;
  }

  @Override
  public PGReplicationConnection getReplicationAPI() {
    return new PGReplicationConnectionImpl(this);
  }

  private static void appendArray(StringBuilder sb, Object elements, char delim) {
    sb.append('{');

    int nElements = java.lang.reflect.Array.getLength(elements);
    for (int i = 0; i < nElements; i++) {
      if (i > 0) {
        sb.append(delim);
      }

      Object o = java.lang.reflect.Array.get(elements, i);
      if (o == null) {
        sb.append("NULL");
      } else if (o.getClass().isArray()) {
        final PrimitiveArraySupport arraySupport = PrimitiveArraySupport.getArraySupport(o);
        if (arraySupport != null) {
          arraySupport.appendArray(sb, delim, o);
        } else {
          appendArray(sb, o, delim);
        }
      } else {
        String s = o.toString();
        PgArray.escapeArrayElement(sb, s);
      }
    }
    sb.append('}');
  }

  // Parse a "dirty" integer surrounded by non-numeric characters
  private static int integerPart(String dirtyString) {
    int start;
    int end;

    for (start = 0; start < dirtyString.length()
        && !Character.isDigit(dirtyString.charAt(start)); ++start) {
      ;
    }

    for (end = start; end < dirtyString.length()
        && Character.isDigit(dirtyString.charAt(end)); ++end) {
      ;
    }

    if (start == end) {
      return 0;
    }

    return Integer.parseInt(dirtyString.substring(start, end));
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    checkClosed();
    return new PgStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    checkClosed();
    return new PgPreparedStatement(this, sql, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    checkClosed();
    return new PgCallableStatement(this, sql, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    checkClosed();
    if (metadata == null) {
      metadata = new PgDatabaseMetaData(this);
    }
    return metadata;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    setTypeMapImpl(map);
    LOGGER.debug("  setTypeMap = " + map);
  }

  protected Array makeArray(int oid, String fieldString) throws SQLException {
    return new PgArray(this, oid, fieldString);
  }

  protected Blob makeBlob(long oid) throws SQLException {
    return new PgBlob(this, oid);
  }

  protected Clob makeClob(long oid) throws SQLException {
    return new PgClob(this, oid);
  }

  protected SQLXML makeSQLXML() throws SQLException {
    return new PgSQLXML(this);
  }

  @Override
  public Clob createClob() throws SQLException {
    checkClosed();
    return new PGClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    checkClosed();
    return new PGBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    checkClosed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "createNClob()");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    checkClosed();
    return makeSQLXML();
  }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        if (attributes == null) {
            return null;
        }
        final TypeInfo typeInfo = getTypeInfo();
        // Query the oid of this type based on typeName
        final int oid = typeInfo.getPGType(typeName);
        if (oid == Oid.UNSPECIFIED) {
            throw new PSQLException(GT.tr("Unable to find server struct type for provided name {0}.", typeName),
                    PSQLState.INVALID_NAME);
        }
        // Create a PGStruct object
        return new PGStruct(this, oid, attributes);
    }

  @Override
  public Array createArrayOf(String typeName, Object elements) throws SQLException {
    checkClosed();

    final TypeInfo typeInfo = getTypeInfo();

    final int oid = typeInfo.getPGArrayType(typeName);
    final char delim = typeInfo.getArrayDelimiter(oid);

    if (oid == Oid.UNSPECIFIED) {
      throw new PSQLException(GT.tr("Unable to find server array type for provided name {0}.", typeName),
          PSQLState.INVALID_NAME);
    }

    if (elements == null) {
      return makeArray(oid, null);
    }

    final String arrayString;

    final PrimitiveArraySupport arraySupport = PrimitiveArraySupport.getArraySupport(elements);

    if (arraySupport != null) {
      // if the oid for the given type matches the default type, we might be
      // able to go straight to binary representation
      if (oid == arraySupport.getDefaultArrayTypeOid(typeInfo) && arraySupport.supportBinaryRepresentation()
          && getPreferQueryMode() != PreferQueryMode.SIMPLE) {
        return new PgArray(this, oid, arraySupport.toBinaryRepresentation(this, elements));
      }
      arrayString = arraySupport.toArrayString(delim, elements);
    } else {
      final Class<?> clazz = elements.getClass();
      if (!clazz.isArray()) {
        throw new PSQLException(GT.tr("Invalid elements {0}", elements), PSQLState.INVALID_PARAMETER_TYPE);
      }
      StringBuilder sb = new StringBuilder();
      appendArray(sb, elements, delim);
      arrayString = sb.toString();
    }

    return makeArray(oid, arrayString);
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    checkClosed();

    int oid = getTypeInfo().getPGArrayType(typeName);

    if (oid == Oid.UNSPECIFIED) {
      throw new PSQLException(
          GT.tr("Unable to find server array type for provided name {0}.", typeName),
          PSQLState.INVALID_NAME);
    }

    if (elements == null) {
      return makeArray(oid, null);
    }

    char delim = getTypeInfo().getArrayDelimiter(oid);
    StringBuilder sb = new StringBuilder();
    appendArray(sb, elements, delim);

    return makeArray(oid, sb.toString());
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new PSQLException(GT.tr("Invalid timeout ({0}<0).", timeout),
          PSQLState.INVALID_PARAMETER_VALUE);
    }
    if (isClosed()) {
      return false;
    }
    try {
      if (replicationConnection) {
        Statement statement = createStatement();
        statement.execute("IDENTIFY_SYSTEM");
        statement.close();
      } else {
        if (checkConnectionQuery == null) {
          checkConnectionQuery = prepareStatement("");
        }
        checkConnectionQuery.setQueryTimeout(timeout);
        checkConnectionQuery.executeUpdate();
      }
      return true;
    } catch (SQLException e) {
      if (PSQLState.IN_FAILED_SQL_TRANSACTION.getState().equals(e.getSQLState())) {
        // "current transaction aborted", assume the connection is up and running
        return true;
      }
      LOGGER.debug(GT.tr("Validating connection."), e);
    }
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      checkClosed();
    } catch (final SQLException cause) {
      Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
      failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
      throw new SQLClientInfoException(GT.tr("This connection has been closed."), failures, cause);
    }

    if (haveMinimumServerVersion(ServerVersion.v9_0) && "ApplicationName".equals(name) || "ApplicationType".equals(name)) {
      Map<String,String>appInfo=new HashMap<String,String>();
      appInfo.put("ApplicationName","application_name");
      appInfo.put("ApplicationType","application_type");
      if (value == null) {
        value = "";
      }

      final String oldValue = "ApplicationName".equals(name) ? queryExecutor.getApplicationName(): queryExecutor.getApplicationType();
      if (value.equals(oldValue)) {
        return;
      }

      try {
        StringBuilder sql = new StringBuilder(String.format("SET %s = '", appInfo.get(name)));  
        Utils.escapeLiteral(sql, value, getStandardConformingStrings());
        sql.append("'");
        execSQLUpdate(sql.toString());
      } catch (SQLException sqle) {
        Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
        failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
        throw new SQLClientInfoException(
            GT.tr("Failed to set ClientInfo property: " + name), sqle.getSQLState(),
            failures, sqle);
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("  setClientInfo = " + name + " " + value);
      }
      _clientInfo.put(name, value);
      return;
    }

    addWarning(new SQLWarning(GT.tr("ClientInfo property not supported."),
        PSQLState.NOT_IMPLEMENTED.getState()));
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      checkClosed();
    } catch (final SQLException cause) {
      Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
      for (Map.Entry<Object, Object> e : properties.entrySet()) {
        failures.put((String) e.getKey(), ClientInfoStatus.REASON_UNKNOWN);
      }
      throw new SQLClientInfoException(GT.tr("This connection has been closed."), failures, cause);
    }

      String gaussdbVersion = queryExecutor.getGaussdbVersion();
      String[] result;
      if (gaussdbVersion.equals("GaussDBKernel")) {
          result = new String[]{"ApplicationName", "ApplicationType"};
      } else {
          result = new String[]{"ApplicationName"};
      }

    Map<String, ClientInfoStatus> failures = new HashMap<String, ClientInfoStatus>();
    for (String name : result) {
      try {
        if("ApplicationType".equals(name) && properties.getProperty(name) == null) {
            continue;
        }
        setClientInfo(name, properties.getProperty(name, null));
      } catch (SQLClientInfoException e) {
        failures.putAll(e.getFailedProperties());
      }
    }

    if (!failures.isEmpty()) {
      throw new SQLClientInfoException(GT.tr("One or more ClientInfo failed."),
          PSQLState.NOT_IMPLEMENTED.getState(), failures);
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    checkClosed();
    _clientInfo.put("ApplicationName", queryExecutor.getApplicationName());
    _clientInfo.put("ApplicationType", queryExecutor.getApplicationType());  //revise
    return _clientInfo.getProperty(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkClosed();
    _clientInfo.put("ApplicationName", queryExecutor.getApplicationName());
    _clientInfo.put("ApplicationType", queryExecutor.getApplicationType());
    return _clientInfo;
  }

  public <T> T createQueryObject(Class<T> ifc) throws SQLException {
    checkClosed();
    throw org.postgresql.Driver.notImplemented(this.getClass(), "createQueryObject(Class<T>)");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    checkClosed();
    return iface.isAssignableFrom(getClass());
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    checkClosed();
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw org.postgresql.Driver.notImplemented(this.getClass(), "unwrap(Class<T>)");
  }

  public String getSchema() throws SQLException {
    checkClosed();
    Statement stmt = createStatement();
    try {
      ResultSet rs = stmt.executeQuery("select current_schema()");
      try {
        if (!rs.next()) {
          return null; // Is it ever possible?
        }
        return rs.getString(1);
      } finally {
        rs.close();
      }
    } finally {
      stmt.close();
    }
  }

  public void setSchema(String schema) throws SQLException {
    checkClosed();
    Statement stmt = createStatement();
    try {
      if (schema == null) {
        stmt.executeUpdate("SET SESSION search_path TO DEFAULT");
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append("SET SESSION search_path TO '");
        Utils.escapeLiteral(sb, schema, getStandardConformingStrings());
        sb.append("'");
        stmt.executeUpdate(sb.toString());
        LOGGER.debug("  setSchema = " + schema);
      }
    } finally {
      stmt.close();
    }
  }

  public class AbortCommand implements Runnable {
    public void run() {
      abort();
    }
  }

  public void abort(Executor executor) throws SQLException {
    if (isClosed()) {
      return;
    }

    SQL_PERMISSION_ABORT.checkGuard(this);

    AbortCommand command = new AbortCommand();
    if (executor != null) {
      executor.execute(command);
    } else {
      command.run();
    }
  }

  public void setNetworkTimeout(Executor executor /*not used*/, int milliseconds) throws SQLException {
    checkClosed();

    if (milliseconds < 0) {
      throw new PSQLException(GT.tr("Network timeout must be a value greater than or equal to 0."),
              PSQLState.INVALID_PARAMETER_VALUE);
    }

    SecurityManager securityManager = System.getSecurityManager();
    if (securityManager != null) {
      securityManager.checkPermission(SQL_PERMISSION_NETWORK_TIMEOUT);
    }

    try {
      queryExecutor.setNetworkTimeout(milliseconds);
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Unable to set network timeout."),
              PSQLState.COMMUNICATION_ERROR, ioe);
    }
  }

  public int getNetworkTimeout() throws SQLException {
    checkClosed();

    try {
      return queryExecutor.getNetworkTimeout();
    } catch (IOException ioe) {
      throw new PSQLException(GT.tr("Unable to get network timeout."),
              PSQLState.COMMUNICATION_ERROR, ioe);
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    checkClosed();

    switch (holdability) {
      case ResultSet.CLOSE_CURSORS_AT_COMMIT:
        rsHoldability = holdability;
        break;
      case ResultSet.HOLD_CURSORS_OVER_COMMIT:
        rsHoldability = holdability;
        break;
      default:
        throw new PSQLException(GT.tr("Unknown ResultSet holdability setting: {0}.", holdability),
            PSQLState.INVALID_PARAMETER_VALUE);
    }
    LOGGER.debug("  setHoldability = " + holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    checkClosed();
    return rsHoldability;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    checkClosed();

    String pgName;
    if (getAutoCommit()) {
      throw new PSQLException(GT.tr("Cannot establish a savepoint in auto-commit mode."),
          PSQLState.NO_ACTIVE_SQL_TRANSACTION);
    }

    PSQLSavepoint savepoint = new PSQLSavepoint(savepointId++);
    pgName = savepoint.getPGName();

    // Note we can't use execSQLUpdate because we don't want
    // to suppress BEGIN.
    Statement stmt = createStatement();
    stmt.executeUpdate("SAVEPOINT " + pgName);
    stmt.close();

    return savepoint;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    checkClosed();

    if (getAutoCommit()) {
      throw new PSQLException(GT.tr("Cannot establish a savepoint in auto-commit mode."),
          PSQLState.NO_ACTIVE_SQL_TRANSACTION);
    }

    PSQLSavepoint savepoint = new PSQLSavepoint(name);

    // Note we can't use execSQLUpdate because we don't want
    // to suppress BEGIN.
    Statement stmt = createStatement();
    stmt.executeUpdate("SAVEPOINT " + savepoint.getPGName());
    stmt.close();

    return savepoint;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    checkClosed();

    PSQLSavepoint pgSavepoint = (PSQLSavepoint) savepoint;
    execSQLUpdate("ROLLBACK TO SAVEPOINT " + pgSavepoint.getPGName());
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    checkClosed();

    PSQLSavepoint pgSavepoint = (PSQLSavepoint) savepoint;
    execSQLUpdate("RELEASE SAVEPOINT " + pgSavepoint.getPGName());
    pgSavepoint.invalidate();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();
    return createStatement(resultSetType, resultSetConcurrency, getHoldability());
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();
    return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkClosed();
    return prepareCall(sql, resultSetType, resultSetConcurrency, getHoldability());
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS) {
      return prepareStatement(sql);
    }

    return prepareStatement(sql, (String[]) null);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    if (columnIndexes != null && columnIndexes.length == 0) {
      return prepareStatement(sql);
    }

    checkClosed();
    throw new PSQLException(GT.tr("Returning autogenerated keys is not supported."),
        PSQLState.NOT_IMPLEMENTED);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    if (columnNames != null && columnNames.length == 0) {
      return prepareStatement(sql);
    }

    CachedQuery cachedQuery = borrowReturningQuery(sql, columnNames);
    PgPreparedStatement ps =
        new PgPreparedStatement(this, cachedQuery,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            getHoldability());
    Query query = cachedQuery.query;
    SqlCommand sqlCommand = query.getSqlCommand();
    if (sqlCommand != null) {
      ps.wantsGeneratedKeysAlways = sqlCommand.isReturningKeywordPresent();
    } else {
      // If composite query is given, just ignore "generated keys" arguments
    }
    return ps;
  }

    @Override
    public PGXmlFactoryFactory getXmlFactoryFactory() throws SQLException {
        if (xmlFactoryFactory == null) {
            if (xmlFactoryFactoryClass == null || xmlFactoryFactoryClass.equals("")) {
                xmlFactoryFactory = DefaultPGXmlFactoryFactory.INSTANCE;
            } else if (xmlFactoryFactoryClass.equals("LEGACY_INSECURE")) {
                xmlFactoryFactory = LegacyInsecurePGXmlFactoryFactory.INSTANCE;
            } else {
                Class<?> clazz;
                try {
                    clazz = Class.forName(xmlFactoryFactoryClass);
                } catch (ClassNotFoundException ex) {
                    throw new PSQLException(
                            GT.tr("Could not instantiate xmlFactoryFactory: {0}", xmlFactoryFactoryClass),
                            PSQLState.INVALID_PARAMETER_VALUE, ex);
                }
                if (!clazz.isAssignableFrom(PGXmlFactoryFactory.class)) {
                    throw new PSQLException(
                            GT.tr("Connection property xmlFactoryFactory must implement PGXmlFactoryFactory: {0}", xmlFactoryFactoryClass),
                            PSQLState.INVALID_PARAMETER_VALUE);
                }
                try {
                    xmlFactoryFactory = (PGXmlFactoryFactory) clazz.newInstance();
                } catch (Exception ex) {
                    throw new PSQLException(
                            GT.tr("Could not instantiate xmlFactoryFactory: {0}", xmlFactoryFactoryClass),
                            PSQLState.INVALID_PARAMETER_VALUE, ex);
                }
            }
        }
        return xmlFactoryFactory;
    }


  
    public boolean isBatchInsert() {
        return batchInsert;
    }

    public boolean isBlobMode() {
        return blobmode;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public String getSocketAddress() {
        return this.socketAddress;
    }

    public String getSecSocketAddress() {
        return this.secSocketAddress;
    }

    public AtomicReferenceFieldUpdater<PgStatement, TimerTask> getTimerUpdater() {
        return CANCEL_TIMER_UPDATER;
    }

    @Override
    public boolean IsBatchInsert() {
        return this.batchInsert;
    }

    public boolean isAdaptiveSetSQLType() {
        return this.adaptiveSetSQLType;
    }

    public boolean isDolphinCmpt() {
        return isDolphinCmpt;
    }

    private void updateDolphinCmpt(boolean isDolphinCmpt) throws SQLException {
        /* set parameter cannot use prepareStatement to set the value */
        try (Statement stmt = createStatement()) {
            String sql = "set dolphin.b_compatibility_mode to " + (isDolphinCmpt ? "on" : "off");
            stmt.execute(sql);
        } catch (SQLException e) {
            throw e;
        }
    }
    public void setDolphinCmpt(boolean isDolphinCmpt) throws SQLException {
        checkClosed();
        updateDolphinCmpt(isDolphinCmpt);
        this.isDolphinCmpt = isDolphinCmpt;
    }
}
