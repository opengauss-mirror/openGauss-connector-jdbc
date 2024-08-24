/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.core.Oid;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.types.PGBlob;
import org.postgresql.core.types.PGClob;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class PgCallableStatement extends PgPreparedStatement implements CallableStatement {
  // Used by the callablestatement style methods
  private boolean isFunction;
  // functionReturnType contains the user supplied value to check
  // testReturn contains a modified version to make it easier to
  // check the getXXX methods..
  private int[] functionReturnType;
  private int[] testReturn;
  // returnTypeSet is true when a proper call to registerOutParameter has been made
  private boolean returnTypeSet;
  protected Object[] callResult;
  private int lastIndex = 0;
  private String compatibilityMode;
  private boolean isACompatibilityFunction;
  private boolean enableOutparamOveride;
  // whether the current executed SQL statement contains a custom type
  private boolean isContainCompositeType = false;
  // cache custom types that have been queried
  private ConcurrentHashMap<String, List<Object[]>> compositeTypeMap = new ConcurrentHashMap<>();

  private int[] returnOids;

  PgCallableStatement(PgConnection connection, String sql, int rsType, int rsConcurrency,
      int rsHoldability) throws SQLException {
    super(connection, connection.borrowCallableQuery(sql), rsType, rsConcurrency, rsHoldability);
    this.isFunction = preparedQuery.isFunction;
    this.isACompatibilityFunction = preparedQuery.isACompatibilityFunction;
    this.compatibilityMode = connection.getQueryExecutor().getCompatibilityMode();
    this.enableOutparamOveride = connection.getQueryExecutor().getEnableOutparamOveride();
    if (this.isFunction) {
      int inParamCount = this.preparedParameters.getInParameterCount() + 1;
      this.testReturn = new int[inParamCount];
      this.functionReturnType = new int[inParamCount];
      this.returnOids = new int[inParamCount];
    }
  }

  public int executeUpdate() throws SQLException {
    if (isFunction) {
      executeWithFlags(0);
      return 0;
    }
    return super.executeUpdate();
  }

  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(i, map);
  }

  public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
    return getObjectImpl(s, map);
  }

  @Override
  public boolean executeWithFlags(int flags) throws SQLException {
    boolean hasResultSet = super.executeWithFlags(flags);
    if (!isFunction || !returnTypeSet) {
      return hasResultSet;
    }

    // If we are executing and there are out parameters
    // callable statement function set the return data
    if (!hasResultSet) {
      throw new PSQLException(GT.tr("A CallableStatement was executed with nothing returned."),
              PSQLState.NO_DATA);
    }

    ResultSet rs;
    synchronized (this) {
      checkClosed();
      rs = result.getResultSet();
    }
    if (!rs.next()) {
      throw new PSQLException(GT.tr("A CallableStatement was executed with nothing returned."),
              PSQLState.NO_DATA);
    }

    // figure out how many columns
    int cols = rs.getMetaData().getColumnCount();

    int outParameterCount = preparedParameters.getOutParameterCount();

    // reset last result fetched (for wasNull)
    lastIndex = 0;

    // allocate enough space for all possible parameters without regard to in/out
    callResult = new Object[preparedParameters.getParameterCount() + 1];

    if (isContainCompositeType && outParameterCount == 1) {
      for (int i = 0, j = 0; i < cols; i++, j++) {
        while (j < functionReturnType.length && functionReturnType[j] == 0) {
          j++;
        }
        Object[] compositeTypeStruct = connection.getTypeInfo().getStructAttributesName(returnOids[j]);
        if (compositeTypeStruct == null) {
          throw new PSQLException(GT.tr("Unknown composite type."), PSQLState.UNKNOWN_STATE);
        }
        int compositeTypeLength = compositeTypeStruct.length;
        StringBuffer sb = new StringBuffer();
        sb.append("(");
        for (int k = 0; k < compositeTypeLength; k++, i++) {
          String str = rs.getString(i + 1);
          if (str != null) {
            if (isContainSpecialChar(str)) {
              // escape double quotes.
              if (str.contains(Character.toString('"'))) {
                str = str.replaceAll("\"", "\"\"");
              }
              // escape backslashes.
              if (str.contains(Character.toString('\\'))) {
                str = str.replaceAll("\\\\", "\\\\\\\\");
              }
              sb.append("\"" + str + "\"" + ",");
            } else {
              sb.append(str + ",");
            }
          } else {
            sb.append(",");
          }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        PGStruct pgStruct = new PGStruct(connection, 0, sb.toString());
        pgStruct.setAttrsSqlTypeList(connection.getTypeInfo().getStructAttributesOid(returnOids[j]));
        pgStruct.setStruct(compositeTypeStruct);
        callResult[j] = pgStruct;
      }
    } else {
      if (cols != outParameterCount) {
        throw new PSQLException(
                GT.tr("A CallableStatement was executed with an invalid number of parameters"),
                PSQLState.SYNTAX_ERROR);
      }
      // move them into the result set
      for (int i = 0, j = 0; i < cols; i++, j++) {
        // find the next out parameter, the assumption is that the functionReturnType
        // array will be initialized with 0 and only out parameters will have values
        // other than 0. 0 is the value for java.sql.Types.NULL, which should not
        // conflict
        while (j < functionReturnType.length && functionReturnType[j] == 0) {
          j++;
        }
        int columnType = rs.getMetaData().getColumnType(i + 1);

        if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
            callResult[j] = rs.getBigDecimal(i + 1);
        } else {
            callResult[j] = rs.getObject(i + 1);
        }
        if (columnType != functionReturnType[j]) {
          // this is here for the sole purpose of passing the cts
          PgCallstatementTypeCompatibility typeCompatibility = new PgCallstatementTypeCompatibility(
                  columnType, functionReturnType[j]);
          if (typeCompatibility.isCompatibilityType()) {
            if (callResult[j] != null && typeCompatibility.needConvert()) {
              callResult[j] = typeCompatibility.convert(callResult[j]);
            }
          } else {
            throw new PSQLException(GT.tr(
                    "A CallableStatement function was executed and the out parameter {0} was of type {1} however type" +
                            " {2} was registered.",
                    i + 1, "java.sql.Types=" + columnType, "java.sql.Types=" + functionReturnType[j]),
                    PSQLState.DATA_TYPE_MISMATCH);
          }
        }
      }
    }
    rs.close();
    synchronized (this) {
      result = null;
    }
    return false;
  }

  /**
   * Whether it contains special characters
   *
   * @param str string used for judgment
   * @return contains or does not contain
   */
  private boolean isContainSpecialChar(String str) {
    return str.contains(Character.toString('"')) || str.contains(Character.toString('\\')) ||
            str.contains(Character.toString('(')) || str.contains(Character.toString(')')) ||
            str.contains(Character.toString(',')) || str.contains(Character.toString(' '));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Before executing a stored procedure call you must explicitly call registerOutParameter to
   * register the java.sql.Type of each out parameter.</p>
   *
   * <p>Note: When reading the value of an out parameter, you must use the getXXX method whose Java
   * type XXX corresponds to the parameter's registered SQL type.</p>
   *
   * <p>ONLY 1 RETURN PARAMETER if {?= call ..} syntax is used</p>
   *
   * @param parameterIndex the first parameter is 1, the second is 2,...
   * @param sqlType SQL type code defined by java.sql.Types; for parameters of type Numeric or
   *        Decimal use the version of registerOutParameter that accepts a scale value
   * @throws SQLException if a database-access error occurs.
   */
  @Override
  public void registerOutParameter(int parameterIndex, int sqlType)
      throws SQLException {
    checkClosed();
    switch (sqlType) {
      case Types.TINYINT:
        // we don't have a TINYINT type use SMALLINT
        sqlType = Types.TINYINT;
        break;
      case Types.LONGVARCHAR:
        sqlType = Types.VARCHAR;
        break;
      case Types.NUMERIC:
        sqlType = Types.DECIMAL;
        break;
      case Types.FLOAT:
        // float is the same as double
        sqlType = Types.DOUBLE;
        break;
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        sqlType = Types.BINARY;
        break;
      case Types.BOOLEAN:
        sqlType = Types.BIT;
        break;
      case Types.NCHAR:
        sqlType = Types.CHAR;
        break;
      case -10:
        sqlType = Types.REF_CURSOR;
        break;
      default:
        break;
    }
    if (!isFunction) {
      throw new PSQLException(
          GT.tr(
              "This statement does not declare an OUT parameter.  Use '{' ?= call ... '}' to declare one."),
          PSQLState.STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL);
    }
    checkIndex(parameterIndex, false);

    preparedParameters.registerOutParameter(parameterIndex, sqlType);

    // determine whether to overwrite the original VOID with the oid of the out parameter according to the
    // compatibility mode and the state of the guc parameter value
    if (isACompatibilityAndOverLoad()) {
      Integer oid;
      if (sqlTypeToOid.get(sqlType) == null) {
        oid = Integer.valueOf(0);
      } else {
        oid = sqlTypeToOid.get(sqlType);
      }
      preparedParameters.bindRegisterOutParameter(parameterIndex, oid, isACompatibilityFunction);
    }

    // functionReturnType contains the user supplied value to check
    // testReturn contains a modified version to make it easier to
    // check the getXXX methods..
    functionReturnType[parameterIndex - 1] = sqlType;
    testReturn[parameterIndex - 1] = sqlType;

    if (functionReturnType[parameterIndex - 1] == Types.CHAR
        || functionReturnType[parameterIndex - 1] == Types.LONGVARCHAR) {
      testReturn[parameterIndex - 1] = Types.VARCHAR;
    } else if (functionReturnType[parameterIndex - 1] == Types.FLOAT) {
      testReturn[parameterIndex - 1] = Types.REAL; // changes to streamline later error checking
    }
    returnTypeSet = true;
  }

  private static HashMap<Integer, Integer> sqlTypeToOid = new HashMap<>();
  static {
    sqlTypeToOid.put(Types.SQLXML, Oid.XML);
    sqlTypeToOid.put(Types.INTEGER, Oid.INT4);
    sqlTypeToOid.put(Types.TINYINT, Oid.INT1);
    sqlTypeToOid.put(Types.SMALLINT, Oid.INT2);
    sqlTypeToOid.put(Types.BIGINT, Oid.INT8);
    sqlTypeToOid.put(Types.REAL, Oid.FLOAT4);
    sqlTypeToOid.put(Types.VARCHAR, Oid.VARCHAR);
    sqlTypeToOid.put(Types.DOUBLE, Oid.FLOAT8);
    sqlTypeToOid.put(Types.FLOAT, Oid.FLOAT8);
    sqlTypeToOid.put(Types.DECIMAL, Oid.NUMERIC);
    sqlTypeToOid.put(Types.NUMERIC, Oid.NUMERIC);
    sqlTypeToOid.put(Types.CHAR, Oid.BPCHAR);
    sqlTypeToOid.put(Types.DATE, Oid.DATE);
    sqlTypeToOid.put(Types.TIME, Oid.TIME);
    sqlTypeToOid.put(Types.TIMESTAMP, Oid.TIMESTAMP);
    sqlTypeToOid.put(Types.TIME_WITH_TIMEZONE, Oid.UNSPECIFIED);
    sqlTypeToOid.put(Types.TIMESTAMP_WITH_TIMEZONE, Oid.UNSPECIFIED);
    sqlTypeToOid.put(Types.BOOLEAN, Oid.BOOL);
    sqlTypeToOid.put(Types.BIT, Oid.BOOL);
    sqlTypeToOid.put(Types.BINARY, Oid.BYTEA);
    sqlTypeToOid.put(Types.VARBINARY, Oid.BYTEA);
    sqlTypeToOid.put(Types.LONGVARBINARY, Oid.BYTEA);
    sqlTypeToOid.put(Types.BLOB, Oid.BLOB);
    sqlTypeToOid.put(Types.CLOB, Oid.CLOB);
    sqlTypeToOid.put(Types.ARRAY, Oid.VARCHAR_ARRAY);
    sqlTypeToOid.put(Types.DISTINCT, Oid.UNSPECIFIED);
    sqlTypeToOid.put(Types.STRUCT, Oid.UNSPECIFIED);
    sqlTypeToOid.put(Types.NULL, Oid.UNSPECIFIED);
    sqlTypeToOid.put(Types.OTHER, Oid.UNSPECIFIED);
    sqlTypeToOid.put(Types.LONGVARCHAR, Oid.VARCHAR);
    sqlTypeToOid.put(Types.NVARCHAR, Oid.VARCHAR);
    sqlTypeToOid.put(Types.LONGNVARCHAR, Oid.VARCHAR);
    sqlTypeToOid.put(Types.NCHAR, Oid.CHAR);
    sqlTypeToOid.put(Types.REF_CURSOR, Oid.REF_CURSOR);
    
  }

  public boolean wasNull() throws SQLException {
    if (lastIndex == 0) {
      throw new PSQLException(GT.tr("wasNull cannot be call before fetching a result."),
          PSQLState.OBJECT_NOT_IN_STATE);
    }

    // check to see if the last access threw an exception
    return (callResult[lastIndex - 1] == null);
  }

  public String getString(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.VARCHAR, "String");
    try {
    	return (String) callResult[parameterIndex - 1];
    }catch(Exception e) {
    	return callResult[parameterIndex-1].toString();
    }
  }

  /*
   * Get the value of a CHAR, VARCHAR, or LONGVARCHAR parameter as a
   * Java String.
   *
   * @param parameterIndex the first parameter is 1, the second is 2,...
   * @return the parameter value; if the value is SQL NULL, the result is null
   * @exception SQLException if a database-access error occurs.
   */
  public ResultSet getCursor(int parameterIndex) throws SQLException
  {
      checkClosed();
      checkIndex (parameterIndex, Types.OTHER, "cursor");
      return (ResultSet)callResult[parameterIndex-1];
  }

  
  public boolean getBoolean(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.BIT, "Boolean");
    if (callResult[parameterIndex - 1] == null) {
      return false;
    }

    return (Boolean) callResult[parameterIndex - 1];
  }

  public byte getByte(int parameterIndex) throws SQLException {
    checkClosed();
    // fake tiny int with smallint
    checkIndex(parameterIndex, Types.TINYINT, "Byte");

    if (callResult[parameterIndex - 1] == null) {
      return 0;
    }

    return ((Integer) callResult[parameterIndex - 1]).byteValue();

  }

  public short getShort(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.SMALLINT, "Short");
    if (callResult[parameterIndex - 1] == null) {
      return 0;
    }
    return ((Integer) callResult[parameterIndex - 1]).shortValue();
  }

  public int getInt(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.INTEGER, "Int");
    if (callResult[parameterIndex - 1] == null) {
      return 0;
    }

    return (Integer) callResult[parameterIndex - 1];
  }

  public long getLong(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.BIGINT, "Long");
    if (callResult[parameterIndex - 1] == null) {
      return 0;
    }

    return (Long) callResult[parameterIndex - 1];
  }

  public float getFloat(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.REAL, "Float");
    if (callResult[parameterIndex - 1] == null) {
      return 0;
    }

    return (Float) callResult[parameterIndex - 1];
  }

  public double getDouble(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.DOUBLE, "Double");
    if (callResult[parameterIndex - 1] == null) {
      return 0;
    }

    return (Double) callResult[parameterIndex - 1];
  }

  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.NUMERIC, "BigDecimal");
    return ((BigDecimal) callResult[parameterIndex - 1]);
  }

  public byte[] getBytes(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.VARBINARY, Types.BINARY, "Bytes");
    return ((byte[]) callResult[parameterIndex - 1]);
  }

  public java.sql.Date getDate(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.DATE, "Date");
    return (java.sql.Date) callResult[parameterIndex - 1];
  }

  public java.sql.Time getTime(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.TIME, "Time");
    return (java.sql.Time) callResult[parameterIndex - 1];
  }

  public java.sql.Timestamp getTimestamp(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.TIMESTAMP, "Timestamp");
    return (java.sql.Timestamp) callResult[parameterIndex - 1];
  }

  public Object getObject(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex);
    return callResult[parameterIndex - 1];
  }

  /**
   * helperfunction for the getXXX calls to check isFunction and index == 1 Compare BOTH type fields
   * against the return type.
   *
   * @param parameterIndex parameter index (1-based)
   * @param type1 type 1
   * @param type2 type 2
   * @param getName getter name
   * @throws SQLException if something goes wrong
   */
  protected void checkIndex(int parameterIndex, int type1, int type2, String getName)
      throws SQLException {
    checkIndex(parameterIndex);
    if (type1 != this.testReturn[parameterIndex - 1]
        && type2 != this.testReturn[parameterIndex - 1]) {
      throw new PSQLException(
          GT.tr("Parameter of type {0} was registered, but call to get{1} (sqltype={2}) was made.",
                  "java.sql.Types=" + testReturn[parameterIndex - 1], getName,
                  "java.sql.Types=" + type1),
          PSQLState.MOST_SPECIFIC_TYPE_DOES_NOT_MATCH);
    }
  }

  /**
   * Helper function for the getXXX calls to check isFunction and index == 1.
   *
   * @param parameterIndex parameter index (1-based)
   * @param type type
   * @param getName getter name
   * @throws SQLException if given index is not valid
   */
  protected void checkIndex(int parameterIndex, int type, String getName) throws SQLException {
    checkIndex(parameterIndex);
    if (type != this.testReturn[parameterIndex - 1]) {
    	if(type == Types.VARCHAR && this.testReturn[parameterIndex-1] == Types.CLOB){
    		return;
    	}else if(type == Types.OTHER && this.testReturn[parameterIndex-1] == -10){
    		return;
    	}else if(type == Types.VARCHAR && this.testReturn[parameterIndex-1] == Types.INTEGER){
    		return;
    	}else if(type == Types.VARCHAR && this.testReturn[parameterIndex-1] == Types.NUMERIC){
    		return;
    	}else if(type == Types.INTEGER && this.testReturn[parameterIndex -1] == Types.NUMERIC){
    		return;
    	}else if(type == Types.NUMERIC && this.testReturn[parameterIndex -1] == Types.INTEGER){
    		return;
    	}
    	
      throw new PSQLException(
          GT.tr("Parameter of type {0} was registered, but call to get{1} (sqltype={2}) was made.",
              "java.sql.Types=" + testReturn[parameterIndex - 1], getName,
                  "java.sql.Types=" + type),
          PSQLState.MOST_SPECIFIC_TYPE_DOES_NOT_MATCH);
    }
  }

  private void checkIndex(int parameterIndex) throws SQLException {
    checkIndex(parameterIndex, true);
  }

  /**
   * Helper function for the getXXX calls to check isFunction and index == 1.
   *
   * @param parameterIndex index of getXXX (index) check to make sure is a function and index == 1
   * @param fetchingData fetching data
   */
  private void checkIndex(int parameterIndex, boolean fetchingData) throws SQLException {
    if (!isFunction) {
      throw new PSQLException(
          GT.tr(
              "A CallableStatement was declared, but no call to registerOutParameter(1, <some type>) was made."),
          PSQLState.STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL);
    }

    if (fetchingData) {
      if (!returnTypeSet) {
        throw new PSQLException(GT.tr("No function outputs were registered."),
            PSQLState.OBJECT_NOT_IN_STATE);
      }

      if (callResult == null) {
        throw new PSQLException(
            GT.tr("Results cannot be retrieved from a CallableStatement before it is executed."),
            PSQLState.NO_DATA);
      }

      lastIndex = parameterIndex;
    }
  }

  @Override
  protected BatchResultHandler createBatchHandler(Query[] queries,
      ParameterList[] parameterLists) {
    return new CallableBatchResultHandler(this, queries, parameterLists);
  }

  public java.sql.Array getArray(int i) throws SQLException {
    checkClosed();
    checkIndex(i, Types.ARRAY, "Array");
    return (Array) callResult[i - 1];
  }

  public java.math.BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.DECIMAL, "BigDecimal");
    try {
    	 return ((BigDecimal) callResult[parameterIndex - 1]);
    }catch(Exception e) {
    	return new BigDecimal(callResult[parameterIndex-1].toString());

    }
   
  }

  public Blob getBlob(int i) throws SQLException {
    byte[] byt = (byte[]) callResult[i - 1];
    PGBlob blob = new PGBlob();
    blob.setBytes(1, byt);
    return blob;
  }

  public Clob getClob(int i) throws SQLException {
    Clob clob = null;
    if (callResult[i - 1] instanceof PGClob) {
      PGClob pgClob = (PGClob) callResult[i - 1];
      String str = pgClob.getSubString(1, (int) pgClob.length());
      clob = new PGClob();
      clob.setString(1, str);
    } else {
      /* before c20 version, clob type is actually text */
      /* we just use 'getString' method to get Text and set it into clob Object */
      clob = new PGClob();
      clob.setString(1, getString(i));
    }
    return clob;
  }

  public Object getObjectImpl(int i, Map<String, Class<?>> map) throws SQLException {
    if (map == null || map.isEmpty()) {
      return getObject(i);
    }
    throw Driver.notImplemented(this.getClass(), "getObjectImpl(int,Map)");
  }

  public Ref getRef(int i) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getRef(int)");
  }

  public java.sql.Date getDate(int i, java.util.Calendar cal) throws SQLException {
    checkClosed();
    checkIndex(i, Types.DATE, "Date");

    if (callResult[i - 1] == null) {
      return null;
    }

    String value = callResult[i - 1].toString();
    return connection.getTimestampUtils().toDate(cal, value);
  }

  public Time getTime(int i, java.util.Calendar cal) throws SQLException {
    checkClosed();
    checkIndex(i, Types.TIME, "Time");

    if (callResult[i - 1] == null) {
      return null;
    }

    String value = callResult[i - 1].toString();
    return connection.getTimestampUtils().toTime(cal, value);
  }

  public Timestamp getTimestamp(int i, java.util.Calendar cal) throws SQLException {
    checkClosed();
    checkIndex(i, Types.TIMESTAMP, "Timestamp");

    if (callResult[i - 1] == null) {
      return null;
    }

    String value = callResult[i - 1].toString();
    return connection.getTimestampUtils().toTimestamp(cal, value);
  }

  public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
          throws SQLException {
    if (sqlType == Types.STRUCT) {
      // Bind struct object
      checkClosed();
      if (!isFunction) {
        throw new PSQLException(
                GT.tr(
                        "This statement does not declare an OUT parameter.  Use '{' ?= call ... '}' to declare one."),
                PSQLState.STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL);
      }
      int oid = connection.getTypeInfo().getPGType(typeName);
      if (oid == Oid.UNSPECIFIED) {
        throw new PSQLException(
                GT.tr("Unable to find server array type for provided name {0}.", typeName),
                PSQLState.INVALID_NAME);
      }
      checkIndex(parameterIndex, false);
      // determine whether to overwrite the original VOID with the oid of the out parameter according to the
      // compatibility mode and the state of the guc parameter value
      preparedParameters.registerOutParameter(parameterIndex, Types.STRUCT);
      if (isACompatibilityAndOverLoad()) {
        preparedParameters.bindRegisterOutParameter(parameterIndex, oid, isACompatibilityFunction);
      }
      returnOids[parameterIndex - 1] = oid;
      functionReturnType[parameterIndex - 1] = Types.STRUCT;
      testReturn[parameterIndex - 1] = Types.STRUCT;
      returnTypeSet = true;
      isContainCompositeType = true;
    } else if (sqlType == Types.ARRAY) {
      // Bind array object
      int oid = connection.getTypeInfo().getPGArrayType(typeName);
      if (oid == Oid.UNSPECIFIED) {
        throw new PSQLException(
                GT.tr("Unable to find server array type for provided name {0}.", typeName),
                PSQLState.INVALID_NAME);
      }
      preparedParameters.registerOutParameter(parameterIndex, Types.ARRAY);
      if (isACompatibilityAndOverLoad()) {
        preparedParameters.bindRegisterOutParameter(parameterIndex, oid, isACompatibilityFunction);
      }
      returnOids[parameterIndex - 1] = oid;
      functionReturnType[parameterIndex - 1] = Types.ARRAY;
      testReturn[parameterIndex - 1] = Types.ARRAY;
      returnTypeSet = true;
    } else {
      throw Driver.notImplemented(this.getClass(), "registerOutParameter(int,int,String)");
    }
  }

  /**
   * whether it is oracle compatibility mode and reload is turned on
   * @return true or false
   */
  private boolean isACompatibilityAndOverLoad() {
    return enableOutparamOveride && ("A".equalsIgnoreCase(compatibilityMode) || "ORA".equalsIgnoreCase(compatibilityMode));
  }

  public RowId getRowId(int parameterIndex) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getRowId(int)");
  }

  public RowId getRowId(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getRowId(String)");
  }

  public void setRowId(String parameterName, RowId x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setRowId(String, RowId)");
  }

  public void setNString(String parameterName, String value) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNString(String, String)");
  }

  public void setNCharacterStream(String parameterName, Reader value, long length)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNCharacterStream(String, Reader, long)");
  }

  public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNCharacterStream(String, Reader)");
  }

  public void setCharacterStream(String parameterName, Reader value, long length)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setCharacterStream(String, Reader, long)");
  }

  public void setCharacterStream(String parameterName, Reader value) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setCharacterStream(String, Reader)");
  }

  public void setBinaryStream(String parameterName, InputStream value, long length)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBinaryStream(String, InputStream, long)");
  }

  public void setBinaryStream(String parameterName, InputStream value) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBinaryStream(String, InputStream)");
  }

  public void setAsciiStream(String parameterName, InputStream value, long length)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setAsciiStream(String, InputStream, long)");
  }

  public void setAsciiStream(String parameterName, InputStream value) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setAsciiStream(String, InputStream)");
  }

  public void setNClob(String parameterName, NClob value) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNClob(String, NClob)");
  }

  public void setClob(String parameterName, Reader reader, long length) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setClob(String, Reader, long)");
  }

  public void setClob(String parameterName, Reader reader) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setClob(String, Reader)");
  }

  public void setBlob(String parameterName, InputStream inputStream, long length)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBlob(String, InputStream, long)");
  }

  public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBlob(String, InputStream)");
  }

  public void setBlob(String parameterName, Blob x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBlob(String, Blob)");
  }

  public void setClob(String parameterName, Clob x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setClob(String, Clob)");
  }

  public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNClob(String, Reader, long)");
  }

  public void setNClob(String parameterName, Reader reader) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNClob(String, Reader)");
  }

  public NClob getNClob(int parameterIndex) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getNClob(int)");
  }

  public NClob getNClob(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getNClob(String)");
  }

  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setSQLXML(String, SQLXML)");
  }

  public SQLXML getSQLXML(int parameterIndex) throws SQLException {
    checkClosed();
    checkIndex(parameterIndex, Types.SQLXML, "SQLXML");
    return (SQLXML) callResult[parameterIndex - 1];
  }

  public SQLXML getSQLXML(String parameterIndex) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getSQLXML(String)");
  }

  public String getNString(int parameterIndex) throws SQLException {
    return this.getString(parameterIndex);
  }

  public String getNString(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getNString(String)");
  }

  public Reader getNCharacterStream(int parameterIndex) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getNCharacterStream(int)");
  }

  public Reader getNCharacterStream(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getNCharacterStream(String)");
  }

  public Reader getCharacterStream(int parameterIndex) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getCharacterStream(int)");
  }

  public Reader getCharacterStream(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getCharacterStream(String)");
  }

  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    if (type == ResultSet.class) {
      return type.cast(getObject(parameterIndex));
    }
    throw new PSQLException(GT.tr("Unsupported type conversion to {1}.", type),
            PSQLState.INVALID_PARAMETER_VALUE);
  }

  public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getObject(String, Class<T>)");
  }

  public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "registerOutParameter(String,int)");
  }

  public void registerOutParameter(String parameterName, int sqlType, int scale)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "registerOutParameter(String,int,int)");
  }

  public void registerOutParameter(String parameterName, int sqlType, String typeName)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "registerOutParameter(String,int,String)");
  }

  public java.net.URL getURL(int parameterIndex) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getURL(String)");
  }

  public void setURL(String parameterName, java.net.URL val) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setURL(String,URL)");
  }

  public void setNull(String parameterName, int sqlType) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNull(String,int)");
  }

  public void setBoolean(String parameterName, boolean x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBoolean(String,boolean)");
  }

  public void setByte(String parameterName, byte x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setByte(String,byte)");
  }

  public void setShort(String parameterName, short x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setShort(String,short)");
  }

  public void setInt(String parameterName, int x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setInt(String,int)");
  }

  public void setLong(String parameterName, long x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setLong(String,long)");
  }

  public void setFloat(String parameterName, float x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setFloat(String,float)");
  }

  public void setDouble(String parameterName, double x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setDouble(String,double)");
  }

  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBigDecimal(String,BigDecimal)");
  }

  public void setString(String parameterName, String x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setString(String,String)");
  }

  public void setBytes(String parameterName, byte[] x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBytes(String,byte)");
  }

  public void setDate(String parameterName, java.sql.Date x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setDate(String,Date)");
  }

  public void setTime(String parameterName, Time x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setTime(String,Time)");
  }

  public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setTimestamp(String,Timestamp)");
  }

  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setAsciiStream(String,InputStream,int)");
  }

  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setBinaryStream(String,InputStream,int)");
  }

  public void setObject(String parameterName, Object x, int targetSqlType, int scale)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setObject(String,Object,int,int)");
  }

  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setObject(String,Object,int)");
  }

  public void setObject(String parameterName, Object x) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setObject(String,Object)");
  }

  public void setCharacterStream(String parameterName, Reader reader, int length)
      throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setCharacterStream(String,Reader,int)");
  }

  public void setDate(String parameterName, java.sql.Date x, Calendar cal) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setDate(String,Date,Calendar)");
  }

  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setTime(String,Time,Calendar)");
  }

  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setTimestamp(String,Timestamp,Calendar)");
  }

  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "setNull(String,int,String)");
  }

  public String getString(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getString(String)");
  }

  public boolean getBoolean(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getBoolean(String)");
  }

  public byte getByte(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getByte(String)");
  }

  public short getShort(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getShort(String)");
  }

  public int getInt(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getInt(String)");
  }

  public long getLong(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getLong(String)");
  }

  public float getFloat(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getFloat(String)");
  }

  public double getDouble(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getDouble(String)");
  }

  public byte[] getBytes(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getBytes(String)");
  }

  public java.sql.Date getDate(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getDate(String)");
  }

  public Time getTime(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getTime(String)");
  }

  public Timestamp getTimestamp(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getTimestamp(String)");
  }

  public Object getObject(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getObject(String)");
  }

  public BigDecimal getBigDecimal(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getBigDecimal(String)");
  }

  public Object getObjectImpl(String parameterName, Map<String, Class<?>> map) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getObject(String,Map)");
  }

  public Ref getRef(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getRef(String)");
  }

  public Blob getBlob(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getBlob(String)");
  }

  public Clob getClob(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getClob(String)");
  }

  public Array getArray(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getArray(String)");
  }

  public java.sql.Date getDate(String parameterName, Calendar cal) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getDate(String,Calendar)");
  }

  public Time getTime(String parameterName, Calendar cal) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getTime(String,Calendar)");
  }

  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getTimestamp(String,Calendar)");
  }

  public java.net.URL getURL(String parameterName) throws SQLException {
    throw Driver.notImplemented(this.getClass(), "getURL(String)");
  }

  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
    // ignore scale for now
    registerOutParameter(parameterIndex, sqlType);
  }
}
