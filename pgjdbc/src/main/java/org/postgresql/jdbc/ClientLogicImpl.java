package org.postgresql.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.postgresql.util.JdbcBlackHole;

public class ClientLogicImpl {
    static {
        System.loadLibrary("gauss_cl_jni");
    }
    // Native methods:
    private native Object[] linkClientLogicImpl(String databaseName);
    private native Object[] setKmsInfoImpl(long handle, String key, String value);
    private native Object[] runQueryPreProcessImpl(long handle, String originalQuery);
    private native Object[] runQueryPostProcessImpl(long handle);
    private native Object[] runClientLogicImpl(long handle, String processData, int dataType);

    private native Object[] getRecordIDsImpl(long mHandle, String dataTypeName, int oid);
    private native Object[] runClientLogic4RecordImpl(long handle, String data2Process, int[] originalOids);

    private native Object[] prepareQueryImpl(long handle, String query, String statement_name, int parameter_count);
    private native Object[] replaceStatementParamsImpl(long handle, String statementName, String[] param_values);
    private native Object[] replaceErrorMessageImpl(long handle, String originalMessage);
    private native void reloadCacheImpl(long handle);
    private native void reloadCacheIfNeededImpl(long handle);
    private native void destroy(long handle);

    private long m_handle = 0;
    private PgConnection m_jdbcConn = null;
  /**
   * Link between the Java PgConnection and the PGConn Client logic object
   * @param databaseName the database name
   * @param jdbcConn the JDBC connection to be later use to fetch data
   * @return
   *[0][0] - int status code - zero for success
   *[0][1] - string status description
   *[1] - long - instance handle to be re-used in future calls by the same connection
   */
  public Object[] linkClientLogic(String databaseName, PgConnection jdbcConn) {
  	if (m_handle == 0 && m_jdbcConn == null) {
  		m_jdbcConn = jdbcConn;
  		return linkClientLogicImpl(databaseName);
  	}
  	else {
  		//did not add much logic here, will handle it on the parent class
      return new Object[]{};
  	}
  }

    /**
    * Transfer all parameters that are used to establish a connection with HuaweiCloud IAM and KMS to 'gauss_cl_jni'.
    */
    public Object[] setKmsInfo(String key, String value) {
        return setKmsInfoImpl(m_handle, key, value);
    }

    /**
     * Run the pre query, to replace client logic field values with binary format before sending the query to the database server
     * @param originalQuery the query with potentially client logic values in user format
     * @return array of objects
     *[0][0] - int status code - zero for success
     *[0][1] - string status description
     *[1] - String - The modified query
     */
    public Object[] runQueryPreProcess(String originalQuery) {
        return runQueryPreProcessImpl(m_handle, originalQuery);
    }
  /**
   * Replace client logic field value with user input - used when receiving data in a resultset
   * @param processData the data in binary format (hexa)
   * @param dataType the oid (modid) of the original field type
   * @return array of objects
   *[0][0] - int status code - zero for success
   *[0][1] - string status description
   *[1] - String - The data in user format
   */
  public Object[] runClientLogic(String processData, int dataType) {
  	return runClientLogicImpl(m_handle, processData, dataType);
  }

    /**
     * Gets the list of original oids, needed when returning a record from a function that has client logic fields
     *
     * @param dataTypeName the name of the data type of the oculmn in the resultset
     * @param oid the fields oid
     * @return array of object in the following format
     * [0][0] - int status code - zero for success
     * [0][1] - string status description
     * [1][0...n] the original oids in the record if it contains any clint loigic fields, otherwise this part is omitted
     */
    public Object[] getRecordIDs(String dataTypeName, int oid) {
        return getRecordIDsImpl(m_handle, dataTypeName, oid);
    }

    /**
     * convert client records returned from function that contains client logic fields to user format
     *
     * @param data2Process the record with client logic fields
     * @param originalOids the result from getRecordIDs method for that field
     * @return array of object in the following format
     * [0][0] - int status code - zero for success
     * [0][1] - string status description
     * [0][0] - int status code - zero for success
     * [0][1] - string status description
     * [1] - int 0 not client logic 1 - is client logic
     * [2] - String - The data in user format
     */
    public Object[] runClientLogic4Record(String data2Process, int[] originalOids) {
        return runClientLogic4RecordImpl(m_handle, data2Process, originalOids);
    }

  /**
   * run post process on the backend, to free the client logic state machine when a query is done
   * @return array of objects
   *[0][0] - int status code - zero for success
   *[0][1] - string status description
   */
  public Object[] runQueryPostProcess() {
  	return runQueryPostProcessImpl(m_handle);
  }
  /**
   * Prepare a statement on the backend side
   * @param query the statement query text
   * @param statement_name the name on the the statement
   * @param parameter_count number of parameters in the query
   * @return array of objects
   *[0][0] - int status code - zero for success
   *[0][1] - string status description
   *[1] - String - The modified query - to be used if the query had client logic fields in user format that have to be replaces with binary value
   */
  public Object[] prepareQuery(String query, String statement_name, int parameter_count){
  	return  prepareQueryImpl(m_handle, query, statement_name, parameter_count);
  }
  /**
   * replace parameters values in prepared statement - to be called before binding the parameters and executing the statement
   * @param statementName the name of the statement
   * @param paramValues array of parameters in user format
   * @return array of objects
   * [0][0] - int status code - zero for success
   * [0][1] - string status description
   * [1][0 ... parameter_count - 1] - array with the parameters value, if the parameter is not
   * being replace a NULL apears otherwise the replaced value
   * [2][0 ... parameter_count - 1] - array with the parameters' type-oids,
   * if the parameter is being replaced, otherwise 0
   */
  public Object[] replaceStatementParams(String statementName, String[] paramValues) {
  	return replaceStatementParamsImpl(m_handle, statementName, paramValues);
  }

  /**
   * Replace binary client logic values in an error message back to user input format
   * For example changes unique constraint error message from:
   * 		... Key (name)=(\xa1d4....) already exists. ...
   * to:
   * 		... Key (name)=(John) already exists. ...
   * @param originalMessage the error message received from the server
   * @return array of objects
   *[0][0] - int status code - zero for success
   *[0][1] - string status description
   *[1] - String - The modified message, may return empty string if the message has no client loic values in it
   */
  public Object[] replaceErrorMessage(String originalMessage) {
  	return replaceErrorMessageImpl(m_handle, originalMessage);
  }
  /**
   * closes the backend libpq connection - must be called when the java connectyion is closed.
   */
  protected void close() {
    if (m_handle > 0) {
      destroy(m_handle);
    }
    m_handle = 0;
  }
   /**
    * Standard java method
    */
  protected void finalize() {
      close();
  }

  /**
   * setter function to set the handle
   * @param handle
   */
  public void setHandle(long handle) {
    m_handle = handle;
  }
  /**
   * getter function to get the handler
   * @return
   */
  public long getHandle() {
    return m_handle;
  }

  /**
   * This method is being invoked from the client logic c++ code
   * It is used to fetch data from the server regarding the client logic settings - cache manager
   * @param query the query to incoke
   * @return array of results in the following format
   * [0] - array of column headers
   * [1...n] - array of results
  */
  public Object[] fetchDataFromQuery(String query) {
    PgStatement st = null;
    List<Object> data = new ArrayList<>();
    data.add(""); //No Error
    try {
      st = (PgStatement)m_jdbcConn.createStatement();
      ResultSet rs = st.executeQueryWithNoCL(query);

      ResultSetMetaData rsmd = rs.getMetaData();
      int columnsCount = rsmd.getColumnCount();
      List<String> headers = new ArrayList<>();
      //fill data in headers
      for (int colIndex = 0; colIndex < columnsCount; ++colIndex) {
        headers.add(rsmd.getColumnName(colIndex+1));
      }
      data.add(headers.toArray());
      while (rs.next()){
        List<String> record = new ArrayList<>();
        for (int colIndex = 1; colIndex < columnsCount + 1; ++colIndex) {
          String colValue = rs.getString(colIndex);
          if (colValue == null) {
            colValue = "";
          }
          record.add(colValue);
        }
        data.add(record.toArray());
      }
      st.close();
    }
    catch (SQLException e) {
    	List<Object> errorResponse = new ArrayList<>();
    	errorResponse.add(e.getMessage());
    	return errorResponse.toArray();
    }	finally {
        JdbcBlackHole.close(st);
    }
	  return data.toArray();
  }

    /**
     * Reloads the client logic cache, required when there is an error related to missing client logic cache
     */
    public void reloadCache() {
        if (m_handle > 0) {
            reloadCacheImpl(m_handle);
        }
    }

    /**
     * Reloads the client logic cache ONLY if the timestamp of the configuration fetched is earlier
     * than the timestamp of the configuration on the server
     */
    public void reloadCacheIfNeeded() {
        if (m_handle > 0) {
            reloadCacheIfNeededImpl(m_handle);
        }
    }
}
