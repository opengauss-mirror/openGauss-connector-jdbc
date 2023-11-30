package org.postgresql.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.postgresql.PGProperty;
import org.postgresql.core.NativeQuery;
import org.postgresql.core.Parser;
import org.postgresql.log.Log;
import org.postgresql.log.Logger;
/**
 *
 * Captures the client logic functionality to be exposed in the JDBC driver
 * It uses the JNI interface as a back-end using the ClientLogicImpl class
 *
 */
public class ClientLogic {
    static final int ERROR_BAD_RESPONSE_NULL = 1001; /* using large numbers to avoid collision with libpq error code numbers */
    static final int ERROR_BAD_RESPONSE_1 = 1002;
    static final int ERROR_BAD_RESPONSE_2 = 1003;
    static final int ERROR_BAD_RESPONSE_3 = 1004;
    static final String ERROR_TEXT_BAD_RESPONSE = "Bad response from backend componenet";
    static final int ERROR_CONNECTION_IS_OPEN = 1005;
    static final String ERROR_TEXT_CONNECTION_IS_OPEN = "client logic connection is already opened";
    static final int ERROR_INVALID_HANDLE = 1010;
    static final String ERROR_TEXT_INVALID_HANDLE = "Invalid handle";
    static final int ERROR_EMPTY_DATA = 1012;
    static final String ERROR_TEXT_EMPTY_DATA = "Empty data";
    static final int ERROR_PARSER_FAILURE = 1013;
    static final String ERROR_TEXT_PARSER_FAILURE = "Failed parsing the input query";

    private AtomicInteger stamentNameCounter = new AtomicInteger(0);

    private static Log LOGGER = Logger.getLogger(ClientLogic.class.getName());
    /**
     * CLientLogicStatus class is responsible for parsing the result from the gauss_cl_jni library
     */
    static private class CLientLogicStatus{
        private int errorCode = 0;
        private String errorText = "";
        public CLientLogicStatus(Object[] JNIResult) {
            if (JNIResult == null) {
                errorCode = ERROR_BAD_RESPONSE_NULL;
                errorText = ERROR_TEXT_BAD_RESPONSE;
                return;
            }
            if (JNIResult.length < 1) {
                errorCode = ERROR_BAD_RESPONSE_1;
                errorText = ERROR_TEXT_BAD_RESPONSE;
                return;
            }
            if (JNIResult[0] == null) {
                errorCode = ERROR_BAD_RESPONSE_3;
                errorText = ERROR_TEXT_BAD_RESPONSE;
                return;
            }
            Object[] statusData = (Object[]) JNIResult[0];
            if (statusData.length < 2) {
                errorCode = ERROR_BAD_RESPONSE_2;
                errorText = ERROR_TEXT_BAD_RESPONSE;
                return;
            }
            if (statusData[0] != null) {
                errorText = (String)statusData[1];
                if (statusData[0].getClass().isArray()) {
                    int[] errorCodeArr = (int[])statusData[0];
                    if (errorCodeArr.length > 0) {
                        errorCode = (int)errorCodeArr[0];
                    }
                }
            }
            if (statusData[1] != null) {
                errorText = (String)statusData[1];
            }
        }
        /**
         *
         * @return if true if the current request is OK and false if it is failed
         */
        public boolean isOK() {
            return errorCode == 0;
        }
        @Override
        public String toString() {
            return "CLientLogicStatus [error code=" + errorCode + ", error text=" + errorText + "]";
        }

        public int getErrorCode() {
            return errorCode;
        }

        public String getErrorText() {
            return errorText;
        }
    }
    private ClientLogicImpl impl = null;
    public ClientLogic() {
    }

    /**
     * Closes the connection to the database
     */
    public void close() {
        if (impl != null) {
            impl.close();
            impl = null;
        }
    }

    /**
     * if finalize called make sure the connectiion is cloased
     */
    protected void finalize() {
 		close();
	}

    /**
     * Link the client logic JNI & C side to the PgConnection instance
     * @param databaseName database name
     * @param jdbcConn the JDBC connection object to be used to select cache data with
     * @throws ClientLogicException
     */
    public void linkClientLogic(String databaseName, PgConnection jdbcConn) throws ClientLogicException {
		impl = new ClientLogicImpl();
		Object[] result = impl.linkClientLogic(databaseName, jdbcConn);
		CLientLogicStatus status = new CLientLogicStatus(result);
		if (!status.isOK()) {
			throw new ClientLogicException(status.getErrorCode(), status.getErrorText());
		}
		long handle = 0;
		if (result.length > 1) {
			if (result.getClass().isArray()) {
					long[] handleArr = (long[]) result[1];
					if (handleArr.length > 0) {
						handle = handleArr[0];
				  }
			}
		}
		if (handle == 0 || handle < 0) {
			throw new ClientLogicException(ERROR_INVALID_HANDLE, ERROR_TEXT_INVALID_HANDLE);
		}
		impl.setHandle(handle);
    }
  
    /**
     * Runs the pre-process function of the client logic
     * to change the client logic fields from the user input to the client logic format
     * @param originalQuery query to modify with user input
     * @return the modified query with client logic fields changed
     * @throws ClientLogicException
     */
    public String runQueryPreProcess(String originalQuery) throws ClientLogicException {
        Object[] result = impl.runQueryPreProcess(originalQuery);
        CLientLogicStatus status = new CLientLogicStatus(result);
        if (!status.isOK()) {
          String errorText = status.getErrorText();
          //Remove trailing \n as it fails the tests
          if (errorText.length() > 1) {
            if (errorText.charAt(errorText.length() - 1) == '\n') {
              errorText = errorText.substring(0, errorText.length() - 1);
            }
            throw new ClientLogicException(status.getErrorCode(), errorText);
          }
        }

        String resultQuery = "";
        if (result.length > 1) {
            if (result[1] != null) {
                resultQuery  = (String) result[1];
            }
        }
        if (resultQuery.length() == 0) {
            resultQuery  = originalQuery;//Empty query received, just ignore
        }
        return resultQuery;
    }
    /**
     * Runs the post query function - executed after the query has completed
     * @throws ClientLogicException
     */
    public void runQueryPostProcess() throws ClientLogicException {
        Object[] result = impl.runQueryPostProcess();
        CLientLogicStatus status = new CLientLogicStatus(result);
        if (!status.isOK()) {
            throw new ClientLogicException(status.getErrorCode(), status.getErrorText());
        }
    }

    /**
     * Runs client logic on fields to get back the user format
     * @param data2Process the client logic data
     * @param dataType the OID of the user data
     * @return the data in user format
     * @throws ClientLogicException
     */
    public String runClientLogic(String data2Process, int dataType) throws ClientLogicException {
        Object[] result = impl.runClientLogic(data2Process, dataType);
        CLientLogicStatus status = new CLientLogicStatus(result);
        if (!status.isOK()) {
            throw new ClientLogicException(status.getErrorCode(), status.getErrorText());
        }
        String resultData = "";
        if (result.length > 1) {
            if (result[1] != null) {
                resultData  = (String) result[1];
            }
        }
        if (resultData.length() == 0) {
            throw new ClientLogicException(ERROR_EMPTY_DATA, ERROR_TEXT_EMPTY_DATA);
        }
        return resultData;
    }
    /**
     * @return true if the client logic instance is active (connected to the database) and false if it is not
     */
    public boolean isActive() {
        if (impl != null && impl.getHandle() > 0) {
            return true;
        }
        return false;
    }

    /**
     * Check if oid of returned type is a client logic field
     * @param dataType oid of data type
     * @return true for client logic and false if it is not
     */
    public static boolean isClientLogicField(int dataType) {
        return (dataType == 4402 || dataType == 4403);
    }

    /**
     * prepare a query
     * @param query the query SQL
     * @param statement_name the statement name
     * @return the modified query
     * @throws ClientLogicException
     */
    public String prepareQuery(String query, String statement_name) throws ClientLogicException {
        //Replace the query syntax from jdbc syntax with ? for bind parameters to $1 $2 ... $n
        List<NativeQuery> queries;
        try {
            queries = Parser.parseJdbcSql(query, true, true, true, true, true);
        } catch (SQLException e) {
            throw new ClientLogicException(ERROR_PARSER_FAILURE, ERROR_TEXT_PARSER_FAILURE, true);
        }
        String queryNative;
        int parameter_count;
        if (queries.size() > 0 && queries.get(0) != null) {
            queryNative = queries.get(0).nativeSql;
            parameter_count = queries.get(0).bindPositions.length;
        } else {
            throw new ClientLogicException(ERROR_PARSER_FAILURE, ERROR_TEXT_PARSER_FAILURE, true);
        }
        /*Run the actual query */
        Object[] result = impl.prepareQuery(queryNative, statement_name, parameter_count);
        CLientLogicStatus status = new CLientLogicStatus(result);
        if (!status.isOK()) {
            throw new ClientLogicException(status.getErrorCode(), status.getErrorText());
        }
        String modifiedQuery = "";
        if (result.length > 1) {
            if (result[1] != null) {
                modifiedQuery  = (String) result[1];
            }
        }
        if (modifiedQuery.length() == 0) {
            throw new ClientLogicException(ERROR_EMPTY_DATA, ERROR_TEXT_EMPTY_DATA, true);
        }
        return modifiedQuery;
    }
    /**
     * Replaces client logic parameter values in a prepared statement
     * @param statementName the name of the statement - the one used in the prepareQuery method
     * @param paramValues the list of current values
     * @return list of modified parameters
     * @throws ClientLogicException
     */
    public List<String> replaceStatementParams(String statementName, List<String> paramValues) throws ClientLogicException {
        String[] arrParams = new String[paramValues.size()];
        for (int i = 0; i < paramValues.size(); ++i) {
            arrParams[i] = paramValues.get(i);
        }
        Object[] resultImpl = impl.replaceStatementParams(statementName, arrParams);
        CLientLogicStatus status = new CLientLogicStatus(resultImpl);
        if (!status.isOK()) {
            throw new ClientLogicException(status.getErrorCode(), status.getErrorText());
        }
        if (resultImpl[1] == null || !resultImpl[1].getClass().isArray()) {
            throw new ClientLogicException(ERROR_EMPTY_DATA, ERROR_TEXT_EMPTY_DATA);
        }
        Object[] resultsImplArr = (Object[])resultImpl[1];
        List<String> result = new ArrayList<>();
        for (int i = 0; i < resultsImplArr.length; ++i) {
            Object value = resultsImplArr[i];
            if (value != null &&  value.getClass().equals(String.class)) {
                result.add((String)value);
            } else {
                result.add(null);
            }
        }
        return result;
    }
    /**
     * @return unique statement name that can be used for prepare statement.
     */
    public String getStatementName() {
        int stamanetCounter = stamentNameCounter.incrementAndGet();
        return "statament_" + stamanetCounter;
    }

    /**
     * Applies client logic to extract client logic data inside error messages into user input
     * @param originalMessage the original message with client logic data
     * @return
     */
    public String clientLogicMessage(String originalMessage) {
        String newMessage = originalMessage;
        Object[] result = impl.replaceErrorMessage(originalMessage);
        CLientLogicStatus status = new CLientLogicStatus(result);
        if (!status.isOK()) {
            return newMessage; // equal to the original message in this case
        }
        if (result.length > 1) {
            if (result[1] != null) {
                newMessage = (String) result[1];
                if (newMessage.length() == 0) { // If it is empty do not use it
                    newMessage = originalMessage;
                }
            }
        }
        return newMessage;
    }
} // End of class
