/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
// Copyright (c) 2004, Open Cloud Limited.

package org.postgresql.core.v3;

import org.postgresql.PGProperty;
import org.postgresql.clusterchooser.ClusterStatus;
import org.postgresql.clusterchooser.GlobalClusterStatusTracker;
import org.postgresql.clusterhealthy.ClusterNodeCache;
import org.postgresql.core.ConnectionFactory;
import org.postgresql.core.PGStream;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ServerVersion;
import org.postgresql.core.SetupQueryRunner;
import org.postgresql.core.SocketFactoryFactory;
import org.postgresql.core.Utils;
import org.postgresql.core.Version;
import org.postgresql.hostchooser.*;
import org.postgresql.jdbc.SslMode;
import org.postgresql.QueryCNListUtils;
import org.postgresql.quickautobalance.ConnectionManager;
import org.postgresql.util.*;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import javax.net.SocketFactory;

/**
 * ConnectionFactory implementation for version 3 (7.4+) connections.
 *
 * @author Oliver Jowett (oliver@opencloud.com), based on the previous implementation
 */
public class ConnectionFactoryImpl extends ConnectionFactory {

  private static Log LOGGER = Logger.getLogger(ConnectionFactoryImpl.class.getName());
  private static final int AUTH_REQ_OK = 0;
  private static final int AUTH_REQ_KRB4 = 1;
  private static final int AUTH_REQ_KRB5 = 2;
  private static final int AUTH_REQ_PASSWORD = 3;
  private static final int AUTH_REQ_CRYPT = 4;
  private static final int AUTH_REQ_MD5 = 5;
  private static final int AUTH_REQ_SCM = 6;
  private static final int AUTH_REQ_GSS = 7;
  private static final int AUTH_REQ_GSS_CONTINUE = 8;
  private static final int AUTH_REQ_SSPI = 9;

  public static String CLIENT_ENCODING = "UTF8";
  public static String USE_BOOLEAN = "false";
  private static final int AUTH_REQ_SHA256 = 10;
  private static final int AUTH_REQ_MD5_SHA256 = 11;
    private static final int AUTH_REQ_SM3 = 13;
  private static final int PLAIN_PASSWORD = 0;
  private static final int MD5_PASSWORD  = 1;
  private static final int SHA256_PASSWORD =  2;
    private static final int SM3_PASSWORD = 3;
    private static final int ERROR_PASSWORD = 4;
  private static final int PROTOCOL_VERSION_351 = 351;
  private static final int PROTOCOL_VERSION_350 = 350;
  private int protocolVerion = PROTOCOL_VERSION_351;
  private String connectInfo = "";

  /**
   * Whitelist of supported client_encoding
   */
  public static final HashMap<String, String> CLIENT_ENCODING_WHITELIST = new HashMap<>();

  static {
    CLIENT_ENCODING_WHITELIST.put("UTF8", "UTF8");
    CLIENT_ENCODING_WHITELIST.put("UTF-8", "UTF-8");
    CLIENT_ENCODING_WHITELIST.put("GBK", "GBK");
    CLIENT_ENCODING_WHITELIST.put("LATIN1", "LATIN1");
  }
  public static void setStaticClientEncoding(String client) {
      ConnectionFactoryImpl.CLIENT_ENCODING = client;
  }
  public void setClientEncoding(String client) {
      setStaticClientEncoding(client);
  }
  public static void setStaticUseBoolean(String useBoolean) {
      ConnectionFactoryImpl.USE_BOOLEAN = useBoolean;
  }
  public void setUseBooleang(String useBoolean) {
      setStaticUseBoolean(useBoolean);
  }


  private void setSocketTimeout(PGStream stream, Properties info, PGProperty propKey) throws SQLException, IOException {
    // Set the socket timeout if the "socketTimeout" property has been set.
    int socketTimeout = Integer.parseInt(propKey.getDefaultValue());
    if (propKey.getInt(info) <= Integer.MAX_VALUE / 1000) {
      socketTimeout = propKey.getInt(info);
    } else {
      LOGGER.debug("integer socketTimeout is too large, it will occur error after multiply by 1000.");
    }
    if (socketTimeout >= 0) {
      stream.getSocket().setSoTimeout(socketTimeout * 1000);
    }
  }

  public PGStream tryConnect(String user, String database,
      Properties info, SocketFactory socketFactory, HostSpec hostSpec,
      SslMode sslMode)
      throws SQLException, IOException {
    int connectTimeout = Integer.parseInt(PGProperty.CONNECT_TIMEOUT.getDefaultValue());
    if (PGProperty.CONNECT_TIMEOUT.getInt(info) <= Integer.MAX_VALUE / 1000) {
        connectTimeout = PGProperty.CONNECT_TIMEOUT.getInt(info) * 1000;
    } else {
        LOGGER.debug("integer connectTimeout is too large, it will occur error after multiply by 1000.");
    }

    PGStream newStream = new PGStream(socketFactory, hostSpec, connectTimeout);

    // Construct and send an ssl startup packet if requested.
    newStream = enableSSL(newStream, sslMode, info, connectTimeout);

    // Set the socket timeout if the "socketTimeout" property has been set.
    setSocketTimeout(newStream, info, PGProperty.SOCKET_TIMEOUT_IN_CONNECTING);

    // Enable TCP keep-alive probe if required.
    boolean requireTCPKeepAlive = PGProperty.TCP_KEEP_ALIVE.getBoolean(info);
    newStream.getSocket().setKeepAlive(requireTCPKeepAlive);

    // Try to set SO_SNDBUF and SO_RECVBUF socket options, if requested.
    // If receiveBufferSize and send_buffer_size are set to a value greater
    // than 0, adjust. -1 means use the system default, 0 is ignored since not
    // supported.

    // Set SO_RECVBUF read buffer size
    int receiveBufferSize = PGProperty.RECEIVE_BUFFER_SIZE.getInt(info);
    if (receiveBufferSize > -1) {
      // value of 0 not a valid buffer size value
      if (receiveBufferSize > 0) {
        newStream.getSocket().setReceiveBufferSize(receiveBufferSize);
      } else {
        LOGGER.warn("Ignore invalid value for receiveBufferSize: " + receiveBufferSize);
      }
    }

    // Set SO_SNDBUF write buffer size
    int sendBufferSize = PGProperty.SEND_BUFFER_SIZE.getInt(info);
    if (sendBufferSize > -1) {
      if (sendBufferSize > 0) {
        newStream.getSocket().setSendBufferSize(sendBufferSize);
      } else {
        LOGGER.warn("Ignore invalid value for sendBufferSize: " + sendBufferSize);
      }
    }

    List<String[]> paramList = getParametersForStartup(user, database, info);
    String protocolProp = info.getProperty("protocolVersion");
    this.protocolVerion = (protocolProp != null && !protocolProp.isEmpty()) ? Integer.parseInt(protocolProp) : PROTOCOL_VERSION_351;
    sendStartupPacket(newStream, paramList);

    // Do authentication (until AuthenticationOk).
    doAuthentication(newStream, hostSpec.getHost(), user, info);
    setSocketTimeout(newStream, info, PGProperty.SOCKET_TIMEOUT);

    return newStream;
  }

  @Override
  public QueryExecutor openConnectionImpl(HostSpec[] hostSpecs, String user, String database,
                                          Properties info) throws SQLException {
    if (info.getProperty("characterEncoding") != null) {
      if (CLIENT_ENCODING_WHITELIST.containsKey((info.getProperty("characterEncoding")).toUpperCase(Locale.ENGLISH))) {
        setClientEncoding(info.getProperty("characterEncoding").toUpperCase(Locale.ENGLISH));
      } else {
        LOGGER.warn("unsupported client_encoding: " + info.getProperty(
                "characterEncoding") + ", to ensure correct operation, please use the specified range " +
                "of client_encoding.");
      }
    }

    if (info.getProperty("use_boolean") != null) {
      setUseBooleang(info.getProperty("use_boolean").toUpperCase(Locale.ENGLISH));
    }

    SslMode sslMode = SslMode.of(info);

    HostRequirement targetServerType;
    String targetServerTypeStr = PGProperty.TARGET_SERVER_TYPE.get(info);
    try {
      targetServerType = HostRequirement.getTargetServerType(targetServerTypeStr);
    } catch (IllegalArgumentException ex) {
      throw new PSQLException(
              GT.tr("Invalid targetServerType value: {0}", targetServerTypeStr),
              PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    }

    SocketFactory socketFactory = SocketFactoryFactory.getSocketFactory(info);

    Iterator<ClusterSpec> clusterIter = GlobalClusterStatusTracker.getClusterFromHostSpecs(hostSpecs, info);
    Map<HostSpec, HostStatus> knownStates = new HashMap<>();
    Exception exception = new Exception();
    while (clusterIter.hasNext()) {
      ClusterSpec clusterSpec = clusterIter.next();
      HostSpec[] currentHostSpecs = clusterSpec.getHostSpecs();

      if (currentHostSpecs.length > 1 && targetServerType == HostRequirement.master) {
        ClusterNodeCache.checkHostSpecs(currentHostSpecs, info);
      }
      HostChooser hostChooser =
              HostChooserFactory.createHostChooser(currentHostSpecs, targetServerType, info);
      Iterator<CandidateHost> hostIter = hostChooser.iterator();
      boolean isMasterCluster = false;
      boolean isFirstIter = true;
      while (hostIter.hasNext()) {
        CandidateHost candidateHost = hostIter.next();
        HostSpec hostSpec = candidateHost.hostSpec;

        // Note: per-connect-attempt status map is used here instead of GlobalHostStatusTracker
        // for the case when "no good hosts" match (e.g. all the hosts are known as "connectfail")
        // In that case, the system tries to connect to each host in order, thus it should not look into
        // GlobalHostStatusTracker
        HostStatus knownStatus = knownStates.get(hostSpec);
        if (isFirstIter) {
          isFirstIter = false;
        } else {
          ConnectionManager.getInstance().incrementCachedCreatingConnectionSize(hostSpec, info);
        }
        if (knownStatus != null && !candidateHost.targetServerType.allowConnectingTo(knownStatus)) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Known status of host " + hostSpec + " is " + knownStatus + ", and required status was " + candidateHost.targetServerType + ". Will try next host");
          }
          ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
          continue;
        }
        
        //
        // Establish a connection.
        //
        connectInfo = UUID.randomUUID().toString(); // this is used to trace the time taken to establish the connection.
        LOGGER.info("[" + connectInfo + "] " + "Try to connect." + " IP: " + hostSpec.toString());
        PGStream newStream = null;
        try {
          try {
            newStream = tryConnect(user, database, info, socketFactory, hostSpec, sslMode);
          } catch (SQLException e) {
            if (sslMode == SslMode.PREFER
                    && PSQLState.INVALID_AUTHORIZATION_SPECIFICATION.getState().equals(e.getSQLState())) {
              // Try non-SSL connection to cover case like "non-ssl only db"
              // Note: PREFER allows loss of encryption, so no significant harm is made
              Throwable ex = null;
              try {
                newStream =
                        tryConnect(user, database, info, socketFactory, hostSpec, SslMode.DISABLE);
                LOGGER.debug("Downgraded to non-encrypted connection for host " + hostSpec);
              } catch (SQLException ee) {
                ex = ee;
              } catch (IOException ee) {
                ex = ee; // Can't use multi-catch in Java 6 :(
              }
              if (ex != null) {
                LOGGER.debug("sslMode==PREFER, however non-SSL connection failed as well", ex);
                // non-SSL failed as well, so re-throw original exception
                throw e;
              }
            } else if (sslMode == SslMode.ALLOW
                    && PSQLState.INVALID_AUTHORIZATION_SPECIFICATION.getState().equals(e.getSQLState())) {
              // Try using SSL
              Throwable ex = null;
              try {
                newStream =
                        tryConnect(user, database, info, socketFactory, hostSpec, SslMode.REQUIRE);
                LOGGER.debug("Upgraded to encrypted connection for host " +
                        hostSpec);
              } catch (SQLException ee) {
                ex = ee;
              } catch (IOException ee) {
                ex = ee; // Can't use multi-catch in Java 6 :(
              }
              if (ex != null) {
                LOGGER.debug("sslMode==ALLOW, however SSL connection failed as well", ex);
                // non-SSL failed as well, so re-throw original exception
                throw e;
              }

            } else {
              throw e;
            }
          }

          int cancelSignalTimeout = Integer.parseInt(PGProperty.CANCEL_SIGNAL_TIMEOUT.getDefaultValue());
          if (PGProperty.CANCEL_SIGNAL_TIMEOUT.getInt(info) <= Integer.MAX_VALUE / 1000) {
            cancelSignalTimeout = PGProperty.CANCEL_SIGNAL_TIMEOUT.getInt(info) * 1000;
          } else {
            LOGGER.debug("integer cancelSignalTimeout is too large, it will occur error after multiply by 1000.");
          }
          String socketAddress = newStream.getConnectInfo();
          LOGGER.info("[" + socketAddress + "] " + "Connection is established. ID: " + connectInfo);
          // Do final startup.
          QueryExecutor queryExecutor = new QueryExecutorImpl(newStream, user, database,
                  cancelSignalTimeout, info);
          queryExecutor.setProtocolVersion(this.protocolVerion);

          //Check MasterCluster or SecondaryCluster
          if (PGProperty.PRIORITY_SERVERS.get(info) != null) {
            ClusterStatus currentClusterStatus = queryClusterStatus(queryExecutor);
            //report cluster status
            GlobalClusterStatusTracker.reportClusterStatus(clusterSpec, currentClusterStatus);
            if (currentClusterStatus == ClusterStatus.MasterCluster) {
              isMasterCluster = true;
              //report the main cluster currently found
              GlobalClusterStatusTracker.reportMasterCluster(info, clusterSpec);
            } else {
              queryExecutor.close();
              ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
              break;
            }
          }

          // Check Master or Secondary
          HostStatus hostStatus = HostStatus.ConnectOK;
          if (candidateHost.targetServerType != HostRequirement.any) {
            hostStatus = isMaster(queryExecutor) ? HostStatus.Master : HostStatus.Secondary;
            LOGGER.info("Known status of host " + hostSpec + " is " + hostStatus);
            if (hostStatus == HostStatus.Master) {
              ClusterNodeCache.pushHostSpecs(hostSpec, currentHostSpecs, info);
            }
          }
          GlobalHostStatusTracker.reportHostStatus(hostSpec, hostStatus, info);
          knownStates.put(hostSpec, hostStatus);
          if (!candidateHost.targetServerType.allowConnectingTo(hostStatus)) {
            queryExecutor.close();
            ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
            continue;
          }

          // query and update statements cause logical replication to fail, temporarily evade
          if (info.getProperty("replication") == null) {
            runInitialQueries(queryExecutor, info);
            String queryGaussdbVersionResult = queryGaussdbVersion(queryExecutor);
            queryExecutor.setGaussdbVersion(queryGaussdbVersionResult);
            // get database compatibility mode
            queryExecutor.setCompatibilityMode(queryDataBaseDatcompatibility(queryExecutor, database));
          }
          if (MultiHostChooser.isUsingAutoLoadBalance(info)) {
            QueryCNListUtils.runRereshCNListQueryies(queryExecutor, info);
          }

          LOGGER.info("Connect complete. ID: " + connectInfo);
          // And we're done.
          return queryExecutor;
        } catch (ConnectException cex) {
          // Added by Peter Mount <peter@retep.org.uk>
          // ConnectException is thrown when the connection cannot be made.
          // we trap this an return a more meaningful message for the end user
          GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail, info);
          knownStates.put(hostSpec, HostStatus.ConnectFail);
          if (hostIter.hasNext() || clusterIter.hasNext()) {
            LOGGER.info("ConnectException occured while connecting to {0}" + hostSpec, cex);
            exception.addSuppressed(cex);
            ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
            // still more addresses to try
            continue;
          }
          if (exception.getSuppressed().length > 0) {
            cex.addSuppressed(exception);
          }
          ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
          throw new PSQLException(GT.tr(
                  "Connection to {0} refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.",
                  hostSpec), PSQLState.CONNECTION_UNABLE_TO_CONNECT, cex);
        } catch (IOException ioe) {
          closeStream(newStream);
          GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail, info);
          knownStates.put(hostSpec, HostStatus.ConnectFail);
          if (hostIter.hasNext() || clusterIter.hasNext()) {
            LOGGER.info("IOException occured while connecting to " + hostSpec, ioe);
            exception.addSuppressed(ioe);
            ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
            // still more addresses to try
            continue;
          }
          if (exception.getSuppressed().length > 0) {
            ioe.addSuppressed(exception);
          }
          ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
          throw new PSQLException(GT.tr("The connection attempt failed."),
                  PSQLState.CONNECTION_UNABLE_TO_CONNECT, ioe);
        } catch (SQLException se) {
          closeStream(newStream);
          GlobalHostStatusTracker.reportHostStatus(hostSpec, HostStatus.ConnectFail, info);
          knownStates.put(hostSpec, HostStatus.ConnectFail);
          if (hostIter.hasNext() || clusterIter.hasNext()) {
            LOGGER.info("SQLException occured while connecting to " + hostSpec, se);
            exception.addSuppressed(se);
            // still more addresses to try
            ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
            continue;
          }
          if (exception.getSuppressed().length > 0) {
            se.addSuppressed(exception);
          }
          ConnectionManager.getInstance().decrementCachedCreatingConnectionSize(hostSpec, info);
          throw se;
        }
      }
      //When the cluster is a production cluster and there is no exception, still unable to return connection, throw an exception
      if (isMasterCluster) {
        LOGGER.info("Could not find a server with specified targetServerType: " + targetServerType + ". The current server known status is: " + knownStates.entrySet().toString());
        throw new PSQLException(GT
                .tr("Could not find a server with specified targetServerType: {0}", targetServerType),
                PSQLState.CONNECTION_UNABLE_TO_CONNECT);
      } else {
        //update clusterStatus
        GlobalClusterStatusTracker.reportClusterStatus(clusterSpec, ClusterStatus.ConnectFail);
      }
    }

    if (PGProperty.PRIORITY_SERVERS.get(info) != null) {
      LOGGER.info("Could not find production cluster");
      throw new PSQLException(GT.tr("Could not find production cluster"), PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    } else {
      LOGGER.info("Could not find a server with specified targetServerType: " + targetServerType + ". The current server known status is: " + knownStates.entrySet().toString());
      throw new PSQLException(GT
              .tr("Could not find a server with specified targetServerType: {0}", targetServerType),
              PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    }

  }

  private List<String[]> getParametersForStartup(String user, String database, Properties info) {
    List<String[]> paramList = new ArrayList<String[]>();
    paramList.add(new String[]{"user", user});
    paramList.add(new String[]{"database", database});
    paramList.add(new String[]{"client_encoding", CLIENT_ENCODING});
    paramList.add(new String[]{"DateStyle", "ISO"});
    paramList.add(new String[]{"TimeZone", createPostgresTimeZone()});

    Version assumeVersion = ServerVersion.from(PGProperty.ASSUME_MIN_SERVER_VERSION.get(info));

    if (assumeVersion.getVersionNum() >= ServerVersion.v9_0.getVersionNum()) {
      // User is explicitly telling us this is a 9.0+ server so set properties here:
      paramList.add(new String[]{"extra_float_digits", "3"});
      String appName = PGProperty.APPLICATION_NAME.get(info);
      if (appName != null) {
        paramList.add(new String[]{"application_name", appName});
      }
    } else {
      // User has not explicitly told us that this is a 9.0+ server so stick to old default:
      paramList.add(new String[]{"extra_float_digits", "2"});
    }

    String replication = PGProperty.REPLICATION.get(info);
    if (replication != null && assumeVersion.getVersionNum() >= ServerVersion.v9_4.getVersionNum()) {
      paramList.add(new String[]{"replication", replication});
    }

    String currentSchema = PGProperty.CURRENT_SCHEMA.get(info);
    if (currentSchema != null) {
      paramList.add(new String[]{"search_path", currentSchema});
    }

    if (PGProperty.PG_CLIENT_LOGIC.get(info) != null && PGProperty.PG_CLIENT_LOGIC.get(info).equals("1")) {
      paramList.add(new String[]{"enable_full_encryption", "1"});
    }
    return paramList;
  }

  /**
   * Convert Java time zone to postgres time zone. All others stay the same except that GMT+nn
   * changes to GMT-nn and vise versa.
   *
   * @return The current JVM time zone in postgresql format.
   */
  private static String createPostgresTimeZone() {
    String tz = TimeZone.getDefault().getID();
    if (tz.length() <= 3 || !tz.startsWith("GMT")) {
      return tz;
    }
    char sign = tz.charAt(3);
    String start;
    switch (sign) {
      case '+':
        start = "GMT-";
        break;
      case '-':
        start = "GMT+";
        break;
      default:
        // unknown type
        return tz;
    }

    return start + tz.substring(4);
  }

  private PGStream enableSSL(PGStream pgStream, SslMode sslMode, Properties info,
      int connectTimeout)
      throws IOException, PSQLException {
    if (sslMode == SslMode.DISABLE) {
      return pgStream;
    }
    if (sslMode == SslMode.ALLOW) {
      // Allow ==> start with plaintext, use encryption if required by server
      return pgStream;
    }

    LOGGER.trace(" FE=> SSLRequest");

    // Send SSL request packet
    pgStream.sendInteger4(8);
    pgStream.sendInteger2(1234);
    pgStream.sendInteger2(5679);
    pgStream.flush();

    // Now get the response from the backend, one of N, E, S.
    int beresp = pgStream.receiveChar();
    switch (beresp) {
      case 'E':
        LOGGER.trace(" <=BE SSLError");

        // Server doesn't even know about the SSL handshake protocol
        if (sslMode.requireEncryption()) {
          throw new PSQLException(GT.tr("The server does not support SSL."),
              PSQLState.CONNECTION_REJECTED);
        }

        // We have to reconnect to continue.
        pgStream.close();
        return new PGStream(pgStream.getSocketFactory(), pgStream.getHostSpec(), connectTimeout);

      case 'N':
        LOGGER.trace(" <=BE SSLRefused");

        // Server does not support ssl
        if (sslMode.requireEncryption()) {
          throw new PSQLException(GT.tr("The server does not support SSL."),
              PSQLState.CONNECTION_REJECTED);
        }

        return pgStream;

      case 'S':
        LOGGER.trace(" <=BE SSLOk");

        // Server supports ssl
        org.postgresql.ssl.MakeSSL.convert(pgStream, info);
        return pgStream;

      default:
        throw new PSQLException(GT.tr("An error occured while setting up the SSL connection."),
            PSQLState.PROTOCOL_VIOLATION);
    }
  }

  private void sendStartupPacket(PGStream pgStream, List<String[]> params)
      throws IOException {
    if (LOGGER.isDebugEnabled()) {
      StringBuilder details = new StringBuilder();
      for (int i = 0; i < params.size(); ++i) {
        if (i != 0) {
          details.append(", ");
        }
        details.append(params.get(i)[0]);
        details.append("=");
        details.append(params.get(i)[1]);
      }
      LOGGER.debug("[" + connectInfo + "] " + " FE=> StartupPacket(" + details + ")");
    }

    // Precalculate message length and encode params.
    int length = 4 + 4;
    byte[][] encodedParams = new byte[params.size() * 2][];
    for (int i = 0; i < params.size(); ++i) {
      encodedParams[i * 2] = params.get(i)[0].getBytes("UTF-8");
      encodedParams[i * 2 + 1] = params.get(i)[1].getBytes("UTF-8");
      length += encodedParams[i * 2].length + 1 + encodedParams[i * 2 + 1].length + 1;
    }

    length += 1; // Terminating \0

    // Send the startup message.
    pgStream.sendInteger4(length);
    pgStream.sendInteger2(3); // protocol major
    if(this.protocolVerion < PROTOCOL_VERSION_350)
        pgStream.sendInteger2(0); // protocol minor
    else if(this.protocolVerion == PROTOCOL_VERSION_350)
        pgStream.sendInteger2(50); // protocol minor
	else if(this.protocolVerion == PROTOCOL_VERSION_351)
		pgStream.sendInteger2(51); // protocol minor
    for (byte[] encodedParam : encodedParams) {
      pgStream.send(encodedParam);
      pgStream.sendChar(0);
    }

    pgStream.sendChar(0);
    pgStream.flush();
  }

  private void doAuthentication(PGStream pgStream, String host, String user, Properties info) throws IOException, SQLException {
    // Now get the response from the backend, either an error message
    // or an authentication request

    String password = PGProperty.PASSWORD.get(info);

    /* SCRAM authentication state, if used */
    //#endif
      authloop: while (true) {
        int beresp = pgStream.receiveChar();

        switch (beresp) {
          case 'E':
            // An error occurred, so pass the error message to the
            // user.
            //
            // The most common one to be thrown here is:
            // "User authentication failed"
            //
            int l_elen = pgStream.receiveInteger4();

            ServerErrorMessage errorMsg =
                new ServerErrorMessage(pgStream.receiveErrorString(l_elen - 4), pgStream.getConnectInfo());
            LOGGER.trace("[" + connectInfo + "] " + " <=BE ErrorMessage(" + errorMsg + ")");
            throw new PSQLException(errorMsg);

          case 'R':
            // Authentication request.
            // Get the message length
            pgStream.receiveInteger4();

            // Get the type of request
            int areq = pgStream.receiveInteger4();

            // Process the request.
            switch (areq) {
              case AUTH_REQ_MD5: {
                byte[] md5Salt = pgStream.receive(4);
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("[" + connectInfo + "] " + " <=BE AuthenticationReqMD5(salt=" + Utils.toHexString(md5Salt) + ")");
                }

                if (password == null) {
                  throw new PSQLException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      PSQLState.CONNECTION_REJECTED);
                }

                byte[] digest =
                    MD5Digest.encode(user.getBytes("UTF-8"), password.getBytes("UTF-8"), md5Salt);

                pgStream.sendChar('p');
                pgStream.sendInteger4(4 + digest.length + 1);
                pgStream.send(digest);
                pgStream.sendChar(0);
                pgStream.flush();

                break;
              }
              case AUTH_REQ_MD5_SHA256: {
                  LOGGER.trace("AUTH_REQ_MD5_SHA256" + " ID: " + connectInfo);
                  byte[] digest;
                  String random64code = pgStream.receiveString(64);
                  byte[] md5Salt = pgStream.receive(4);
                  digest = MD5Digest.MD5_SHA256encode(password, random64code, md5Salt);
                  pgStream.sendChar('p');
                  pgStream.sendInteger4(4 + digest.length + 1);
                  pgStream.send(digest);
                  pgStream.sendChar(0);
                  pgStream.flush();

                  break;
              }
                case AUTH_REQ_SM3: {
                    LOGGER.trace("[" + connectInfo + "] " + "AUTH_REQ_SM3");
                    int passwordStoredMethod = pgStream.receiveInteger4();
                    if (password == null)
                        throw new PSQLException(
                              GT.tr(
                                      "The server requested password-based authentication, but no password"
                                          + " was provided."),
                              PSQLState.CONNECTION_REJECTED);
                    if (passwordStoredMethod == SM3_PASSWORD) {
                        String random64code = pgStream.receiveString(64);
                        String token = pgStream.receiveString(8);
                        int serverIteration = pgStream.receiveInteger4();
                        byte[] result = null;
                        result = MD5Digest.RFC5802Algorithm(password, random64code, token, null, serverIteration, false);
                        if (result == null)
                            throw new PSQLException(
                                    GT.tr("Invalid username/password,login denied."),
                                    PSQLState.CONNECTION_REJECTED);
                        pgStream.sendChar('p');
                        pgStream.sendInteger4(4 + result.length + 1);
                        pgStream.send(result);
                        pgStream.sendChar(0);
                        pgStream.flush();
                        break;
                    } else {
                        throw new PSQLException(
                                GT.tr(
                                        "The password-stored method is not supported, must be md5, "
                                            + "sha256 or sm3."),
                                PSQLState.CONNECTION_REJECTED);
                    }
                }
              case AUTH_REQ_SHA256: {
                  LOGGER.trace("[" + connectInfo + "] " + "AUTH_REQ_SHA256");
                  byte[] digest;
                  int passwordStoredMethod = pgStream.receiveInteger4();
                  if (password == null)
                      throw new PSQLException(
                              GT.tr(
                                      "The server requested password-based authentication, but no password"
                                          + " was provided."),
                              PSQLState.CONNECTION_REJECTED);
                  if (passwordStoredMethod == PLAIN_PASSWORD || passwordStoredMethod == SHA256_PASSWORD) {
                      String random64code = pgStream.receiveString(64);
                      String token = pgStream.receiveString(8);
                      byte[] result = null;
                      if (this.protocolVerion < PROTOCOL_VERSION_350) {
                          String server_signature = pgStream.receiveString(64);
                          int server_iteration_350 = 2048;
                          result =
                                  MD5Digest.RFC5802Algorithm(
                                          password,
                                          random64code,
                                          token,
                                          server_signature,
                                          server_iteration_350,
                                          true);
                      } else if (this.protocolVerion == PROTOCOL_VERSION_350) {
                          result = MD5Digest.RFC5802Algorithm(password, random64code, token);
                      } else {
                          int server_iteration = pgStream.receiveInteger4();
                          result =
                                  MD5Digest.RFC5802Algorithm(password, random64code, token, server_iteration);
                      }
                      if (result == null)
                          throw new PSQLException(
                                  GT.tr("Invalid username/password,login denied."),
                                  PSQLState.CONNECTION_REJECTED);
                      pgStream.sendChar('p');
                      pgStream.sendInteger4(4 + result.length + 1);
                      pgStream.send(result);
                      pgStream.sendChar(0);
                      pgStream.flush();
                      break;
                  } else if (passwordStoredMethod == MD5_PASSWORD) {
                      byte[] md5Salt = pgStream.receive(4);
                      digest =
                              MD5Digest.SHA256_MD5encode(
                                      user.getBytes("UTF-8"), password.getBytes("UTF-8"), md5Salt);
                  } else {
                      throw new PSQLException(
                              GT.tr(
                                      "The password-stored method is not supported, must be md5, "
                                          + "sha256 or sm3."),
                              PSQLState.CONNECTION_REJECTED);
                  }
                  pgStream.sendChar('p');
                  pgStream.sendInteger4(4 + digest.length + 1);
                  pgStream.send(digest);
                  pgStream.sendChar(0);
                  pgStream.flush();
                  break;
              }


              case AUTH_REQ_PASSWORD: {
                LOGGER.debug("[" + connectInfo + "] " + "<=BE AuthenticationReqPassword" + " ID: " + connectInfo);

                if (password == null) {
                  throw new PSQLException(
                      GT.tr(
                          "The server requested password-based authentication, but no password was provided."),
                      PSQLState.CONNECTION_REJECTED);
                }

                byte[] encodedPassword = password.getBytes("UTF-8");

                pgStream.sendChar('p');
                pgStream.sendInteger4(4 + encodedPassword.length + 1);
                pgStream.send(encodedPassword);
                pgStream.sendChar(0);
                pgStream.flush();

                break;
              }

              case AUTH_REQ_GSS:
              case AUTH_REQ_SSPI:
                  if(areq==AUTH_REQ_GSS){
                      String kerberosServerHostname= info.getProperty("kerberosServerHostname");
                      if(kerberosServerHostname!=null && kerberosServerHostname.length()!=0){
                          System.setProperty("kerberosServerHostname",kerberosServerHostname);
                      }
                  }
                  org.postgresql.gss.MakeGSS.authenticate(pgStream, host, user, password,
                          PGProperty.JAAS_APPLICATION_NAME.get(info),
                          PGProperty.KERBEROS_SERVER_NAME.get(info), PGProperty.USE_SPNEGO.getBoolean(info),
                          PGProperty.JAAS_LOGIN.getBoolean(info));
                  if (LOGGER.isDebugEnabled()) {
                    if (areq == AUTH_REQ_GSS) {
                        LOGGER.debug("[" + connectInfo + "] " + "AUTH_REQ_GSS");
                    } else {
                        LOGGER.debug("[" + connectInfo + "] " + "AUTH_REQ_SSPI");
                    }
                  }
                  break;

              case AUTH_REQ_OK:
                /* Cleanup after successful authentication */
                LOGGER.debug("[" + connectInfo + "] " + " <=BE AuthenticationOk");
                break authloop; // We're done.

              default:
                LOGGER.trace("[" + connectInfo + "] " + " <=BE AuthenticationReq (unsupported type " + areq + ")");
                throw new PSQLException(GT.tr(
                    "The authentication type {0} is not supported. Check that you have configured the pg_hba.conf file to include the client''s IP address or subnet, and that it is using an authentication scheme supported by the driver.",
                    areq), PSQLState.CONNECTION_REJECTED);
            }

            break;

          default:
            throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
                PSQLState.PROTOCOL_VIOLATION);
        }
      }


  }

  private void runInitialQueries(QueryExecutor queryExecutor, Properties info)
      throws SQLException {
    String assumeMinServerVersion = PGProperty.ASSUME_MIN_SERVER_VERSION.get(info);
    if (Utils.parseServerVersionStr(assumeMinServerVersion) >= ServerVersion.v9_0.getVersionNum()) {
      // We already sent the parameter values in the StartupMessage so skip this
      return;
    }

    final int dbVersion = queryExecutor.getServerVersionNum();

    if (dbVersion >= ServerVersion.v9_0.getVersionNum()) {
      String setSql = "SET extra_float_digits = 3;set client_encoding = '"+CLIENT_ENCODING+"';";
      SetupQueryRunner.run(queryExecutor, setSql, false);
    }

    String appName = PGProperty.APPLICATION_NAME.get(info);
    if (appName != null && dbVersion >= ServerVersion.v9_0.getVersionNum()) {
      StringBuilder sql = new StringBuilder();
      sql.append("SET application_name = '");
      Utils.escapeLiteral(sql, appName, queryExecutor.getStandardConformingStrings());
      sql.append("'");
      SetupQueryRunner.run(queryExecutor, sql.toString(), false);
    }

    String appType = PGProperty.APPLICATION_TYPE.get(info);
    if (appType !=null && !appType.equals(queryExecutor.getApplicationType())) {
      StringBuilder sql = new StringBuilder();
      sql.append("SET application_type = '");
      Utils.escapeLiteral(sql, appType, queryExecutor.getStandardConformingStrings());
      sql.append("'");
      SetupQueryRunner.run(queryExecutor, sql.toString(), false);
    }
  }

  public boolean isMaster(QueryExecutor queryExecutor) throws SQLException, IOException {
    String localRole = "";
    String dbState = "";
    List<byte[][]> results = SetupQueryRunner.runForList(queryExecutor, "select local_role, db_state from pg_stat_get_stream_replications();", true);
    for (byte[][] result : results) {
      localRole = queryExecutor.getEncoding().decode(result[0]);
      dbState = queryExecutor.getEncoding().decode(result[1]);
    }
    return localRole.equalsIgnoreCase("Primary") && dbState.equalsIgnoreCase("Normal");
  }

  private String queryDataBaseDatcompatibility(QueryExecutor queryExecutor, String database) throws SQLException,
          IOException {
    byte[][] result = SetupQueryRunner.run(queryExecutor, "select datcompatibility from pg_database where " +
            "datname='" + database + "';", true);
    String datcompatibility = queryExecutor.getEncoding().decode(result[0]);
    return datcompatibility == null ? "PG" : datcompatibility;
  }


  private String queryGaussdbVersion(QueryExecutor queryExecutor) throws SQLException, IOException {
    byte[][] result = SetupQueryRunner.run(queryExecutor, "select version();", true);
    String version = queryExecutor.getEncoding().decode(result[0]);
    if (version != null && version.contains("GaussDB Kernel")) {
      return "GaussDBKernel";
    } else if (version != null && version.contains("openGauss")) {
      return "openGauss";
    } else {
      return "";
    }
  }

  private ClusterStatus queryClusterStatus(QueryExecutor queryExecutor) throws SQLException, IOException {
    byte[][] result = SetupQueryRunner.run(queryExecutor, "select barrier_id from gs_get_local_barrier_status();", true);
    String barrierId = queryExecutor.getEncoding().decode(result[0]);
    if(barrierId != null && !barrierId.equals("")){
      return ClusterStatus.SecondaryCluster;
    }else{
      return ClusterStatus.MasterCluster;
    }
  }
}
