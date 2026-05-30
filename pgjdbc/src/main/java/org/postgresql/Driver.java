/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql;

import org.postgresql.clusterchooser.GlobalClusterStatusTracker;
import org.postgresql.hostchooser.MultiHostChooser;
import org.postgresql.jdbc.ORConnection;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.log.Logger;
import org.postgresql.log.Log;
import org.postgresql.log.Tracer;
import org.postgresql.quickautobalance.ConnectionManager;
import org.postgresql.quickautobalance.LoadBalanceHeartBeating;
import org.postgresql.readwritesplitting.ReadWriteSplittingPgConnection;
import org.postgresql.util.DriverInfo;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.SharedTimer;
import org.postgresql.util.URLCoder;
import org.postgresql.util.WriterHandler;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.regex.Pattern;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;


/**
 * <p>The Java SQL framework allows for multiple database drivers. Each driver should supply a class
 * that implements the Driver interface</p>
 *
 * <p>The DriverManager will try to load as many drivers as it can find and then for any given
 * connection request, it will ask each driver in turn to try to connect to the target URL.</p>
 *
 * <p>It is strongly recommended that each Driver class should be small and standalone so that the
 * Driver class can be loaded and queried without bringing in vast quantities of supporting code.</p>
 *
 * <p>When a Driver class is loaded, it should create an instance of itself and register it with the
 * DriverManager. This means that a user can load and register a driver by doing
 * Class.forName("foo.bah.Driver")</p>
 *
 * @see org.postgresql.PGConnection
 * @see java.sql.Driver
 */
public class Driver implements java.sql.Driver {

    private static Driver registeredDriver;
    private static final java.util.logging.Logger PARENT_LOGGER = java.util.logging.Logger.getLogger("org.postgresql");

    private static Log LOGGER = Logger.getLogger("org.postgresql.Driver");
    private static SharedTimer sharedTimer = new SharedTimer();
    private static final String DEFAULT_PORT =
            /*$"\""+mvn.project.property.template.default.pg.port+"\";"$*//*-*/"5432";
    // Remove iam certification
    // private static CloseableHttpClient client = null;
    private static final String gsVersion = "@GSVERSION@";
    /* generate one log file */
    public static AtomicBoolean isLogFileCreated;
    private static final String[] SENSITIVE_CHARACTERS = {"sslpassword", "iamPassword", "password"};
    private static final String UDS_HOST = "localhost";

    private static Tracer tracer;
    private static AtomicBoolean tracerInitialized = new AtomicBoolean(false);

    private static final String CERTIFICATE_TYPE_X509 = "X.509";
    private static final String SSL_PROTOCOL_TLS_V1_2 = "TLSv1.2";
    private static final String SSL_PROTOCOL_TLS_V1_3 = "TLSv1.3";
    private static final String[] ENABLED_TLS_PROTOCOLS = {
        SSL_PROTOCOL_TLS_V1_2,
        SSL_PROTOCOL_TLS_V1_3
    };

    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;
    private static final String IPV4_SEGMENT_PATTERN = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
    private static final String IPV4_PATTERN =
        IPV4_SEGMENT_PATTERN + "\\."
        + IPV4_SEGMENT_PATTERN + "\\."
        + IPV4_SEGMENT_PATTERN + "\\."
        + IPV4_SEGMENT_PATTERN;

    private static int randomATFIndex;

    static {
        try {
            // moved the registerDriver from the constructor to here
            // because some clients call the driver themselves (I know, as
            // my early jdbc work did - and that was based on other examples).
            // Placing it here, means that the driver is registered once only.
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }

        try {
            Class.forName("org.postgresql.util.GT");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Helper to retrieve default properties from classloader resource
    // properties files.
    private Properties defaultProperties;

    /**
     * Obtain the trace id from the implementation class.
     *
     * @return trace id
     */
    public static String getTracer() {
        if (tracer == null) {
            return null;
        }
        String traceId = tracer.getTraceId();
        if (traceId == null) {
            return null;
        } else if (traceId.length() > 32) {
            traceId = traceId.substring(0, 32);
            LOGGER.warn("When used link trace, the length of trace id should be less or equals than 32, currently " +
                    "truncated to " + traceId + ".");
        } else if (traceId.length() < 1) {
            LOGGER.warn("When used link trace, the length of trace id should be greater than 0.");
        }
        return traceId;
    }

    private synchronized Properties getDefaultProperties() throws IOException {
        if (defaultProperties != null) {
            return defaultProperties;
        }

        // Make sure we load properties with the maximum possible privileges.
        try {
            defaultProperties =
                    AccessController.doPrivileged(new PrivilegedExceptionAction<Properties>() {
                        public Properties run() throws IOException {
                            return loadDefaultProperties();
                        }
                    });
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }

        return defaultProperties;
    }

    private Properties loadDefaultProperties() throws IOException {
        Properties merged = new Properties();

        try {
            PGProperty.USER.set(merged, System.getProperty("user.name"));
        } catch (SecurityException se) {
            // We're just trying to set a default, so if we can't
            // it's not a big deal.
        }

        // If we are loaded by the bootstrap classloader, getClassLoader()
        // may return null. In that case, try to fall back to the system
        // classloader.
        //
        // We should not need to catch SecurityException here as we are
        // accessing either our own classloader, or the system classloader
        // when our classloader is null. The ClassLoader javadoc claims
        // neither case can throw SecurityException.
        ClassLoader cl = getClass().getClassLoader();
        if (cl == null) {
            LOGGER.debug("Can't find our classloader for the Driver; "
                    + "attempt to use the system class loader");
            cl = ClassLoader.getSystemClassLoader();
        }

        if (cl == null) {
            LOGGER.warn("Can't find a classloader for the Driver; not loading driver "
                    + "configuration from org/postgresql/driverconfig.properties");
            return merged; // Give up on finding defaults.
        }

        LOGGER.debug("Loading driver configuration via classloader " + cl);

        // When loading the driver config files we don't want settings found
        // in later files in the classpath to override settings specified in
        // earlier files. To do this we've got to read the returned
        // Enumeration into temporary storage.
        ArrayList<URL> urls = new ArrayList<URL>();
        Enumeration<URL> urlEnum = cl.getResources("org/postgresql/driverconfig.properties");
        while (urlEnum.hasMoreElements()) {
            urls.add(urlEnum.nextElement());
        }

        for (int i = urls.size() - 1; i >= 0; i--) {
            URL url = urls.get(i);
            LOGGER.debug("Loading driver configuration from: " + url);
            InputStream is = url.openStream();
            merged.load(is);
            is.close();
        }

        return merged;
    }

    public static Properties GetProps(Properties defaults, Properties info) throws PSQLException {
        Properties newProps;
        newProps = new Properties(defaults);

        if (info != null) {
            Set<String> e = info.stringPropertyNames();
            /* judge if each value of info is null */
            for (String propName : e) {
                String propValue = info.getProperty(propName);
                if (propValue == null) {
                    throw new PSQLException(
                            GT.tr("Properties for the driver contains a non-string value for the key ") + propName,
                            PSQLState.UNEXPECTED_ERROR);
                }
                /* if info has this value, intend this property and value into props */
                newProps.setProperty(propName, propValue);
            }
        }
        return newProps;
    }

    /**
     * <p>Try to make a database connection to the given URL. The driver should return "null" if it
     * realizes it is the wrong kind of driver to connect to the given URL. This will be common, as
     * when the JDBC driverManager is asked to connect to a given URL, it passes the URL to each
     * loaded driver in turn.</p>
     *
     * <p>The driver should raise an SQLException if it is the right driver to connect to the given URL,
     * but has trouble connecting to the database.</p>
     *
     * <p>The java.util.Properties argument can be used to pass arbitrary string tag/value pairs as
     * connection arguments.</p>
     *
     * <ul>
     * <li>user - (required) The user to connect as</li>
     * <li>password - (optional) The password for the user</li>
     * <li>ssl -(optional) Use SSL when connecting to the server</li>
     * <li>readOnly - (optional) Set connection to read-only by default</li>
     * <li>charSet - (optional) The character set to be used for converting to/from
     * the database to unicode. If multibyte is enabled on the server then the character set of the
     * database is used as the default, otherwise the jvm character encoding is used as the default.
     * This value is only used when connecting to a 7.2 or older server.</li>
     * <li>loglevel - (optional) Enable logging of messages from the driver. The value is an integer
     * from 0 to 2 where: OFF = 0, INFO =1, DEBUG = 2 The output is sent to
     * DriverManager.getPrintWriter() if set, otherwise it is sent to System.out.</li>
     * <li>compatible - (optional) This is used to toggle between different functionality
     * as it changes across different releases of the jdbc driver code. The values here are versions
     * of the jdbc client and not server versions. For example in 7.1 get/setBytes worked on
     * LargeObject values, in 7.2 these methods were changed to work on bytea values. This change in
     * functionality could be disabled by setting the compatible level to be "7.1", in which case the
     * driver will revert to the 7.1 functionality.</li>
     * </ul>
     *
     * <p>Normally, at least "user" and "password" properties should be included in the properties. For a
     * list of supported character encoding , see
     * http://java.sun.com/products/jdk/1.2/docs/guide/internat/encoding.doc.html Note that you will
     * probably want to have set up the Postgres database itself to use the same encoding, with the
     * {@code -E <encoding>} argument to createdb.</p>
     *
     * <p>Our protocol takes the forms:</p>
     *
     * <pre>
     *  jdbc:postgresql://host:port/database?param1=val1&amp;...
     * </pre>
     *
     * @param url  the URL of the database to connect to
     * @param info a list of arbitrary tag/value pairs as connection arguments
     * @return a connection to the URL or null if it isnt us
     * @throws SQLException if a database access error occurs
     * @see java.sql.Driver#connect
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // get defaults
        Properties defaults, props;

        String[] legalUrlTags = {"jdbc:postgresql:", "jdbc:dws:iam:", "jdbc:oGRAC:"};
        boolean isUrlLegal = false;
        boolean isOGRAC = false;
        for (String urlTag : legalUrlTags) {
            if (url.startsWith(urlTag)) {
                isUrlLegal = true;
                if (urlTag.equals("jdbc:oGRAC:")) {
                    isOGRAC = true;
                }
            }
        }
        if (!isUrlLegal) {
            return null;
        }

        try {
            defaults = getDefaultProperties();
        } catch (IOException ioe) {
            throw new PSQLException(GT.tr("Error loading default settings from driverconfig.properties"),
                    PSQLState.UNEXPECTED_ERROR, ioe);
        }

        props = GetProps(defaults, info);

        // parse URL and add more properties
        if ((props = parseURL(url, props)) == null) {
            return null;
        }

        if (!parseConnectionProperties(props, isOGRAC)) {
            return null;
        }

        try {
            LOGGER.debug("Connecting with URL: " + filterAuthenticationCredentials(url));

            // Enforce login timeout, if specified, by running the connection
            // attempt in a separate thread. If we hit the timeout without the
            // connection completing, we abandon the connection attempt in
            // the calling thread, but the separate thread will keep trying.
            // Eventually, the separate thread will either fail or complete
            // the connection; at that point we clean up the connection if
            // we managed to establish one after all. See ConnectThread for
            // more details.
            long timeout = timeout(props);
            if (timeout <= 0) {
                if (isOGRAC) {
                    return makeCTConnection(url, props);
                }
                Connection con = makeConnection(url, props);
                return con;
            }

            ConnectThread ct = new ConnectThread(url, props);
            Thread thread = new Thread(ct, "PostgreSQL JDBC driver connection thread");
            thread.setDaemon(true); // Don't prevent the VM from shutting down
            thread.start();
            return ct.getResult(timeout);
        } catch (PSQLException ex1) {
            LOGGER.debug("Connection error: ", ex1);
            // re-throw the exception, otherwise it will be caught next, and a
            // org.postgresql.unusual error will be returned instead.
            throw ex1;
        } catch (java.security.AccessControlException ace) {
            throw new PSQLException(GT.tr("Your security policy has prevented the connection from being attempted.  " +
                    "You probably need to grant the connect java.net.SocketPermission to the database server host and" +
                    " port that you wish to connect to."), PSQLState.UNEXPECTED_ERROR, ace);
        } catch (Exception ex2) {
            LOGGER.debug("Unexpected connection error: ", ex2);
            throw new PSQLException(GT.tr("Something unusual has occured to cause the driver to fail: {0}.",
                    ex2.getMessage()), PSQLState.UNEXPECTED_ERROR, ex2);
        }
    }

    /**
     * Parse the configuration items in the connection string.
     *
     * @param props Connection Properties
     * @param isOGRAC is OGRAC
     */
    private Boolean parseConnectionProperties(Properties props, boolean isOGRAC) {
        Logger.setLoggerName(props.getProperty("logger"));
        if (Logger.isUsingJDKLogger()) {
            setupLoggerFromProperties(props);
        } else {
            LOGGER = Logger.getLogger("org.postgresql.Driver");
        }

        Boolean parseStatus = true;
        if (PGProperty.PRIORITY_SERVERS.get(props) != null) {
            if (!GlobalClusterStatusTracker.isVaildPriorityServers(props)) {
                parseStatus = false;
            }
            GlobalClusterStatusTracker.refreshProperties(props);
        }

        if (MultiHostChooser.isUsingAutoLoadBalance(props) && !isOGRAC) {
            if (!MultiHostChooser.isVaildPriorityLoadBalance(props)) {
                parseStatus = false;
            }
            QueryCNListUtils.refreshProperties(props);
        }
        return parseStatus;
    }

    // Used to check if the handler file is the same
    private static String loggerHandlerFile;

    /**
     * <p>Setup java.util.logging.Logger using connection properties.</p>
     *
     * <p>See {@link PGProperty#LOGGER_FILE} and {@link PGProperty#LOGGER_FILE}</p>
     *
     * @param driverLogLevel Connection Properties
     */
    private Boolean initLoggerProperties(String driverLogLevel) {
        if (driverLogLevel == null) {
            return false; // Don't mess with Logger if not set
        }
        if (!"OFF".equalsIgnoreCase(driverLogLevel) && !isLogFileCreated.compareAndSet(false, true)) {
            return false;
        }
        if ("OFF".equalsIgnoreCase(driverLogLevel)) {
            PARENT_LOGGER.setLevel(Level.OFF);
            return false; // Don't mess with Logger if set to OFF
        } else if ("DEBUG".equalsIgnoreCase(driverLogLevel)) {
            PARENT_LOGGER.setLevel(Level.FINE);
        } else if ("TRACE".equalsIgnoreCase(driverLogLevel)) {
            PARENT_LOGGER.setLevel(Level.FINEST);
        } else if ("INFO".equalsIgnoreCase(driverLogLevel)) {
            PARENT_LOGGER.setLevel(Level.INFO);
        } else {
            PARENT_LOGGER.setLevel(Level.OFF);
        }
        return true;
    }

    /**
     * <p>Setup java.util.logging.Logger using connection properties.</p>
     *
     * <p>See {@link PGProperty#LOGGER_FILE} and {@link PGProperty#LOGGER_FILE}</p>
     *
     * @param props Connection Properties
     */
    private void setupLoggerFromProperties(final Properties props) {
        final String driverLogLevel = PGProperty.LOGGER_LEVEL.get(props);
        if (!initLoggerProperties(driverLogLevel)) {
            return;
        }

        final String driverLogFile = PGProperty.LOGGER_FILE.getDefaultValue();
        if (driverLogFile != null && driverLogFile.equals(loggerHandlerFile)) {
            return; // Same file output, do nothing.
        }

        for (java.util.logging.Handler handlers : PARENT_LOGGER.getHandlers()) {
            // Remove previously set Handlers
            handlers.close();
            PARENT_LOGGER.removeHandler(handlers);
            loggerHandlerFile = null;
        }

        java.util.logging.Handler handler = null;
        if (driverLogFile != null) {
            try {
                handler = new java.util.logging.FileHandler(driverLogFile);
                loggerHandlerFile = driverLogFile;
            } catch (Exception ex) {
                System.err.println("Cannot enable FileHandler, fallback to ConsoleHandler.");
            }
        }

        Formatter formatter = new SimpleFormatter();

        if (handler == null) {
            if (DriverManager.getLogWriter() != null) {
                handler = new WriterHandler(DriverManager.getLogWriter());
            } else if (DriverManager.getLogStream() != null) {
                handler = new StreamHandler(DriverManager.getLogStream(), formatter);
            } else {
                handler = new StreamHandler(System.err, formatter);
            }
        } else {
            handler.setFormatter(formatter);
        }

        handler.setLevel(PARENT_LOGGER.getLevel());
        PARENT_LOGGER.setUseParentHandlers(false);
        PARENT_LOGGER.addHandler(handler);
    }

    /**
     * Perform a connect in a separate thread; supports getting the results from the original thread
     * while enforcing a login timeout.
     */
    private static class ConnectThread implements Runnable {
        ConnectThread(String url, Properties props) {
            this.url = url;
            this.props = props;
        }

        public void run() {
            Connection conn;
            Throwable error;

            try {
                conn = makeConnection(url, props);
                error = null;
            } catch (Throwable t) {
                conn = null;
                error = t;
            }

            synchronized (this) {
                if (abandoned) {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                        }
                    }
                } else {
                    result = conn;
                    resultException = error;
                    notify();
                }
            }
        }

        /**
         * Get the connection result from this (assumed running) thread. If the timeout is reached
         * without a result being available, a SQLException is thrown.
         *
         * @param timeout timeout in milliseconds
         * @return the new connection, if successful
         * @throws SQLException if a connection error occurs or the timeout is reached
         */
        public Connection getResult(long timeout) throws SQLException {
            long expiry = System.currentTimeMillis() + timeout;
            synchronized (this) {
                while (true) {
                    if (result != null) {
                        return result;
                    }

                    if (resultException != null) {
                        if (resultException instanceof SQLException) {
                            resultException.fillInStackTrace();
                            throw (SQLException) resultException;
                        } else {
                            throw new PSQLException(
                                    GT.tr(
                                            "Something unusual has occured to cause the driver to fail. Please report" +
                                                    " this exception."),
                                    PSQLState.UNEXPECTED_ERROR, resultException);
                        }
                    }

                    long delay = expiry - System.currentTimeMillis();
                    if (delay <= 0) {
                        abandoned = true;
                        throw new PSQLException(GT.tr("Connection attempt timed out."),
                                PSQLState.CONNECTION_UNABLE_TO_CONNECT);
                    }

                    try {
                        wait(delay);
                    } catch (InterruptedException ie) {

                        // reset the interrupt flag
                        Thread.currentThread().interrupt();
                        abandoned = true;

                        // throw an unchecked exception which will hopefully not be ignored by the calling code
                        throw new RuntimeException(GT.tr("Interrupted while attempting to connect."));
                    }
                }
            }
        }

        private final String url;
        private final Properties props;
        private Connection result;
        private Throwable resultException;
        private boolean abandoned;
    }

    /**
     * Create a connection from URL and properties. Always does the connection work in the current
     * thread without enforcing a timeout, regardless of any timeout specified in the properties.
     *
     * @param url   the original URL
     * @param props the parsed/defaulted connection properties
     * @return a new connection
     * @throws SQLException if the connection could not be made
     */
    private static Connection makeConnection(String url, Properties props) throws SQLException {
        ConnectionManager.getInstance().setCluster(props);
        if (PGProperty.ENABLE_STATEMENT_LOAD_BALANCE.getBoolean(props)) {
            return new ReadWriteSplittingPgConnection(hostSpecs(props), props, user(props), database(props), url);
        }
        PgConnection pgConnection;
        if (PGProperty.ATF_LEVEL.get(props).equals("U")) {
            ArrayList<ATFAddress> addressesATF = parseATFURL(props);
            SSLSocket socketATF = makeConnectionToATF(addressesATF, PGProperty.ATF_TIMEOUT.getInt(props));
            pgConnection = new PgConnection(hostSpecs(props), user(props), database(props), props,
                url, socketATF, addressesATF);
        } else {
            pgConnection = new PgConnection(hostSpecs(props), user(props), database(props), props, url);
        }
        GlobalConnectionTracker.possessConnectionReference(pgConnection.getQueryExecutor(), props);
        LoadBalanceHeartBeating.setConnection(pgConnection, props);
        return pgConnection;
    }

    /**
     * class to record ATFAddress
     */
    public static class ATFAddress {
        private String host;
        private int port;
        private String sslCert;

        public ATFAddress(String host, int port, String sslCert) {
            this.host = host;
            this.port = port;
            this.sslCert = sslCert;
        }

        public String getATFHost() {
            return host;
        }
        public void setATFHost(String host) {
            this.host = host;
        }
        public int getATFPort() {
            return port;
        }
        public void setATFPort(int port) {
            this.port = port;
        }
        public String getSslCert() {
            return sslCert;
        }
        public void setSslCert(String sslCert) {
            this.sslCert = sslCert;
        }
    }

    /**
     * @param addressesATF an ATFaddress List
     * @return a suitable socket to ATF
     */
    public static SSLSocket makeConnectionToATF(ArrayList<ATFAddress> addressesATF, int timeoutMs) throws SQLException {
        // choose an ATFAddress
        if (addressesATF == null) {
            throw new SQLException("ATFAddress List is null.");
        }
        int pickedATF = pickATFAddress(addressesATF);
        SSLSocketFactory sslSocketFactory = initSSLSocket(addressesATF.get(pickedATF).getSslCert());
        // connect to atf server
        return connectToATF(addressesATF, pickedATF,
                            sslSocketFactory, timeoutMs);
    }

    private static SSLSocketFactory initSSLSocket(String certPath) throws SQLException {
        SSLSocketFactory sslSocketFactory;
        try (FileInputStream fis = new FileInputStream(certPath)) {
            CertificateFactory cf = CertificateFactory.getInstance(CERTIFICATE_TYPE_X509);
            Certificate certObj = cf.generateCertificate(fis);
            if (certObj instanceof X509Certificate) {
                X509Certificate cert = (X509Certificate) certObj;
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStore.load(null);
                trustStore.setCertificateEntry("atf-server", cert);

                TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm()
                );
                tmf.init(trustStore);
                SSLContext sslContext = SSLContext.getInstance(SSL_PROTOCOL_TLS_V1_2);
                sslContext.init(null, tmf.getTrustManagers(), null);
                sslSocketFactory = sslContext.getSocketFactory();
            } else {
                throw new IllegalArgumentException("ATF cert type wrong.");
            }
        } catch (CertificateException | IOException | KeyStoreException
                 | NoSuchAlgorithmException | KeyManagementException e) {
            throw new SQLException("SSL fail to initialize（cert path：" + certPath + "）", e);
        }
        return sslSocketFactory;
    }

    private static SSLSocket connectToATF(ArrayList<ATFAddress> addressesATF, int randomIndex,
        SSLSocketFactory sslSocketFactory, int timeoutMs) throws SQLException {
        IOException lastException = null;
        int pickedATF = randomIndex;
        for (int i = 0; i < addressesATF.size(); i++) {
            SSLSocket sslSocket = null;
            Socket socketObj = null;
            try {
                socketObj = sslSocketFactory.createSocket();
                if (socketObj instanceof SSLSocket) {
                    sslSocket = (SSLSocket) socketObj;
                    sslSocket.setEnabledProtocols(ENABLED_TLS_PROTOCOLS);
                    sslSocket.connect(
                        new InetSocketAddress(
                            addressesATF.get(pickedATF).getATFHost(),
                            addressesATF.get(pickedATF).getATFPort()
                        ),
                        timeoutMs
                    );
                } else {
                    throw new IllegalArgumentException("Not SSLSocket.");
                }
                sslSocket.startHandshake();
                return sslSocket;
            } catch (IOException e) {
                lastException = e;
                closeSSLSocket(socketObj);
                pickedATF = (pickedATF + 1) % addressesATF.size();
            }
        }
        throw new SQLException("Fail to connect to any ATFServer", lastException);
    }

    private static void closeSSLSocket(Socket socketObj) throws SQLException {
        try {
            if (socketObj != null) {
                socketObj.close();
            }
        } catch (IOException closeEx) {
            throw new SQLException("socketObj close faliure");
        }
    }

    /**
     * @param props properties
     * @return the list of structured ATFURL
     * @throws SQLException exception when connecting to Database
     */
    public static ArrayList<ATFAddress> parseATFURL(Properties props) throws SQLException {
        String url = PGProperty.ATF_ADDRESS.get(props).trim();
        String sslCert = PGProperty.ATF_SSLCERT.get(props).trim();
        if (url == null || url.isEmpty()) {
            throw new SQLException("ATFAddress wrong.");
        }
        if (sslCert == null || sslCert.isEmpty()) {
            throw new SQLException("ATFSslcertpath wrong.");
        }
        // split the url and the sslcertpath into individual ones
        String[] urlStrings = url.split(",");
        String[] sslcertpaths = sslCert.split(",");
        if (urlStrings.length != sslcertpaths.length) {
            throw new SQLException("less or more ATFsslcertpath.");
        }
        ArrayList<String> validParts = new ArrayList<>();
        for (String part : urlStrings) {
            part = part.trim();
            if (!part.isEmpty()) {
                validParts.add(part);
            }
        }
        if (validParts.isEmpty()) {
            throw new SQLException("ATFAddress wrong");
        }
        // parse url into list
        ArrayList<ATFAddress> addressesATF = new ArrayList<>();
        for (int i = 0; i < validParts.size(); i++) {
            String item = validParts.get(i);
            int colonIndex = item.lastIndexOf(':');
            if (colonIndex == -1) {
                throw new SQLException("ATFAddresses parse wrong.");
            }
            if (item.indexOf(':', 0) != colonIndex) {
                throw new SQLException("ATFAddresses parse wrong.");
            }
            String host = item.substring(0, colonIndex);
            String portStr = item.substring(colonIndex + 1);
            int port;
            try {
                port = Integer.parseInt(portStr);
                if (!isValidHost(host) || port < MIN_PORT || port > MAX_PORT) {
                    throw new SQLException("Invalid ATFAddress");
                }
                addressesATF.add(new ATFAddress(host, port, sslcertpaths[i]));
            } catch (NumberFormatException e) {
                throw new SQLException("Invaild port");
            }
        }
        return addressesATF;
    }

    /**
     * @param host host for checking
     * @return is a Valid host for IPV4
     */
    public static boolean isValidHost(String host) {
        // IPV4 check
        if (host != null && host.matches(IPV4_PATTERN)) {
            return true;
        }
        // Check whether it is a valid hostname
        if (host != null && host.matches("^[a-zA-Z][a-zA-Z0-9\\-]*(\\.[a-zA-Z0-9][a-zA-Z0-9\\-]*)*$")) {
            return true;
        }
        if ("localhost".equalsIgnoreCase(host)) {
            return true;
        }
        return false;
    }

    /**
     * @param addressesATF an url list of ATF for connection
     * @return a suitable atfaddress
     */
    public static int pickATFAddress(ArrayList<ATFAddress> addressesATF) {
        randomATFIndex = new SecureRandom().nextInt(addressesATF.size());
        return randomATFIndex;
    }

    /**
     * Create a connection from URL and properties with oGRAC. Always does the connection work in the current
     * thread without enforcing a timeout, regardless of any timeout specified in the properties.
     *
     * @param url   the original URL
     * @param props the parsed/defaulted connection properties
     * @return a new connection
     * @throws SQLException if the connection could not be made
     */
    private static Connection makeCTConnection(String url, Properties props) throws SQLException, IOException {
        String simpleUrl = url;
        int addressEnd = url.lastIndexOf("?");
        if (addressEnd > 0) {
            simpleUrl = url.substring(0, addressEnd);
        }
        ConnectionManager.getInstance().setCluster(props);
        return new ORConnection(hostSpecs(props), user(props), props, simpleUrl);
    }

    /**
     * Returns true if the driver thinks it can open a connection to the given URL. Typically, drivers
     * will return true if they understand the subprotocol specified in the URL and false if they
     * don't. Our protocols start with jdbc:postgresql:
     *
     * @param url the URL of the driver
     * @return true if this driver accepts the given URL
     * @throws PSQLException
     * @see java.sql.Driver#acceptsURL
     */
    @Override
    public boolean acceptsURL(String url) throws PSQLException {
        return parseURL(url, null) != null;
    }

    /**
     * <p>The getPropertyInfo method is intended to allow a generic GUI tool to discover what properties
     * it should prompt a human for in order to get enough information to connect to a database.</p>
     *
     * <p>Note that depending on the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate through several calls to getPropertyInfo</p>
     *
     * @param url  the Url of the database to connect to
     * @param info a proposed list of tag/value pairs that will be sent on connect open.
     * @return An array of DriverPropertyInfo objects describing possible properties. This array may
     * be an empty array if no properties are required
     * @throws PSQLException
     * @see java.sql.Driver#getPropertyInfo
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws PSQLException {
        Properties copy = new Properties(info);
        Properties parse = parseURL(url, copy);
        if (parse != null) {
            copy = parse;
        }

        PGProperty[] knownProperties = PGProperty.values();
        DriverPropertyInfo[] props = new DriverPropertyInfo[knownProperties.length];
        for (int i = 0; i < props.length; ++i) {
            props[i] = knownProperties[i].toDriverPropertyInfo(copy);
        }

        return props;
    }

    @Override
    public int getMajorVersion() {
        return org.postgresql.util.DriverInfo.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return org.postgresql.util.DriverInfo.MINOR_VERSION;
    }

    /**
     * Returns the server version series of this driver and the specific build number.
     *
     * @return JDBC driver version
     * @deprecated use {@link #getMajorVersion()} and {@link #getMinorVersion()} instead
     */
    @Deprecated
    public static String getVersion() {
        return DriverInfo.DRIVER_FULL_NAME;
    }

    /**
     * <p>Report whether the driver is a genuine JDBC compliant driver. A driver may only report "true"
     * here if it passes the JDBC compliance tests, otherwise it is required to return false. JDBC
     * compliance requires full support for the JDBC API and full support for SQL 92 Entry Level.</p>
     *
     * <p>For PostgreSQL, this is not yet possible, as we are not SQL92 compliant (yet).</p>
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public static String parseIPValid(String address) {
        if (Objects.equals(address, "") || address == null) {
            return address;
        }
        String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
        Pattern pattern = Pattern.compile(regex);
        if (!pattern.matcher(address).matches()) {
            LOGGER.debug("JDBC URL invalid ip address: " + address);
        }
        return address;
    }

    /**
     * Constructs a new DriverURL, splitting the specified URL into its component parts.
     *
     * @param url      JDBC URL to parse
     * @param defaults Default properties
     * @return Properties with elements added from the url
     * @throws PSQLException
     */
    public static Properties parseURL(String url, Properties defaults) throws PSQLException {
        Properties urlProps = new Properties(defaults);
        String l_urlServer = url;
        String l_urlArgs = "";
        boolean isUrlLegal = false;
        String[] legalUrlTags = {"jdbc:postgresql:", "jdbc:dws:iam:", "jdbc:oGRAC:"};

        int l_qPos = url.indexOf('?');
        if (l_qPos != -1) {
            l_urlServer = url.substring(0, l_qPos);
            l_urlArgs = url.substring(l_qPos + 1);
        }

        boolean isOGRAC = false;
        for (String urlTag : legalUrlTags) {
            if (l_urlServer.startsWith(urlTag)) {
                isUrlLegal = true;
                if (urlTag.equals("jdbc:oGRAC:")) {
                    isOGRAC = true;
                }
            }
        }

        if (!isUrlLegal) {
            LOGGER.debug("JDBC URL must start with \"jdbc:postgresql:\" or \"jdbc:dws:iam:\" but was: "
                    + filterAuthenticationCredentials(url));
            return null;
        }

        if (isOGRAC) {
            l_urlServer = l_urlServer.substring("jdbc:oGRAC:".length());
        } else {
            l_urlServer = l_urlServer.substring("jdbc:postgresql:".length());
        }

        if (l_urlServer.startsWith("//")) {
            l_urlServer = l_urlServer.substring(2);
            int slash;
            if (isOGRAC) {
                slash = l_urlServer.length();
            } else {
                slash = l_urlServer.indexOf('/');
                if (slash == -1) {
                    LOGGER.warn("JDBC URL must contain a / at the end of the host or port: "
                            + filterAuthenticationCredentials(url));
                    return null;
                }
                urlProps.setProperty("PGDBNAME", URLCoder.decode(l_urlServer.substring(slash + 1)));
            }

            // Save the ip and port configured in the url
            String[] addresses = l_urlServer.substring(0, slash).split(",");
            StringBuilder hosts = new StringBuilder();
            StringBuilder ports = new StringBuilder();
            for (String address : addresses) {
                int portIdx = address.lastIndexOf(':');
                if (portIdx != -1 && address.lastIndexOf(']') < portIdx) {
                    String portStr = address.substring(portIdx + 1);
                    try {
                        int port = Integer.parseInt(portStr);
                        if (port < 1 || port > 65535) {
                            LOGGER.warn("JDBC URL port: " + portStr + " not valid (1:65535) ");
                            return null;
                        }
                    } catch (NumberFormatException ignore) {
                        LOGGER.warn("JDBC URL invalid port number: " + portStr);
                        return null;
                    }
                    ports.append(portStr);
                    hosts.append(parseIPValid((String) address.subSequence(0, portIdx)));
                } else {
                    if (address.equalsIgnoreCase(UDS_HOST)) {
                        urlProps.setProperty("UDS", "1");
                    }
                    ports.append(DEFAULT_PORT);
                    hosts.append(parseIPValid(address));
                }
                ports.append(',');
                hosts.append(',');
            }
            ports.setLength(ports.length() - 1);
            hosts.setLength(hosts.length() - 1);

            urlProps.setProperty("PGHOST", hosts.toString());
            urlProps.setProperty("PGPORT", ports.toString());

            // The first connection, put the host and port in the url into the props
            urlProps.setProperty("PGHOSTURL", hosts.toString());
            urlProps.setProperty("PGPORTURL", ports.toString());
        } else {
            // if there are no defaults set or any one of PORT, HOST, DBNAME not set
            // then set it to default
            if (defaults == null || !defaults.containsKey("PGPORT")) {
                urlProps.setProperty("PGPORT", DEFAULT_PORT);
            }
            if (defaults == null || !defaults.containsKey("PGHOST")) {
                urlProps.setProperty("PGHOST", "localhost");
            }
            if (defaults == null || !defaults.containsKey("PGDBNAME")) {
                urlProps.setProperty("PGDBNAME", URLCoder.decode(l_urlServer));
            }
        }

        // parse the args part of the url
        urlProps.putAll(parseParam(l_urlArgs));
        if (urlProps.getProperty("enable_ce") != null && urlProps.getProperty("enable_ce").equals("1")) {
            urlProps.setProperty("CLIENTLOGIC", "1");
        }
        return urlProps;
    }

    /**
     * Shield sensitive information in the connection string, such as password, sslpassword, iamPassword
     *
     * @param url Connection string.
     * @return Connection string without sensitive information
     */
    private static String filterAuthenticationCredentials(String url) {
        String notSensitiveUrl = url;
        int paramStartIndex = notSensitiveUrl.indexOf("?");
        if (paramStartIndex != -1) {
            LinkedHashMap<String, String> paramsMap = parseParam(notSensitiveUrl.substring(paramStartIndex + 1));
            StringBuilder stringBuilder = new StringBuilder(notSensitiveUrl.substring(0, paramStartIndex) + "?");
            for (Map.Entry<String, String> entry : paramsMap.entrySet()) {
                boolean isKeyword = false;
                for (String sensitiveCharacter : SENSITIVE_CHARACTERS) {
                    if (entry.getKey().equals(sensitiveCharacter)) {
                        isKeyword = true;
                        break;
                    }
                }
                if (!isKeyword) {
                    stringBuilder.append(entry.getKey() + "=" + entry.getValue() + "&");
                }
            }
            notSensitiveUrl = stringBuilder.toString();
            if (notSensitiveUrl.endsWith("?") || notSensitiveUrl.endsWith("&")) {
                notSensitiveUrl = notSensitiveUrl.substring(0, notSensitiveUrl.length() - 1);
            }
        }
        return notSensitiveUrl;
    }

    /**
     * @param urlArgs The parameter part in the connection string
     * @return parameter map
     */
    private static LinkedHashMap<String, String> parseParam(String urlArgs) {
        LinkedHashMap<String, String> paramsMap = new LinkedHashMap<>();
        String[] args = urlArgs.split("&");
        for (String token : args) {
            if (token.isEmpty()) {
                continue;
            }
            int pos = token.indexOf('=');
            if (pos == -1) {
                paramsMap.put(token, "");
            } else {
                paramsMap.put(token.substring(0, pos), URLCoder.decode(token.substring(pos + 1)));
            }
        }
        return paramsMap;
    }

    /* public method */
    public static HostSpec[] GetHostSpecs(Properties props) {
        return hostSpecs(props);
    }

    /**
     * @param props Connection properties
     * @return the address portion of the orgin URL
     */
    public static HostSpec[] getURLHostSpecs(Properties props) {
        return urlHostSpecs(props);
    }

    public static String GetUser(Properties props) {
        return user(props);
    }

    public static String GetDatabase(Properties props) {
        return database(props);
    }

    private static HostSpec[] urlHostSpecs(Properties props) {
        String[] ports = props.getProperty("PGPORTURL").split(",");
        String[] hosts = props.getProperty("PGHOSTURL").split(",", ports.length);
        HostSpec[] hostSpecs = new HostSpec[hosts.length];
        for (int i = 0; i < hostSpecs.length; ++i) {
            hostSpecs[i] = new HostSpec(hosts[i], Integer.parseInt(ports[i]));
        }
        return hostSpecs;
    }

    /**
     * @return the address portion of the URL
     */
    private static HostSpec[] hostSpecs(Properties props) {
        String[] ports = props.getProperty("PGPORT").split(",");
        String[] hosts = props.getProperty("PGHOST").split(",", ports.length);
        HostSpec[] hostSpecs = new HostSpec[hosts.length];
        for (int i = 0; i < hostSpecs.length; ++i) {
            hostSpecs[i] = new HostSpec(hosts[i], Integer.parseInt(ports[i]));
        }
        return hostSpecs;
    }

    /**
     * @return the username of the URL
     */
    private static String user(Properties props) {
        return props.getProperty("user", "");
    }

    /**
     * @return the database name of the URL
     */
    private static String database(Properties props) {
        return props.getProperty("PGDBNAME", "");
    }

    /**
     * @return the timeout from the URL, in milliseconds
     */
    private static long timeout(Properties props) {
        String timeout = PGProperty.LOGIN_TIMEOUT.get(props);
        if (timeout != null) {
            try {
                return (long) (Float.parseFloat(timeout) * 1000);
            } catch (NumberFormatException e) {
                LOGGER.warn("Couldn't parse loginTimeout value: " + timeout);
            }
        }
        return (long) DriverManager.getLoginTimeout() * 1000;
    }

    /**
     * This method was added in v6.5, and simply throws an SQLException for an unimplemented method. I
     * decided to do it this way while implementing the JDBC2 extensions to JDBC, as it should help
     * keep the overall driver size down. It now requires the call Class and the function name to help
     * when the driver is used with closed software that don't report the stack strace
     *
     * @param callClass    the call Class
     * @param functionName the name of the unimplemented function with the type of its arguments
     * @return PSQLException with a localized message giving the complete description of the
     * unimplemeted function
     */
    public static SQLFeatureNotSupportedException notImplemented(Class<?> callClass,
                                                                 String functionName) {
        return new SQLFeatureNotSupportedException(
                GT.tr("Method {0} is not yet implemented.", callClass.getName() + "." + functionName),
                PSQLState.NOT_IMPLEMENTED.getState());
    }

    public java.util.logging.Logger getParentLogger() {
        if (Logger.isUsingJDKLogger()) {
            return PARENT_LOGGER;
        } else {
            return null;
        }
    }

    public static SharedTimer getSharedTimer() {
        return sharedTimer;
    }

    /**
     * Register the driver against {@link DriverManager}. This is done automatically when the class is
     * loaded. Dropping the driver from DriverManager's list is possible using {@link #deregister()}
     * method.
     *
     * @throws IllegalStateException if the driver is already registered
     * @throws SQLException          if registering the driver fails
     */
    public static void register() throws SQLException {
        if (isRegistered()) {
            throw new IllegalStateException(
                    "Driver is already registered. It can only be registered once.");
        }
        registeredDriver = new Driver();
        DriverManager.registerDriver(registeredDriver);
        Driver.registeredDriver = registeredDriver;

        isLogFileCreated = new AtomicBoolean(false);
    }

    /**
     * According to JDBC specification, this driver is registered against {@link DriverManager} when
     * the class is loaded. To avoid leaks, this method allow unregistering the driver so that the
     * class can be gc'ed if necessary.
     *
     * @throws IllegalStateException if the driver is not registered
     * @throws SQLException          if deregistering the driver fails
     */
    public static void deregister() throws SQLException {
        if (!isRegistered()) {
            throw new IllegalStateException(
                    "Driver is not registered (or it has not been registered using Driver.register() method)");
        }
        DriverManager.deregisterDriver(registeredDriver);
        registeredDriver = null;
    }

    /**
     * @return {@code true} if the driver is registered against {@link DriverManager}
     */
    public static boolean isRegistered() {
        return registeredDriver != null;
    }

    public static String getGSVersion() {
        return gsVersion;
    }
}
