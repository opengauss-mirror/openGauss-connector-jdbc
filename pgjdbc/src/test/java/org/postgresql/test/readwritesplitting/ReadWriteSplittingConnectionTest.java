/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package org.postgresql.test.readwritesplitting;

import org.junit.*;
import org.postgresql.readwritesplitting.ReadWriteSplittingHostSpec;
import org.postgresql.readwritesplitting.ReadWriteSplittingPgConnection;
import org.postgresql.test.TestUtil;
import org.postgresql.util.HostSpec;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.instanceOf;

/**
 * Read write splitting connection test.
 *
 * @since 2023-11-20
 */
@Ignore
public class ReadWriteSplittingConnectionTest {
    private static final int DN_NUM = 3;

    private static final String ACCOUNT_TABLE = "account";

    private static HostSpec[] hostSpecs;

    private static HostSpec writeHostSpec;

    private static HostSpec[] readHostSpecs;

    private int currentIndex;

    private static HostSpec[] initHostSpecs() {
        HostSpec[] result = new HostSpec[DN_NUM];
        result[0] = getMasterHostSpec();
        result[1] = new HostSpec(TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort());
        result[2] = new HostSpec(TestUtil.getSecondaryServer2(), TestUtil.getSecondaryServerPort2());
        return result;
    }

    private static HostSpec[] initReadSpecs() {
        HostSpec[] result = new HostSpec[DN_NUM - 1];
        result[0] = new HostSpec(TestUtil.getSecondaryServer(), TestUtil.getSecondaryPort());
        result[1] = new HostSpec(TestUtil.getSecondaryServer2(), TestUtil.getSecondaryServerPort2());
        return result;
    }

    private static HostSpec[] getReadHostSpecs() {
        return readHostSpecs;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        hostSpecs = initHostSpecs();
        readHostSpecs = initReadSpecs();
        writeHostSpec = hostSpecs[0];
        try (Connection connection = TestUtil.openDB()) {
            TestUtil.createTable(connection, ACCOUNT_TABLE, "id int, balance float, transaction_id int");
            TestUtil.execute("insert into account values(1, 1, 1)", connection);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try (Connection connection = TestUtil.openDB()) {
            TestUtil.dropTable(connection, ACCOUNT_TABLE);
        }
    }

    private static HostSpec getMasterHostSpec() {
        return new HostSpec(TestUtil.getServer(), TestUtil.getPort());
    }

    private String initURL(HostSpec[] hostSpecs) {
        String host1 = hostSpecs[0].getHost() + ":" + hostSpecs[0].getPort();
        String host2 = hostSpecs[1].getHost() + ":" + hostSpecs[1].getPort();
        String host3 = hostSpecs[2].getHost() + ":" + hostSpecs[2].getPort();
        return "jdbc:postgresql://" + host1 + "," + host2 + "," + host3 + "/" + TestUtil.getDatabase();
    }

    private Connection getConnection(String urlParams) throws SQLException {
        String url = initURL(hostSpecs) + urlParams;
        Properties props = getProperties();
        return DriverManager.getConnection(url, props);
    }

    @Test
    public void roundRobinLoadBalanceTest() throws SQLException {
        String urlParams = String.format("?enableStatementLoadBalance=true&autoBalance=roundrobin"
                + "&writeDataSourceAddress=%s", getMasterHostSpec());
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            ReadWriteSplittingHostSpec readWriteSplittingHostSpec =
                    readWriteSplittingPgConnection.getReadWriteSplittingHostSpec();
            Assert.assertEquals(readWriteSplittingHostSpec.getWriteHostSpec(), getMasterHostSpec());
            Assert.assertEquals(readWriteSplittingHostSpec.getReadHostSpecs(), getReadHostSpecs());
            try (Statement statement = connection.createStatement()) {
                Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                HostSpec actual = getRoutedReadHostSpec(readWriteSplittingPgConnection);
                for (int i = 0; i < readHostSpecs.length; i++) {
                    HostSpec firstExpected = getNextExpectedRoundRobinSpec();
                    if (firstExpected.equals(actual)) {
                        break;
                    }
                }
                Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
            }
            for (int i = 0; i < 10; i++) {
                try (Statement statement = connection.createStatement()) {
                    Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                    HostSpec actual = getRoutedReadHostSpec(readWriteSplittingPgConnection);
                    Assert.assertEquals(getNextExpectedRoundRobinSpec(), actual);
                }
            }
            for (int i = 0; i < 10; i++) {
                String sql = "SELECT * FROM account WHERE id = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, "1");
                    Assert.assertTrue(statement.execute());
                    HostSpec actual = getRoutedReadHostSpec(readWriteSplittingPgConnection);
                    Assert.assertEquals(getNextExpectedRoundRobinSpec(), actual);
                }
            }
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("UPDATE account SET balance = 11 WHERE id = 1");
                    Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
                }
            }
        }
    }

    private static ReadWriteSplittingPgConnection getReadWriteSplittingPgConnection(Connection connection) {
        Assert.assertThat(connection, instanceOf(ReadWriteSplittingPgConnection.class));
        if (connection instanceof ReadWriteSplittingPgConnection) {
            return (ReadWriteSplittingPgConnection) connection;
        }
        throw new IllegalStateException("Unexpected connection type");
    }

    @Test
    public void shuffleLoadBalanceTest() throws SQLException {
        String urlParams = String.format("?enableStatementLoadBalance=true&autoBalance=shuffle&writeDataSourceAddress"
                + "=%s", getMasterHostSpec());
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            ReadWriteSplittingHostSpec readWriteSplittingHostSpec =
                    readWriteSplittingPgConnection.getReadWriteSplittingHostSpec();
            Assert.assertEquals(readWriteSplittingHostSpec.getWriteHostSpec(), getMasterHostSpec());
            Assert.assertEquals(readWriteSplittingHostSpec.getReadHostSpecs(), getReadHostSpecs());
            for (int i = 0; i < 10; i++) {
                try (Statement statement = connection.createStatement()) {
                    Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                    Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
                }
            }
            for (int i = 0; i < 10; i++) {
                String sql = "SELECT * FROM account WHERE id = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, "1");
                    Assert.assertTrue(statement.execute());
                    Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
                }
            }
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("UPDATE account SET balance = 11 WHERE id = 1");
                    Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
                }
            }
        }
    }

    @Test
    public void priorityLoadBalanceTest() throws SQLException {
        String urlParams = String.format("?enableStatementLoadBalance=true&autoBalance=priority2"
                + "&writeDataSourceAddress=%s", getMasterHostSpec());
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            ReadWriteSplittingHostSpec readWriteSplittingHostSpec =
                    readWriteSplittingPgConnection.getReadWriteSplittingHostSpec();
            Assert.assertEquals(readWriteSplittingHostSpec.getWriteHostSpec(), getMasterHostSpec());
            Assert.assertEquals(readWriteSplittingHostSpec.getReadHostSpecs(), getReadHostSpecs());
            HostSpec firstActual;
            try (Statement statement = connection.createStatement()) {
                Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                firstActual = getRoutedReadHostSpec(readWriteSplittingPgConnection);
            }
            for (int i = 0; i < 10; i++) {
                try (Statement statement = connection.createStatement()) {
                    Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                    HostSpec actual = getRoutedReadHostSpec(readWriteSplittingPgConnection);
                    Assert.assertEquals(firstActual, actual);
                }
            }
            for (int i = 0; i < 10; i++) {
                String sql = "SELECT * FROM account WHERE id = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, "1");
                    Assert.assertTrue(statement.execute());
                    HostSpec actual = getRoutedReadHostSpec(readWriteSplittingPgConnection);
                    Assert.assertEquals(firstActual, actual);
                }
            }
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("UPDATE account SET balance = 11 WHERE id = 1");
                    Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
                }
            }
        }
    }

    @Test
    public void targetServerTypeOfMasterTest() throws SQLException {
        String urlParams = String.format("?enableStatementLoadBalance=true&autoBalance=shuffle&writeDataSourceAddress"
                + "=%s&targetServerType=master", getMasterHostSpec());
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            ReadWriteSplittingHostSpec readWriteSplittingHostSpec =
                    readWriteSplittingPgConnection.getReadWriteSplittingHostSpec();
            Assert.assertEquals(readWriteSplittingHostSpec.getWriteHostSpec(), getMasterHostSpec());
            Assert.assertEquals(readWriteSplittingHostSpec.getReadHostSpecs(), getReadHostSpecs());
            for (int i = 0; i < 10; i++) {
                try (Statement statement = connection.createStatement()) {
                    Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                    Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
                }
            }
            for (int i = 0; i < 10; i++) {
                String sql = "SELECT * FROM account WHERE id = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, "1");
                    Assert.assertTrue(statement.execute());
                    Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
                }
            }
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("UPDATE account SET balance = 11 WHERE id = 1");
                    Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
                }
            }
        }
    }

    @Test
    public void targetServerTypeOfSlaveTest() throws SQLException {
        String urlParams = String.format("?enableStatementLoadBalance=true&autoBalance=shuffle&writeDataSourceAddress"
                + "=%s&targetServerType=slave", getMasterHostSpec());
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            ReadWriteSplittingHostSpec readWriteSplittingHostSpec =
                    readWriteSplittingPgConnection.getReadWriteSplittingHostSpec();
            Assert.assertEquals(readWriteSplittingHostSpec.getWriteHostSpec(), getMasterHostSpec());
            Assert.assertEquals(readWriteSplittingHostSpec.getReadHostSpecs(), getReadHostSpecs());
            for (int i = 0; i < 10; i++) {
                try (Statement statement = connection.createStatement()) {
                    Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                    Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
                }
            }
            for (int i = 0; i < 10; i++) {
                String sql = "SELECT * FROM account WHERE id = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, "1");
                    Assert.assertTrue(statement.execute());
                    Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
                }
            }
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    try {
                        statement.execute("UPDATE account SET balance = 11 WHERE id = 1");
                    } catch (SQLException e) {
                        Assert.assertTrue(e.getMessage().contains("ERROR: cannot execute UPDATE in a read-only "
                                + "transaction"));
                    }
                }
            }
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", TestUtil.getUser());
        properties.setProperty("password", TestUtil.getPassword());
        return properties;
    }

    @Test
    public void transactionRouteTest() throws SQLException {
        String params = "?enableStatementLoadBalance=true&autoBalance=shuffle&writeDataSourceAddress=%s";
        String urlParams = String.format(params, getMasterHostSpec());
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            connection.setAutoCommit(false);
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                    Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
                }
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute("UPDATE account SET balance = 11 WHERE id = 1");
                Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
            }
            connection.commit();
            try (Statement statement = connection.createStatement()) {
                Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
            }
            connection.rollback();
            try (Statement statement = connection.createStatement()) {
                Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
            }
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                Assert.assertTrue(statement.execute("SELECT * FROM account WHERE id = 1"));
                Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute("UPDATE account SET balance = 11 WHERE id = 1");
                Assert.assertTrue(isRoutedToWriteHostSpecs(readWriteSplittingPgConnection));
            }
        }
    }

    @Test
    public void executeMultiQueryByOneStatementTest() throws Exception {
        String params = "?enableStatementLoadBalance=true&autoBalance=shuffle&writeDataSourceAddress=%s";
        String urlParams = String.format(params, getMasterHostSpec());
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            try (Statement statement = connection.createStatement()) {
                for (int i = 0; i < 3; i++) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM account WHERE id = 1");
                    ResultSet resultSet2 = statement.executeQuery("SELECT * FROM account WHERE id = 1");
                    ResultSet resultSet3 = statement.executeQuery("SELECT * FROM account WHERE id = 1");
                    Assert.assertTrue(resultSet.next());
                    Assert.assertTrue(resultSet2.next());
                    Assert.assertTrue(resultSet3.next());
                    Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
                }
            }
        }
    }

    @Test
    public void executeQueryWithoutWriteDataSourceAddressParamTest() throws Exception {
        String urlParams = "?enableStatementLoadBalance=true&autoBalance=shuffle";
        try (Connection connection = getConnection(urlParams)) {
            ReadWriteSplittingPgConnection readWriteSplittingPgConnection =
                    getReadWriteSplittingPgConnection(connection);
            for (int i = 0; i < 3; i++) {
                try (Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM account WHERE id = 1");
                    Assert.assertTrue(resultSet.next());
                    Assert.assertTrue(isRoutedToReadHostSpecs(readWriteSplittingPgConnection));
                }
            }
        }
    }

    private HostSpec getNextExpectedRoundRobinSpec() {
        return readHostSpecs[(currentIndex++) % readHostSpecs.length];
    }

    private static boolean isRoutedToReadHostSpecs(ReadWriteSplittingPgConnection readWriteSplittingPgConnection)
            throws SQLException {
        String socketAddress =
                readWriteSplittingPgConnection.getConnectionManager().getCurrentConnection().getSocketAddress();
        for (HostSpec readHostSpec : getReadHostSpecs()) {
            if (socketAddress.endsWith(getHostOrAlias(readHostSpec) + ":" + readHostSpec.getPort())) {
                return true;
            }
        }
        return false;
    }

    private static HostSpec getRoutedReadHostSpec(ReadWriteSplittingPgConnection readWriteSplittingPgConnection)
            throws SQLException {
        String socketAddress =
                readWriteSplittingPgConnection.getConnectionManager().getCurrentConnection().getSocketAddress();
        for (HostSpec readHostSpec : getReadHostSpecs()) {
            if (socketAddress.endsWith(getHostOrAlias(readHostSpec) + ":" + readHostSpec.getPort())) {
                return readHostSpec;
            }
        }
        throw new IllegalStateException("Must routed to one read host spec");
    }

    private static boolean isRoutedToWriteHostSpecs(ReadWriteSplittingPgConnection readWriteSplittingPgConnection)
            throws SQLException {
        String socketAddress =
                readWriteSplittingPgConnection.getConnectionManager().getCurrentConnection().getSocketAddress();
        return socketAddress.endsWith(getHostOrAlias(writeHostSpec) + ":" + writeHostSpec.getPort());
    }

    private static String getHostOrAlias(HostSpec readHostSpec) {
        String host = readHostSpec.getHost();
        return host.equalsIgnoreCase("localhost") ? "127.0.0.1" : host;
    }
}
