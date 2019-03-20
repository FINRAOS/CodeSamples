package org.finra.samples;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class PostgresContainerTest {
  private static Logger LOGGER = LoggerFactory.getLogger(PostgresContainerTest.class);

  private static PostgreSQLContainer postgres;
  private static String url = null;
  private static Properties properties;

  public static void startPostgres() {
    postgres = new PostgreSQLContainer("postgres:9.6.8");
    postgres.start();

    url = postgres.getJdbcUrl();

    properties = new Properties();
    properties.setProperty("user", postgres.getUsername());
    properties.setProperty("password", postgres.getPassword());

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> stop())
    );
  }

  public static void stopPostgres() {
    if (postgres != null) {
      postgres.stop();
      postgres.close();
    }
  }

  @BeforeClass
  public static void start() {
    startPostgres();
  }

  public static void stop() {
    stopPostgres();
  }

  @Before
  public void setUp() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/setUp.sql"), Charsets.UTF_8));
      }
    }
  }

  @After
  public void tearDown() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/tearDown.sql"), Charsets.UTF_8));
      }
    }
  }

  @Test
  public void testOne() throws SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(
            "INSERT INTO test_values VALUES\n"
                + "(1, 25),\n"
                + "(2, 41);"
        );

        stmt.executeQuery(
            "SELECT * FROM add_one();");

        try (ResultSet rs = stmt.executeQuery(
            "SELECT value FROM test_values\n"
                + "WHERE id = 1")) {
          while (rs.next()) {
            Assert.assertEquals(26, rs.getInt(1));
          }
        }

        try (ResultSet rs = stmt.executeQuery(
            "SELECT value FROM test_values\n"
                + "WHERE id = 2")) {
          while (rs.next()) {
            Assert.assertEquals(42, rs.getInt(1));
          }
        }
      }
    }
  }

  @Test
  public void testTwo() throws SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(
            "INSERT INTO test_values VALUES\n"
                + "(1, 50),\n"
                + "(2, -10);"
        );

        stmt.executeQuery(
            "SELECT * FROM add_one();");

        try (ResultSet rs = stmt.executeQuery(
            "SELECT value FROM test_values\n"
                + "WHERE id = 1")) {
          while (rs.next()) {
            Assert.assertEquals(51, rs.getInt(1));
          }
        }

        try (ResultSet rs = stmt.executeQuery(
            "SELECT value FROM test_values\n"
                + "WHERE id = 2")) {
          while (rs.next()) {
            Assert.assertEquals(-9, rs.getInt(1));
          }
        }
      }
    }
  }
}
