package org.finra.samples;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import de.flapdoodle.embed.process.io.file.Files;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.ext.SubdirTempDir;

import java.io.IOException;
import java.sql.*;

public class PostgresEmbeddedTest {
  private static Logger LOGGER = LoggerFactory.getLogger(PostgresEmbeddedSqlTest.class);

  private static EmbeddedPostgres postgres;
  private static String url = null;

  public static void startPostgres() throws IOException {
    postgres = new EmbeddedPostgres(() -> "9.6.3-1");
    url = postgres.start("localhost", 5433, "dbname", "username", "password");

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> stop())
    );
  }

  public static void stopPostgres() {
    if (postgres != null && postgres.getProcess().isPresent() && postgres.getProcess().get().isProcessRunning()) {
      postgres.stop();
    }
    Files.forceDelete(SubdirTempDir.defaultInstance().asFile());
  }

  @BeforeClass
  public static void start() throws IOException {
    startPostgres();
  }

  @AfterClass
  public static void stop() {
    stopPostgres();
  }

  @Before
  public void setUp() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/setUp.sql"), Charsets.UTF_8));
      }
    }
  }

  @After
  public void tearDown() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/tearDown.sql"), Charsets.UTF_8));
      }
    }
  }

  @Test
  public void testOne() throws SQLException {
    try (Connection conn = DriverManager.getConnection(url)) {
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
    try (Connection conn = DriverManager.getConnection(url)) {
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
