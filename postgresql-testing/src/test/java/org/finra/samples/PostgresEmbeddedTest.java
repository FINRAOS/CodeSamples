package org.finra.samples;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import de.flapdoodle.embed.process.io.file.Files;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.ext.SubdirTempDir;

public class PostgresEmbeddedTest {
  private static Logger LOGGER = LoggerFactory.getLogger(PostgresEmbeddedTestPureSQL.class);

  private static EmbeddedPostgres postgres;
  private static String url = null;

  public static void startPostgres() throws IOException {
    postgres = new EmbeddedPostgres(() -> "9.6.3-1");
    url = postgres.start("localhost", 5433, "dbname", "username", "password");

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          try {
            stop();
          }
          catch (IOException e) {
            LOGGER.error("Error while stopping Postgres instance.");
          }
        })
    );
  }

  public static void stopPostgres() throws IOException {
    postgres.stop();
  }

  @BeforeClass
  public static void start() throws IOException {
    startPostgres();
  }

  public static void stop() throws IOException {
    stopPostgres();
    Files.forceDelete(SubdirTempDir.defaultInstance().asFile());
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
  public void testOne() throws IOException, SQLException {
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
  public void testTwo() throws IOException, SQLException {
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
