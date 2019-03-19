package org.finra.samples;

import static java.lang.String.format;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.googlecode.junittoolbox.ParallelRunner;
import de.flapdoodle.embed.process.io.file.Files;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qatools.embed.postgresql.Command;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Credentials;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Net;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Storage;
import ru.yandex.qatools.embed.postgresql.config.AbstractPostgresConfig.Timeout;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;
import ru.yandex.qatools.embed.postgresql.config.RuntimeConfigBuilder;
import ru.yandex.qatools.embed.postgresql.ext.SubdirTempDir;

@RunWith(ParallelRunner.class)
public class PostgresEmbeddedTestParallel {
  private static Logger LOGGER = LoggerFactory.getLogger(PostgresEmbeddedTestPureSQL.class);

  private PostgresProcess postgres;
  private String url = null;
  private static PostgresExecutable exec;
  private Properties properties;

  @BeforeClass
  public static void getPostgres() throws IOException {
    PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getInstance(
        new RuntimeConfigBuilder().defaults(Command.Postgres).build());

    PostgresConfig config = new PostgresConfig(
        () -> "9.6.3-1",
        new Net(),
        new Storage("dbname"),
        new Timeout(),
        new Credentials("username", "password"));

    exec = runtime.prepare(config);

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> removePostgres())
    );
  }

  @AfterClass
  public static void removePostgres() {
    exec.stop();
    Files.forceDelete(SubdirTempDir.defaultInstance().asFile());
  }

  public void startPostgres() throws IOException {
    PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getInstance(
        new RuntimeConfigBuilder().defaults(Command.Postgres).build());

    PostgresConfig config = new PostgresConfig(
        () -> "9.6.3-1",
        new Net(),
        new Storage("dbname"),
        new Timeout(),
        new Credentials("username", "password"));

    PostgresExecutable exec = runtime.prepare(config);

    postgres = exec.start();

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          try {
            stopPostgres();
          }
          catch (IOException e) {
            LOGGER.error("Error while stopping Postgres instance.");
          }
        })
    );

    url = format("jdbc:postgresql://%s:%s/%s",
        config.net().host(),
        config.net().port(),
        config.storage().dbName());

    properties = new Properties();
    properties.setProperty("user", config.credentials().username());
    properties.setProperty("password", config.credentials().password());
  }

  public void stopPostgres() throws IOException {
    if (postgres.isProcessRunning()) {
      postgres.stop();
    }
  }

  @Before
  public void start() throws IOException, SQLException {
    startPostgres();
    setUp();
  }

  @After
  public void stop() throws IOException, SQLException {
    tearDown();
    stopPostgres();
  }

  public void setUp() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/setUp.sql"), Charsets.UTF_8));
      }
    }
  }

  public void tearDown() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url, properties)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/tearDown.sql"), Charsets.UTF_8));
      }
    }
  }

  @Test
  public void testOne() throws IOException, SQLException {
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
  public void testTwo() throws IOException, SQLException {
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
