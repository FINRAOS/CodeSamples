package org.finra.samples;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import de.flapdoodle.embed.process.io.file.Files;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.ext.SubdirTempDir;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class PostgresEmbeddedSqlTest {
  private static Logger LOGGER = LoggerFactory.getLogger(PostgresEmbeddedSqlTest.class);

  private static EmbeddedPostgres postgres;
  private static String url = null;

  private String functionName;
  private String message;
  private String status;
  private String executed;

  public static void startPostgres() throws IOException {
    postgres = new EmbeddedPostgres(() -> "9.6.3-1");
    url = postgres.start("localhost", 5433, "dbname", "username", "password");

    Runtime.getRuntime().addShutdownHook(
        new Thread(() -> {
          try {
            stop();
          }
          catch (IOException | SQLException e) {
            LOGGER.error("Error while stopping Postgres instance.");
          }
        })
    );
  }

  @AfterClass
  public static void stopPostgres() {
    if (postgres != null && postgres.getProcess().isPresent() && postgres.getProcess().get().isProcessRunning()) {
      postgres.stop();
    }
    Files.forceDelete(SubdirTempDir.defaultInstance().asFile());
  }

  public static void start() throws IOException, SQLException {
    startPostgres();
    setUp();
  }

  public static void stop() throws IOException, SQLException {
    tearDown();
    stopPostgres();
  }

  public static void setUp() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url)) {
      try (Statement stmt = conn.createStatement()) {
        // Following installation file comes from https://github.com/mixerp/plpgunit (install folder)
        stmt.executeUpdate(Resources.toString(Resources.getResource("install/1.install-unit-test.sql"), Charsets.UTF_8));
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/setUp.sql"), Charsets.UTF_8));
        stmt.executeUpdate(Resources.toString(Resources.getResource("testFunctions/addOnePositiveTest.sql"), Charsets.UTF_8));
        stmt.executeUpdate(Resources.toString(Resources.getResource("testFunctions/addOneNegativeTest.sql"), Charsets.UTF_8));
      }
    }
  }

  public static void tearDown() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(url)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate(Resources.toString(Resources.getResource("scripts/tearDown.sql"), Charsets.UTF_8));
      }
    }
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection data() throws IOException, SQLException {
    start();
    List<String[]> data = new ArrayList<>();

    try (Connection conn = DriverManager.getConnection(url)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery("SELECT * FROM unit_tests.begin();");

        try (ResultSet rs = stmt.executeQuery(
            "SELECT function_name, message, status, executed FROM unit_tests.test_details;")) {
          while (rs.next()) {
            data.add(new String[]
                {
                    rs.getString(1),
                    rs.getString(2),
                    rs.getString(3),
                    rs.getString(4)
                }
            );
          }
        }
      }
    }
    catch (SQLException e) {
      Assert.assertNull(e);
    }

    return data;
  }

  public PostgresEmbeddedSqlTest(String functionName, String message, String status,
                                 String executed) {
    this.functionName = functionName;
    this.message = message;
    this.status = status;
    this.executed = executed;
  }

  @Test
  public void test() {
    LOGGER.info(functionName + " " + message);
    Assert.assertEquals(functionName + " status", "t", this.status);
    Assert.assertEquals(functionName + " executed", "t", this.executed);
  }
}
