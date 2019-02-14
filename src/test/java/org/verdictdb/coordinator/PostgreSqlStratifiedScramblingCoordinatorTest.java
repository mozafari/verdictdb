package org.verdictdb.coordinator;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PostgreSqlStratifiedScramblingCoordinatorTest {

  private static Connection postgresConn;

  private static Statement postgresStmt;

  static VerdictOption options = new VerdictOption();

  private static final String POSTGRES_HOST;

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_SCHEMA = "stratified_scrambling_coordinator_test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupPostgresDatabase() throws SQLException, VerdictDBDbmsException, IOException {
    String postgresConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    postgresConn =
        DatabaseConnectionHelpers.setupPostgresql(
            postgresConnectionString, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_SCHEMA);
    postgresStmt = postgresConn.createStatement();
    postgresStmt.execute(String.format("create schema if not exists \"%s\"", POSTGRES_SCHEMA));
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    postgresStmt.execute(String.format("drop schema if exists \"%s\" CASCADE", POSTGRES_SCHEMA));
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    DbmsConnection conn = JdbcConnection.create(postgresConn);
    DbmsQueryResult result = conn.execute(String.format("select * from \"%s\".lineitem", POSTGRES_SCHEMA));
    int rowCount = 0;
    while (result.next()) {
      rowCount++;
    }
    assertEquals(1000, rowCount);
  }

  @Test
  public void testScramblingCoordinatorLineitem() throws VerdictDBException {
    testScramblingCoordinator("lineitem", "l_quantity");
  }

  public void testScramblingCoordinator(String tablename, String columnname) throws VerdictDBException {
    DbmsConnection conn = JdbcConnection.create(postgresConn);

    String scrambleSchema = POSTGRES_SCHEMA;
    String scratchpadSchema = POSTGRES_SCHEMA;
    long blockSize = 100;
    ScramblingCoordinator scrambler = new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = POSTGRES_SCHEMA;
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists %s.%s", POSTGRES_SCHEMA, scrambledTable));
    ScrambleMeta meta = scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "stratified",
        columnname, 11, null, Arrays.asList(columnname), 1, new HashMap<String, String>());

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns(POSTGRES_SCHEMA, originalTable);
    List<Pair<String, String>> columns = conn.getColumns(POSTGRES_SCHEMA, scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size()+2, columns.size());

    List<String> partitions = conn.getPartitionColumns(POSTGRES_SCHEMA, scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 =
        conn.execute(String.format("select count(*) from %s.%s", POSTGRES_SCHEMA, originalTable));
    DbmsQueryResult result2 =
        conn.execute(String.format("select count(*) from %s.%s", POSTGRES_SCHEMA, scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    //assertEquals((int) Math.ceil(result2.getInt(0) / (float) blockSize) - 1, result.getInt(1));

    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(conn, options);
    ScrambleMetaSet scrambleMetas = new ScrambleMetaSet();
    scrambleMetas.addScrambleMeta(meta);
    coordinator.setScrambleMetaSet(scrambleMetas);
    ExecutionResultReader reader =
        coordinator.process(
            String.format("select count(*) from %s.%s",
                POSTGRES_SCHEMA, scrambledTable));
    int count = 0;
    while (reader.hasNext()) {
      reader.next();
      count++;
    }
    assertEquals(10, count);
  }

}
