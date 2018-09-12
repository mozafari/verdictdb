package org.verdictdb.jdbc41;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Created by Dong Young Yoon on 7/18/18. */
@RunWith(Parameterized.class)
public class JdbcTpchQueryForAllDatabasesTest {

  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, Connection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static VerdictOption options = new VerdictOption();

  private static final String MYSQL_HOST;

  private static final int MYSQL_TPCH_QUERY_COUNT = 21;

  private static final int TPCH_QUERY_COUNT = 22;

  private static final String VERDICT_META_SCHEMA =
      "verdictdbmetaschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  private static final String VERDICT_TEMP_SCHEMA =
      "verdictdbtempschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private String database = "";

  private String query;

  // TODO: Add support for all four databases
  //  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};
  private static final String[] targetDatabases = {"mysql", "impala", "redshift"};

  public JdbcTpchQueryForAllDatabasesTest(String database, String query) {
    this.database = database;
    this.query = query;
  }

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE =
      "tpch_test_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String IMPALA_HOST;

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  private static final String IMPALA_DATABASE = "tpch_2_parquet";

  private static final String IMPALA_USER = "";

  private static final String IMPALA_PASSWORD = "";

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  private static final String POSTGRES_HOST;

  private static final String POSTGRES_DATABASE = "test";

  private static final String POSTGRES_USER = "postgres";

  private static final String POSTGRES_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  private static final String SCHEMA_NAME =
      "verdictdb_tpch_query_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  @BeforeClass
  public static void setupDatabases() throws SQLException, VerdictDBDbmsException, IOException {
    options.setVerdictMetaSchemaName(VERDICT_META_SCHEMA);
    options.setVerdictTempSchemaName(VERDICT_TEMP_SCHEMA);
    setupMysql();
    setupImpala();
    setupRedshift();
    
    // TODO: Add below databases too
    //    setupPostgresql();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    for (String s : connMap.keySet()) {
      Connection conn = connMap.get(s);
      if (s.equals("mysql")) {
        conn.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s", SCHEMA_NAME));
      } else {
        conn.createStatement()
            .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", SCHEMA_NAME));
      }
    }
  }

  @Parameterized.Parameters(name = "{0}_tpch_{1}")
  public static Collection<Object[]> databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      int queryCount = 0;
      switch (database) {
        case "mysql":
          queryCount = MYSQL_TPCH_QUERY_COUNT;
          break;
        case "impala":
        case "redshift":
          queryCount = TPCH_QUERY_COUNT;
          break;
      }
      // query 4, 13, 16, 21 contains count distinct
      for (int query = 1; query <= queryCount; ++query) {
        if (query!=13 && query!=21) {
          params.add(new Object[] {database, String.valueOf(query)});
        }
      }
      if (database.equals("redshift")) {
        params.add(new Object[] {database, "e1"});
        params.add(new Object[] {database, "e2"});
        params.add(new Object[] {database, "e3"});
        params.add(new Object[] {database, "e4"});
        params.add(new Object[] {database, "e5"});
        params.add(new Object[] {database, "e6"});
        params.add(new Object[] {database, "e7"});
        params.add(new Object[] {database, "e8"});
      }

      // Uncomment below lines to test a specific query
      //      params.clear();
      //      params.add(new Object[] {database, "e8"});
    }
    return params;
  }

  private static Connection setupMysql() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format(
            "jdbc:verdict:mysql://%s?autoReconnect=true&useSSL=false&"
                + "verdictdbmetaschema=%s&verdictdbtempschema=%s",
            MYSQL_HOST, VERDICT_META_SCHEMA, VERDICT_TEMP_SCHEMA);
    Connection conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, SCHEMA_NAME);
    Connection vc =
        DriverManager.getConnection(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    conn.setCatalog(SCHEMA_NAME);
    vc.setCatalog(SCHEMA_NAME);
    connMap.put("mysql", conn);
    vcMap.put("mysql", vc);
    schemaMap.put("mysql", MYSQL_DATABASE + ".");

    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    return conn;
  }

  private static Connection setupImpala() throws SQLException, VerdictDBDbmsException {
    String connectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    String verdictConnectionString =
        String.format(
            "jdbc:verdict:impala://%s;verdictdbmetaschema=%s;verdictdbtempschema=%s",
            IMPALA_HOST, VERDICT_META_SCHEMA, VERDICT_TEMP_SCHEMA);
    Connection conn =
        DatabaseConnectionHelpers.setupImpala(
            connectionString, IMPALA_USER, IMPALA_PASSWORD, SCHEMA_NAME);
    Connection vc =
        DriverManager.getConnection(verdictConnectionString, IMPALA_USER, IMPALA_PASSWORD);
    connMap.put("impala", conn);
    vcMap.put("impala", vc);
    schemaMap.put("impala", IMPALA_DATABASE + ".");
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    return conn;
  }

  private static Connection setupRedshift()
      throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    String verdictConnectionString =
        String.format(
            "jdbc:verdict:redshift://%s/%s;verdictdbtempschema=%s",
            REDSHIFT_HOST, REDSHIFT_DATABASE, SCHEMA_NAME);
    Connection conn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, SCHEMA_NAME);
    Connection vc =
        DriverManager.getConnection(verdictConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    connMap.put("redshift", conn);
    vcMap.put("redshift", vc);
    schemaMap.put("redshift", "");
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    return conn;
  }

  public static Connection setupPostgresql() throws SQLException, IOException, VerdictDBException {
    String connectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRES_HOST, POSTGRES_DATABASE);
    Connection conn =
        DatabaseConnectionHelpers.setupPostgresql(
            connectionString, POSTGRES_HOST, POSTGRES_PASSWORD, SCHEMA_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD, options);
    connMap.put("postgresql", conn);
    vcMap.put("postgresql", vc);
    schemaMap.put("postgresql", "");
    return conn;
  }

  @Test
  public void testTpch() throws IOException, SQLException {
    ClassLoader classLoader = getClass().getClassLoader();
    String filename = "";
    switch (database) {
      case "mysql":
        filename = "companya/templated/mysql_queries/tpchMySQLQuery" + query + ".sql";
        break;
      case "impala":
        filename = "companya/templated/impala_queries/tpchImpalaQuery" + query + ".sql";
        break;
      case "redshift":
        filename = "companya/templated/redshift_queries/" + query + ".sql";
        break;
      default:
        fail(String.format("Database '%s' not supported.", database));
    }
    File queryFile = new File(classLoader.getResource(filename).getFile());
    if (queryFile.exists()) {
      String originalSql = Files.toString(queryFile, Charsets.UTF_8);
      String sql =
          originalSql.replaceAll(DatabaseConnectionHelpers.TEMPLATE_SCHEMA_NAME, SCHEMA_NAME);

      Statement jdbcStmt = connMap.get(database).createStatement();
      Statement vcStmt = vcMap.get(database).createStatement();

      ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
      ResultSet vcRs = vcStmt.executeQuery(sql);

      int columnCount = jdbcRs.getMetaData().getColumnCount();
      boolean jdbcNext = jdbcRs.next();
      boolean vcNext = vcRs.next();
      while (jdbcNext && vcNext) {
        assertEquals(jdbcNext, vcNext);
        for (int i = 1; i <= columnCount; ++i) {
          System.out.println(jdbcRs.getObject(i) + " : " + vcRs.getObject(i));
          assertEquals(jdbcRs.getObject(i), vcRs.getObject(i));
        }
        jdbcNext = jdbcRs.next();
        vcNext = vcRs.next();
      }
      assertEquals(jdbcNext, vcNext);
    } else {
      System.out.println(String.format("tpch%d does not exist.", query));
    }
  }
}
