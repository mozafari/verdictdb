package org.verdictdb.coordinator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.VerdictContext;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class MySqlShowTableTest {

  private static Connection mysqlConn;

  private static Statement mysqlStmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR;

  private static final String MYSQL_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
      MYSQL_UESR = "root";
    } else {
      MYSQL_HOST = "localhost";
      MYSQL_UESR = "root";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    mysqlConn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    mysqlStmt = mysqlConn.createStatement();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlStmt.execute(String.format("drop schema if exists `%s`", MYSQL_DATABASE));
  }

  @Test
  public void showDatabaseTest() throws VerdictDBException,SQLException {
    String sql = "show databases";
    DbmsConnection dbmsconn = new CachedDbmsConnection(
        new JdbcConnection(mysqlConn, new MysqlSyntax()));
    VerdictContext verdict = new VerdictContext(dbmsconn);
    ExecutionContext exec = new ExecutionContext(verdict, 0);
    VerdictSingleResult result = exec.sql(sql);
    ResultSet rs = mysqlStmt.executeQuery(sql);
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getValue(0));
    }
  }

  @Test
  public void showSchemaTest() throws VerdictDBException,SQLException {
    String sql = "show schemas";
    DbmsConnection dbmsconn = new CachedDbmsConnection(
        new JdbcConnection(mysqlConn, new MysqlSyntax()));
    VerdictContext verdict = new VerdictContext(dbmsconn);
    ExecutionContext exec = new ExecutionContext(verdict, 0);
    VerdictSingleResult result = exec.sql(sql);
    ResultSet rs = mysqlStmt.executeQuery(sql);
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getValue(0));
    }
    assertEquals(rs.getMetaData().getColumnCount(), result.getColumnCount());
  }

  @Test
  public void showTablesTest() throws VerdictDBException,SQLException {
    String sql = "show tables in " + MYSQL_DATABASE;
    DbmsConnection dbmsconn = new CachedDbmsConnection(
        new JdbcConnection(mysqlConn, new MysqlSyntax()));
    VerdictContext verdict = new VerdictContext(dbmsconn);
    ExecutionContext exec = new ExecutionContext(verdict, 0);
    VerdictSingleResult result = exec.sql(sql);
    ResultSet rs = mysqlStmt.executeQuery(sql);
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getValue(0));
    }
    assertEquals(rs.getMetaData().getColumnCount(), result.getColumnCount());
  }

  @Test
  public void DescribeTableTest() throws VerdictDBException,SQLException {
    String sql = String.format("DESCRIBE %s.%s",MYSQL_DATABASE, "nation");
    DbmsConnection dbmsconn = new CachedDbmsConnection(
        new JdbcConnection(mysqlConn, new MysqlSyntax()));
    VerdictContext verdict = new VerdictContext(dbmsconn);
    ExecutionContext exec = new ExecutionContext(verdict, 0);
    VerdictSingleResult result = exec.sql(sql);
    ResultSet rs = mysqlStmt.executeQuery(sql);
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getValue(0));
      assertEquals(rs.getString(2), result.getValue(1));
    }
    assertEquals(rs.getMetaData().getColumnCount(), result.getColumnCount());
  }

}
