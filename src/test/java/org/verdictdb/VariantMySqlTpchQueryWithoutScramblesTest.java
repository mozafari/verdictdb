package org.verdictdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class VariantMySqlTpchQueryWithoutScramblesTest {

  static Connection conn;

  static DbmsConnection dbmsConnection;

  static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "tpch_flat_orc_2";

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE);
    dbmsConnection = JdbcConnection.create(conn);
    dbmsConnection.setDefaultSchema(MYSQL_DATABASE);
    stmt = conn.createStatement();
    stmt.execute(
        String.format(
            "create schema if not exists `%s`", VerdictOption.getDefaultTempSchemaName()));
  }

  public Pair<VerdictSingleResult, ResultSet> getAnswer(int queryNum)
      throws IOException, VerdictDBException, SQLException {
    ClassLoader classLoader = getClass().getClassLoader();
    String filename = "companya/mysql_queries/tpchMySQLQuery" + queryNum + ".sql";
    File queryFile = new File(classLoader.getResource(filename).getFile());
    String sql = Files.toString(queryFile, Charsets.UTF_8);

    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);

    VerdictSingleResult result = verdictContext.sql(sql);

    ResultSet rs = stmt.executeQuery(stdQuery);
    return new ImmutablePair<>(result, rs);
  }

  @Test
  public void Query1Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(1);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getDouble(1), result.getDouble(0), 1e-5);
      assertEquals(rs.getDouble(2), result.getDouble(1), 1e-5);
      assertEquals(rs.getDouble(3), result.getDouble(2), 1e-5);
      assertEquals(rs.getString(4), result.getString(3));
      assertEquals(rs.getString(5), result.getString(4));
      assertEquals(rs.getDouble(6), result.getDouble(5), 1e-5);
      assertEquals(rs.getDouble(7), result.getDouble(6), 1e-5);
      assertEquals(rs.getDouble(8), result.getDouble(7), 1e-5);
      assertEquals(rs.getDouble(9), result.getDouble(8), 1e-5);
      assertEquals(rs.getDouble(10), result.getDouble(9), 1e-5);
    }
  }

  @Test
  public void Query2Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(2);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getString(2), result.getString(1));
      assertEquals(rs.getInt(3), result.getInt(2));
      assertEquals(rs.getInt(4), result.getInt(3));
      assertEquals(rs.getString(5), result.getString(4));
      assertEquals(rs.getString(6), result.getString(5));
      assertEquals(rs.getString(7), result.getString(6));
      assertEquals(rs.getString(8), result.getString(7));
      assertEquals(rs.getDouble(9), result.getDouble(8), 1e-5);
    }
  }

  @Test
  public void Query3Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(3);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getDate(1), result.getDate(0));
      assertEquals(rs.getInt(2), result.getInt(1));
      assertEquals(rs.getInt(3), result.getInt(2));
      assertEquals(rs.getDouble(4), result.getDouble(3), 1e-5);
    }
  }


  // count distinct
  @Test
  public void Query4Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(4);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getLong(1), result.getLong(0));
      assertEquals(rs.getString(2), result.getString(1));
    }
  }

  @Test
  public void Query5Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(5);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getDouble(2), result.getDouble(1), 1e-5);
    }
  }

  @Test
  public void Query6Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(6);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getDouble(1), result.getDouble(0), 1e-5);
    }
  }

  @Test
  public void Query7Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(7);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getString(2), result.getString(1));
      assertEquals(rs.getDouble(3), result.getDouble(2), 1e-5);
      assertEquals(rs.getDouble(4), result.getDouble(3), 1e-5);
    }
  }

  @Test
  public void Query8Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(8);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getDouble(2), result.getDouble(1), 1e-5);
      assertEquals(rs.getInt(3), result.getInt(2));
    }
  }

  @Test
  public void Query9Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(9);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getDouble(2), result.getDouble(1), 1e-5);
      assertEquals(rs.getInt(3), result.getInt(2));
    }
  }

  @Test
  public void Query10Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(10);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getDouble(1), result.getDouble(0), 1e-5);
      assertEquals(rs.getString(2), result.getString(1));
      assertEquals(rs.getString(3), result.getString(2));
      assertEquals(rs.getInt(4), result.getInt(3));
      assertEquals(rs.getString(5), result.getString(4));
      assertEquals(rs.getString(6), result.getString(5));
      assertEquals(rs.getString(7), result.getString(6));
      assertEquals(rs.getDouble(8), result.getDouble(7), 1e-5);
    }
  }

  @Test
  public void Query11Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(11);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getInt(1), result.getInt(0));
      assertEquals(rs.getDouble(2), result.getDouble(1), 1e-5);
    }
  }

  @Test
  public void Query12Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(12);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getString(2), result.getString(1));
      assertEquals(rs.getDouble(3), result.getDouble(2), 1e-5);
    }
  }

  // count distinct
  @Test
  public void Query13Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(13);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getInt(1), result.getInt(0));
      assertEquals(rs.getInt(2), result.getInt(1));
    }
  }

  @Test
  public void Query14Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(4);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getLong(1), result.getLong(0));
      assertEquals(rs.getString(2), result.getString(1));
    }
  }

  @Test
  public void Query15Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(15);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getInt(1), result.getInt(0));
      assertEquals(rs.getString(2), result.getString(1));
      assertEquals(rs.getString(3), result.getString(2));
      assertEquals(rs.getString(4), result.getString(3));
      assertEquals(rs.getDouble(5), result.getDouble(4), 1e-5);
    }
  }

  // count distinct
  @Test
  public void Query16Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(16);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getInt(1), result.getInt(0));
      assertEquals(rs.getString(2), result.getString(1));
      assertEquals(rs.getInt(3), result.getInt(2));
      assertEquals(rs.getString(4), result.getString(3));
    }
  }

  @Test
  public void Query17Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(17);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getDouble(1), result.getDouble(0), 1e-5);
    }
  }

  @Test
  public void Query18Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(18);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getInt(1), result.getInt(0));
      assertEquals(rs.getString(2), result.getString(1));
      assertEquals(rs.getDouble(3), result.getDouble(2), 1e-5);
      assertEquals(rs.getString(4), result.getString(3));
      assertEquals(rs.getInt(5), result.getInt(4));
      assertEquals(rs.getDouble(6), result.getDouble(5), 1e-5);
      assertEquals(rs.getDouble(7), result.getDouble(6), 1e-5);
    }
  }

  @Test
  public void Query19Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(19);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getDouble(1), result.getDouble(0), 1e-5);
    }
  }

  @Test
  public void Query20Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(20);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getString(2), result.getString(1));
    }
  }

  @Test
  public void Query21Test() throws VerdictDBException, SQLException, IOException {
    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(21);
    ResultSet rs = answerPair.getRight();
    VerdictSingleResult result = answerPair.getLeft();
    while (rs.next()) {
      result.next();
      assertEquals(rs.getString(1), result.getString(0));
      assertEquals(rs.getDouble(2), result.getDouble(1), 1e-5);
      assertEquals(rs.getDouble(3), result.getDouble(2), 1e-5);
    }
  }

  //  @Test
  //  public void Query22Test() throws VerdictDBException, SQLException, IOException {
  //    Pair<VerdictSingleResult, ResultSet> answerPair = getAnswer(22);
  //    ResultSet rs = answerPair.getRight();
  //    VerdictSingleResult result = answerPair.getLeft();
  //    while (rs.next()) {
  //      result.next();
  //      assertEquals(rs.getString(1), result.getString(0));
  //      assertEquals(rs.getDouble(2), result.getDouble(1), 1e-5);
  //      assertEquals(rs.getDouble(3), result.getDouble(2), 1e-5);
  //    }
  //  }
}
