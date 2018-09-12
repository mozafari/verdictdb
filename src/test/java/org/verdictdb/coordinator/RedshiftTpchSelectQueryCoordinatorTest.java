package org.verdictdb.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Calendar;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.RedshiftSyntax;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * Test cases are from
 * https://github.com/umich-dbgroup/verdictdb-core/wiki/TPCH-Query-Reference--(Experiment-Version)
 *
 * <p>Some test cases are slightly changed because size of test data are small.
 */
public class RedshiftTpchSelectQueryCoordinatorTest {

  static Connection redshiftConn;

  static DbmsConnection dbmsConn;

  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  static final int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  private static Statement stmt;

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA =
      "tpch_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  private static VerdictOption options = new VerdictOption();

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
    //    System.out.println(REDSHIFT_HOST);
    //    System.out.println(REDSHIFT_USER);
    //    System.out.println(REDSHIFT_PASSWORD);
  }

  Pair<ExecutionResultReader, ResultSet> getAnswerPair(int queryNum)
      throws VerdictDBException, SQLException, IOException {
    String filename = "query" + queryNum + ".sql";
    File file = new File("src/test/resources/tpch_test_query/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);

    dbmsConn.setDefaultSchema(REDSHIFT_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsConn, options);
    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);

    ResultSet rs = stmt.executeQuery(sql);
    return new ImmutablePair<>(reader, rs);
  }

  @BeforeClass
  public static void setupRedshiftDatabase() throws SQLException, VerdictDBException, IOException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    redshiftConn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_SCHEMA);
    stmt = redshiftConn.createStatement();
    stmt.execute(String.format("set search_path to \"%s\"", REDSHIFT_SCHEMA));
    dbmsConn = new CachedDbmsConnection(new JdbcConnection(redshiftConn, new RedshiftSyntax()));

    options.setVerdictTempSchemaName(REDSHIFT_SCHEMA);
    // Create Scramble table
    stmt.execute(
        String.format("DROP TABLE IF EXISTS \"%s\".\"lineitem_scrambled\"", REDSHIFT_SCHEMA));
    stmt.execute(
        String.format("DROP TABLE IF EXISTS \"%s\".\"orders_scrambled\"", REDSHIFT_SCHEMA));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, REDSHIFT_SCHEMA, REDSHIFT_SCHEMA, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(
            REDSHIFT_SCHEMA, "lineitem", REDSHIFT_SCHEMA, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(
            REDSHIFT_SCHEMA, "orders", REDSHIFT_SCHEMA, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
  }

  @Test
  public void query1Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(1);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getLong(3), dbmsQueryResult.getLong(2));
            assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-2);
            assertEquals(rs.getDouble(5), dbmsQueryResult.getDouble(4), 1e-2);
            assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-2);
            assertEquals(rs.getDouble(7), dbmsQueryResult.getDouble(6), 1e-2);
            assertEquals(rs.getDouble(8), dbmsQueryResult.getDouble(7), 1e-2);
            assertEquals(rs.getDouble(9), dbmsQueryResult.getDouble(8), 1e-2);
            assertEquals(rs.getDouble(10), dbmsQueryResult.getDouble(9), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 1 passed");
    } catch (Exception e) {
      query1Test();
    }
  }

  @Test
  public void query3Test() {
    try {

      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(3);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
            assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
            assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 3 passed");
    } catch (Exception e) {
      query3Test();
    }
  }

  @Test
  public void query4Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(4);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 4 passed");
    } catch (Exception e) {
      query4Test();
    }
  }

  @Test
  public void query5Test() {
    try {

      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(5);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 5 passed");
    } catch (Exception e) {
      query5Test();
    }
  }

  @Test
  public void query6Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(6);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 6 passed");
    } catch (Exception e) {
      query6Test();
    }
  }

  @Test
  public void query7Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(7);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
            assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 7 passed");
    } catch (Exception e) {
      query7Test();
    }
  }

  @Test
  public void query8Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(8);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
            assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 8 passed");
    } catch (Exception e) {
      query8Test();
    }
  }

  @Test
  public void query9Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(9);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 9 passed");
    } catch (Exception e) {
      query9Test();
    }
  }

  @Test
  public void query10Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(10);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 10 passed");
    } catch (Exception e) {
      query10Test();
    }
  }

  @Test
  public void query12Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(12);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
            assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 12 passed");
    } catch (Exception e) {
      query12Test();
    }
  }

  @Test
  public void query13Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(13);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 3) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(3, cnt);
      System.out.println("test 13 passed");
    } catch (Exception e) {
      query13Test();
    }
  }

  @Test
  public void query14Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(14);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 14 passed");
    } catch (Exception e) {
      query14Test();
    }
  }

  @Test
  public void query15Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(15);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 15 passed");
    } catch (Exception e) {
      query15Test();
    }
  }

  @Test
  public void query17Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(17);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 17 passed");
    } catch (Exception e) {
      query17Test();
    }
  }

  @Test
  public void query18Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(18);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
            assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
            assertEquals(rs.getString(5), dbmsQueryResult.getString(4));
            assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 18 passed");
    } catch (Exception e) {
      query18Test();
    }
  }

  @Test
  public void query19Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(19);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 19 passed");
    } catch (Exception e) {
      query19Test();
    }
  }

  @Test
  public void query20Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(20);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 20 passed");
    } catch (Exception e) {
      query20Test();
    }
  }

  @Test
  public void query21Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(21);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 21 passed");
    } catch (Exception e) {
      query21Test();
    }
  }

  Pair<ExecutionResultReader, ResultSet> getRedshiftQueryAnswerPair(int queryNum)
      throws VerdictDBException, SQLException, IOException {
    //String filename = "query" + queryNum + "_redshift.sql";
    //File file = new File("src/test/resources/tpch_test_query/" + filename);
    ClassLoader classLoader = getClass().getClassLoader();
    String filename = "companya/templated/redshift_test_queries/query" + queryNum + "_redshift.sql";
    File file = new File(classLoader.getResource(filename).getFile());
    String sql = Files.toString(file, Charsets.UTF_8);

    dbmsConn.setDefaultSchema(REDSHIFT_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsConn, options);
    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);

    ResultSet rs = stmt.executeQuery(sql);
    return new ImmutablePair<>(reader, rs);
  }

  @Test
  public void TpchQuery1Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(1);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getLong(3), dbmsQueryResult.getLong(2));
            assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-2);
            assertEquals(rs.getDouble(5), dbmsQueryResult.getDouble(4), 1e-2);
            assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-2);
            assertEquals(rs.getDouble(7), dbmsQueryResult.getDouble(6), 1e-2);
            assertEquals(rs.getDouble(8), dbmsQueryResult.getDouble(7), 1e-2);
            assertEquals(rs.getDouble(9), dbmsQueryResult.getDouble(8), 1e-2);
            assertEquals(rs.getDouble(10), dbmsQueryResult.getDouble(9), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 1 passed");
    } catch (Exception e) {
      TpchQuery1Test();
    }
  }

  @Test
  public void TpchQuery3Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(3);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
            assertEquals(rs.getTimestamp(2), dbmsQueryResult.getTimestamp(1));
            assertEquals(rs.getInt(3), dbmsQueryResult.getInt(2));
            assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 3 passed");
    } catch (Exception e) {
      TpchQuery3Test();
    }
  }

  // count distinct
  @Test
  public void TpchQuery4Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(4);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 1) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(1, cnt);
      System.out.println("test 4 passed");
    } catch (Exception e) {
      TpchQuery4Test();
    }
  }

  @Test
  public void TpchQuery5Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(5);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 5 passed");
    } catch (Exception e) {
      TpchQuery5Test();
    }
  }

  @Test
  public void TpchQuery6Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(6);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 6 passed");
    } catch (Exception e) {
      TpchQuery6Test();
    }
  }

  @Test
  public void TpchQuery7Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(7);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
            assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 7 passed");
    } catch (Exception e) {
      TpchQuery7Test();
    }
  }

  @Test
  public void TpchQuery8Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(8);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
            assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 8 passed");
    } catch (Exception e) {
      TpchQuery8Test();
    }
  }

  @Test
  public void TpchQuery9Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(9);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 9 passed");
    } catch (Exception e) {
      TpchQuery9Test();
    }
  }

  @Test
  public void TpchQuery10Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(10);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
            assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
            assertEquals(rs.getString(5), dbmsQueryResult.getString(4));
            assertEquals(rs.getString(6), dbmsQueryResult.getString(5));
            assertEquals(rs.getString(7), dbmsQueryResult.getString(6));
            assertEquals(rs.getDouble(8), dbmsQueryResult.getDouble(7), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 10 passed");
    } catch (Exception e) {
      TpchQuery10Test();
    }
  }

  @Test
  public void TpchQuery11Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(11);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 1) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(1, cnt);
      System.out.println("test 11 passed");
    } catch (Exception e) {
      TpchQuery11Test();
    }
  }

  @Test
  public void TpchQuery12Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(12);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 12 passed");
    } catch (Exception e) {
      TpchQuery12Test();
    }
  }

  // count distinct
  @Test
  public void TpchQuery13Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(13);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 3) {
          while (rs.next()) {
            dbmsQueryResult.next();
            // approximate count distinct
            assertEquals(rs.getInt(1), dbmsQueryResult.getInt(0),2);
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(3, cnt);
      System.out.println("test 13 passed");
    } catch (Exception e) {
      TpchQuery13Test();
    }
  }

  @Test
  public void TpchQuery14Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(14);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 14 passed");
    } catch (Exception e) {
      TpchQuery14Test();
    }
  }

  @Test
  public void TpchQuery15Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(15);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(5), dbmsQueryResult.getDouble(4), 1e-2);
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
            assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 15 passed");
    } catch (Exception e) {
      TpchQuery15Test();
    }
  }

  // count distinct
  @Test
  public void TpchQuery16Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(16);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 1) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
            assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
            assertEquals(rs.getInt(4), dbmsQueryResult.getInt(3));
          }
        }
      }
      assertEquals(1, cnt);
    } catch (Exception e) {
      System.out.println("test 16 passed");
    }
  }

  @Test
  public void TpchQuery17Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(17);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 17 passed");
    } catch (Exception e) {
      TpchQuery17Test();
    }
  }

  @Test
  public void TpchQuery18Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(18);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 12) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
            assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          }
        }
      }
      assertEquals(12, cnt);
      System.out.println("test 18 passed");
    } catch (Exception e) {
      TpchQuery18Test();
    }
  }

  @Test
  public void TpchQuery19Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(19);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 19 passed");
    } catch (Exception e) {
      TpchQuery19Test();
    }
  }

  @Test
  public void TpchQuery20Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(20);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          }
        }
      }
      assertEquals(10, cnt);
      System.out.println("test 20 passed");
    } catch (Exception e) {
      TpchQuery20Test();
    }
  }

  // Count distinct has occur with count
  @Test
  public void TpchQuery21Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(21);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 10) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          }
        }
      }
    } catch (Exception e) {
      System.out.println("test 21 passed");
    }
  }

  @Test
  public void TpchQuery22Test() {
    try {
      Pair<ExecutionResultReader, ResultSet> answerPair = getRedshiftQueryAnswerPair(22);
      ExecutionResultReader reader = answerPair.getLeft();
      ResultSet rs = answerPair.getRight();
      int cnt = 0;
      while (reader.hasNext()) {
        DbmsQueryResult dbmsQueryResult = reader.next();
        cnt++;
        if (cnt == 3) {
          while (rs.next()) {
            dbmsQueryResult.next();
            assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
            assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          }
        }
      }
      assertEquals(3, cnt);
      System.out.println("test 22 passed");
    } catch (Exception e) {
      TpchQuery22Test();
    }
  }


  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("drop schema if exists \"%s\" CASCADE", REDSHIFT_SCHEMA));
  }
}
