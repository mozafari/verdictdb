package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import org.verdictdb.exception.VerdictDBDbmsException;

/**
 * Tests if our JDBC driver returns expected values when working with Impala JDBC41 driver.
 *
 * Impala data types: https://www.cloudera.com/documentation/enterprise/5-9-x/topics/impala_datatypes.html
 *
 * @author Yongjoo Park
 *
 */
public class JdbcResultSetMetaDataTestForImpala {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String IMPALA_HOST;

  private static final String IMPALA_DATABASE = 
      "metadata_test_" + RandomStringUtils.randomNumeric(3);;

  private static final String IMPALA_UESR = "";

  private static final String IMPALA_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:impala://%s", IMPALA_HOST);
    conn = DriverManager.getConnection(connectionString, IMPALA_UESR, IMPALA_PASSWORD);
    dbmsConn = JdbcConnection.create(conn);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", IMPALA_DATABASE));
    stmt.execute(String.format(
        "CREATE TABLE `%s`.`%s` ("
            + "tinyintCol    TINYINT, "
            + "smallintCol   SMALLINT, "
            + "intCol        INT, "
            + "bigintCol     BIGINT, "
            + "decimalCol    DECIMAL, "
            + "boolCol       BOOLEAN, "
            + "floatCol      FLOAT, "
            + "doubleCol     DOUBLE, "
            + "stringCol     STRING, "
            + "timestampCol  TIMESTAMP "
            + ")",
            IMPALA_DATABASE, TABLE_NAME));

    stmt.execute(String.format("INSERT INTO `%s`.`%s` VALUES ("
        + "1, 1, 1, 1, 1, true, "
        + "1.0, 1.0, 'abc', "
        + "'2018-12-31 00:00:01')",
        IMPALA_DATABASE, TABLE_NAME));

    stmt.execute(String.format("INSERT INTO `%s`.`%s` VALUES ("
        + "2, NULL, NULL, NULL, NULL, NULL, "
        + "NULL, NULL, NULL, NULL)", IMPALA_DATABASE, TABLE_NAME));

//    conn.createStatement().execute(String.format("INSERT INTO `%s` VALUES ("
//        + "1, true, 1, 1, 1, 1, "
//        + "1.0, 1.0, "
//        + "'2018-12-31 00:00:01', "
//        + "'abc', 'abc')",
//        TABLE_NAME));

//    conn.createStatement().execute(String.format("INSERT INTO `%s` VALUES ( "
//        + "NULL, NULL, NULL, NULL, NULL, NULL, "
//        + "NULL, NULL, "
//        + "NULL, "
//        + "NULL, NULL)",
//        TABLE_NAME));
  }

  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    dbmsConn.close();
  }

  @Test
  public void testColumnTypes() throws VerdictDBDbmsException, SQLException {
    String sql = String.format("select * from `%s`.`%s` order by tinyintCol",
        IMPALA_DATABASE, TABLE_NAME);

    ResultSet  expectedResult = conn.createStatement().executeQuery(sql);
    ResultSetMetaData expectedMeta = expectedResult.getMetaData();

    DbmsQueryResult internalResult = dbmsConn.execute(sql);
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(internalResult);
    ResultSet ourResult = new VerdictResultSet(result);
    ResultSetMetaData ourMetaData = ourResult.getMetaData();

    assertEquals(expectedMeta.getColumnCount(), ourMetaData.getColumnCount());

    for (int i = 1; i <= ourMetaData.getColumnCount(); i++) {
      assertEquals(expectedMeta.getColumnType(i), ourMetaData.getColumnType(i));
    }

//    expectedResult.next();
//    internalResult.printContent();
    ourResult.next();

//    for (int i = 1; i <= 10; i++) {
//      System.out.println(ourResult.getObject(i));
//    }
//    System.out.println(expectedResult.getBoolean(1));

    assertEquals(1, ourResult.getInt(1));         // tinyint
    assertEquals(1, ourResult.getLong(1));        // tinyint
    assertEquals(1, ourResult.getByte(1));        // tinyint
    assertNotEquals(2, ourResult.getInt(1));      // tinyint
    assertNotEquals(2, ourResult.getLong(1));     // tinyint
    assertNotEquals(2, ourResult.getByte(1));     // tinyint

    assertEquals(true, ourResult.getBoolean(2));  // smallint
    assertEquals(1, ourResult.getInt(2));         // smallint
    assertEquals(1, ourResult.getLong(2));        // smallint
    assertEquals(1, ourResult.getByte(2));        // smallint

    assertEquals(true, ourResult.getBoolean(3));  // int
    assertEquals(1, ourResult.getInt(3));         // int
    assertEquals(1, ourResult.getLong(3));        // int
    assertEquals(1, ourResult.getByte(3));        // int

    assertEquals(true, ourResult.getBoolean(4));  // bigint
    assertEquals(1, ourResult.getInt(4));         // bigint
    assertEquals(1, ourResult.getLong(4));        // bigint
    assertEquals(1, ourResult.getByte(4));        // bigint

    assertEquals(1.0, ourResult.getFloat(5), 1e-6);         // decimal
    assertEquals(1.0, ourResult.getDouble(5), 1e-6);        // decimal
    assertEquals(1.0, ourResult.getByte(5), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getInt(5), 1e-6);           // decimal
    assertEquals(1.0, ourResult.getLong(5), 1e-6);          // decimal

    assertEquals(true, ourResult.getBoolean(6));  // bool
    assertEquals(1, ourResult.getInt(6));         // bool
    assertEquals(1, ourResult.getLong(6));        // bool
    assertEquals(1, ourResult.getByte(6));        // bool

    assertEquals(1.0, ourResult.getFloat(7), 1e-6);         // float
    assertEquals(1.0, ourResult.getDouble(7), 1e-6);        // float
    assertEquals(1.0, ourResult.getByte(7), 1e-6);          // float
    assertEquals(1.0, ourResult.getInt(7), 1e-6);           // float
    assertEquals(1.0, ourResult.getLong(7), 1e-6);          // float
    assertEquals(1.0, ourResult.getFloat(8), 1e-6);         // double
    assertEquals(1.0, ourResult.getDouble(8), 1e-6);        // double
    assertEquals(1.0, ourResult.getByte(8), 1e-6);          // double
    assertEquals(1.0, ourResult.getInt(8), 1e-6);           // double
    assertEquals(1.0, ourResult.getLong(8), 1e-6);          // double

    assertEquals("abc", ourResult.getString(9));            // string
    assertEquals(Timestamp.valueOf("2018-12-31 00:00:01"), ourResult.getTimestamp(10));  // timestamp
    assertEquals("2018-12-31 00:00:01.0", ourResult.getString(10));  // timestamp

    // null values
    ourResult.next();

    assertEquals(2, ourResult.getInt(1));         // tinyint
    assertEquals(2, ourResult.getLong(1));        // tinyint
    assertEquals(2, ourResult.getByte(1));        // tinyint

    assertEquals(false, ourResult.getBoolean(2));  // smallint
    assertEquals(0, ourResult.getInt(2));         // smallint
    assertEquals(0, ourResult.getLong(2));        // smallint
    assertEquals(0, ourResult.getByte(2));        // smallint

    assertEquals(false, ourResult.getBoolean(3));  // int
    assertEquals(0, ourResult.getInt(3));         // int
    assertEquals(0, ourResult.getLong(3));        // int
    assertEquals(0, ourResult.getByte(3));        // int

    assertEquals(false, ourResult.getBoolean(4));  // bigint
    assertEquals(0, ourResult.getInt(4));         // bigint
    assertEquals(0, ourResult.getLong(4));        // bigint
    assertEquals(0, ourResult.getByte(4));        // bigint

    assertEquals(0, ourResult.getFloat(5), 1e-6);         // decimal
    assertEquals(0, ourResult.getDouble(5), 1e-6);        // decimal
    assertEquals(0, ourResult.getByte(5), 1e-6);          // decimal
    assertEquals(0, ourResult.getInt(5), 1e-6);           // decimal
    assertEquals(0, ourResult.getLong(5), 1e-6);          // decimal

    assertEquals(false, ourResult.getBoolean(6));  // bool
    assertEquals(0, ourResult.getInt(6));          // bool
    assertEquals(0, ourResult.getLong(6));         // bool
    assertEquals(0, ourResult.getByte(6));         // bool

    assertEquals(0, ourResult.getFloat(7), 1e-6);         // float
    assertEquals(0, ourResult.getDouble(7), 1e-6);        // float
    assertEquals(0, ourResult.getByte(7), 1e-6);          // float
    assertEquals(0, ourResult.getInt(7), 1e-6);           // float
    assertEquals(0, ourResult.getLong(7), 1e-6);          // float
    assertEquals(0, ourResult.getFloat(8), 1e-6);         // double
    assertEquals(0, ourResult.getDouble(8), 1e-6);        // double
    assertEquals(0, ourResult.getByte(8), 1e-6);          // double
    assertEquals(0, ourResult.getInt(8), 1e-6);           // double
    assertEquals(0, ourResult.getLong(8), 1e-6);          // double

    assertEquals(null, ourResult.getString(9));            // string
    assertEquals(null, ourResult.getTimestamp(10));        // timestamp

    ourResult.close();
  }


}
