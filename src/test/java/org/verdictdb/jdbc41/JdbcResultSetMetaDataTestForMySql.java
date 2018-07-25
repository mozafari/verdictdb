package org.verdictdb.jdbc41;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import org.verdictdb.exception.VerdictDBDbmsException;

/**
 * This intends to test the correct operations of VerdictDB JDBC driver's get methods for every
 * different data type supported by MySQL.
 * 
 * @author Yongjoo Park
 *
 */
public class JdbcResultSetMetaDataTestForMySql {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    dbmsConn = JdbcConnection.create(conn);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    stmt.execute(String.format(
        "CREATE TABLE %s ("
            + "bitCol        BIT(1), "
            + "tinyintCol    TINYINT(2), "
            + "boolCol       BOOL, "
            + "smallintCol   SMALLINT(3), "
            + "mediumintCol  MEDIUMINT(4), "
            + "intCol        INT(4), "
            + "integerCol    INTEGER(4), "
            + "bigintCol     BIGINT(8), "
            + "decimalCol    DECIMAL(4,2), "
            + "decCol        DEC(4,2), "
            + "floatCol      FLOAT(4,2), "
            + "doubleCol     DOUBLE(8,2), "
            + "doubleprecisionCol DOUBLE PRECISION(8,2), "
            + "dateCol       DATE, "
            + "datetimeCol   DATETIME, "
            + "timestampCol  TIMESTAMP, "
            + "timeCol       TIME, "
            + "yearCol       YEAR(2), "
            + "yearCol2      YEAR(4), "
            + "charCol       CHAR(4), "
            + "varcharCol    VARCHAR(4), "
            + "binaryCol     BINARY(4), "
            + "varbinaryCol  VARBINARY(4), "
            + "tinyblobCol   TINYBLOB, "
            + "tinytextCol   TINYTEXT, "
            + "blobCol       BLOB(4), "
            + "textCol       TEXT(100), "
            + "medimumblobCol MEDIUMBLOB, "
            + "medimumtextCol MEDIUMTEXT, "
            + "longblobCol   LONGBLOB, "
            + "longtextCol   LONGTEXT, "
            + "enumCol       ENUM('1', '2'), "
            + "setCol        SET('1', '2'))"
            , TABLE_NAME));

    stmt.execute(String.format("INSERT INTO %s VALUES ( "
        + "1, 2, 1, 1, 1, 1, 1, 1, "
        + "1.0, 1.0, 1.0, 1.0, 1.0, "
        + "'2018-12-31', '2018-12-31 01:00:00', '2018-12-31 00:00:01', '10:59:59', "
        + "18, 2018, 'abc', 'abc', '10', '10', "
        + "'10', 'a', '10', 'abc', '1110', 'abc', '1110', 'abc', '1', '2')",
        TABLE_NAME));

    stmt.execute(String.format("INSERT INTO %s VALUES ( "
        + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
        + "NULL, NULL, NULL, NULL, NULL, "
        + "NULL, NULL, NULL, NULL, "
        + "NULL, NULL, NULL, NULL, NULL, NULL, "
        + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        TABLE_NAME));
  }

  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    dbmsConn.close();
  }

  @Test
  public void testColumnTypes() throws VerdictDBDbmsException, SQLException, IOException {
    String sql = String.format("select * from %s", TABLE_NAME);
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(dbmsConn.execute(sql));
    ResultSet ourResult = new VerdictResultSet(result);
    ResultSetMetaData ourMetaData = ourResult.getMetaData();
    assertEquals(33, ourMetaData.getColumnCount());

    ResultSet expectedResult = stmt.executeQuery(sql);
    ResultSetMetaData expectedMeta = expectedResult.getMetaData();
    assertEquals(expectedMeta.getColumnCount(), ourMetaData.getColumnCount());

    for (int i = 1; i <= ourMetaData.getColumnCount(); i++) {
      assertEquals(expectedMeta.getColumnType(i), ourMetaData.getColumnType(i));
    }

    expectedResult.next();
    ourResult.next();
    assertEquals(true, ourResult.getBoolean(1));  // bit
    assertEquals(1, ourResult.getByte(1));        // bit
    assertEquals(2, ourResult.getInt(2));         // tinyint
    assertEquals(2, ourResult.getLong(2));        // tinyint
    assertEquals(2, ourResult.getByte(2));        // tinyint
    assertNotEquals(1, ourResult.getInt(2));      // tinyint
    assertNotEquals(1, ourResult.getLong(2));     // tinyint
    assertNotEquals(1, ourResult.getByte(2));     // tinyint
    assertEquals(true, ourResult.getBoolean(3));  // bool
    assertEquals(1, ourResult.getInt(3));         // bool
    assertEquals(1, ourResult.getLong(3));        // bool
    assertEquals(1, ourResult.getByte(3));        // bool
    assertEquals(true, ourResult.getBoolean(4));  // smallint
    assertEquals(1, ourResult.getInt(4));         // smallint
    assertEquals(1, ourResult.getLong(4));        // smallint
    assertEquals(1, ourResult.getByte(4));        // smallint
    assertEquals(true, ourResult.getBoolean(5));  // mediumint
    assertEquals(1, ourResult.getInt(5));         // mediumint
    assertEquals(1, ourResult.getLong(5));        // mediumint
    assertEquals(1, ourResult.getByte(5));        // mediumint
    assertEquals(true, ourResult.getBoolean(6));  // int
    assertEquals(1, ourResult.getInt(6));         // int
    assertEquals(1, ourResult.getLong(6));        // int
    assertEquals(1, ourResult.getByte(6));        // int
    assertEquals(true, ourResult.getBoolean(7));  // integer
    assertEquals(1, ourResult.getInt(7));         // integer
    assertEquals(1, ourResult.getLong(7));        // integer
    assertEquals(1, ourResult.getByte(7));        // integer
    assertEquals(true, ourResult.getBoolean(8));  // bigint
    assertEquals(1, ourResult.getInt(8));         // bigint
    assertEquals(1, ourResult.getLong(8));        // bigint
    assertEquals(1, ourResult.getByte(8));        // bigint

    try {
      assertEquals(true, ourResult.getBoolean(9));          // decimal
      fail();
    } catch (java.sql.SQLException e) {}
    assertEquals(1.0, ourResult.getFloat(9), 1e-6);         // decimal
    assertEquals(1.0, ourResult.getDouble(9), 1e-6);        // decimal
    assertEquals(1.0, ourResult.getByte(9), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getInt(9), 1e-6);           // decimal
    assertEquals(1.0, ourResult.getLong(9), 1e-6);          // decimal
    try {
      assertEquals(true, ourResult.getBoolean(10));          // dec
      fail();
    } catch (java.sql.SQLException e) {}
    assertEquals(1.0, ourResult.getFloat(10), 1e-6);         // dec
    assertEquals(1.0, ourResult.getDouble(10), 1e-6);        // dec
    assertEquals(1.0, ourResult.getByte(10), 1e-6);          // dec
    assertEquals(1.0, ourResult.getInt(10), 1e-6);           // dec
    assertEquals(1.0, ourResult.getLong(10), 1e-6);          // dec
    assertEquals(1.0, ourResult.getFloat(11), 1e-6);         // float
    assertEquals(1.0, ourResult.getDouble(11), 1e-6);        // float
    assertEquals(1.0, ourResult.getByte(11), 1e-6);          // float
    assertEquals(1.0, ourResult.getInt(11), 1e-6);           // float
    assertEquals(1.0, ourResult.getLong(11), 1e-6);          // float
    assertEquals(1.0, ourResult.getFloat(12), 1e-6);         // double
    assertEquals(1.0, ourResult.getDouble(12), 1e-6);        // double
    assertEquals(1.0, ourResult.getByte(12), 1e-6);          // double
    assertEquals(1.0, ourResult.getInt(12), 1e-6);           // double
    assertEquals(1.0, ourResult.getLong(12), 1e-6);          // double
    assertEquals(1.0, ourResult.getFloat(13), 1e-6);         // double precision
    assertEquals(1.0, ourResult.getDouble(13), 1e-6);        // double precision
    assertEquals(1.0, ourResult.getByte(13), 1e-6);          // double precision
    assertEquals(1.0, ourResult.getInt(13), 1e-6);           // double precision
    assertEquals(1.0, ourResult.getLong(13), 1e-6);          // double precision

    assertEquals(Timestamp.valueOf("2018-12-31 00:00:00"), ourResult.getTimestamp(14));  // date
    assertEquals(Timestamp.valueOf("2018-12-31 01:00:00"), ourResult.getTimestamp(15));  // datetime
    assertEquals(Timestamp.valueOf("2018-12-31 00:00:01"), ourResult.getTimestamp(16));  // timestamp
    assertEquals(Time.valueOf("10:59:59"), ourResult.getTime(17));                       // time
    assertEquals(Timestamp.valueOf("1970-01-01 10:59:59"), ourResult.getTimestamp(17));  // time
    assertEquals(Date.valueOf("2018-01-01"), ourResult.getDate(18));                     // year(2)
    assertEquals(Timestamp.valueOf("2018-01-01 00:00:00"), ourResult.getTimestamp(18));  // year(2)
    assertEquals(Date.valueOf("2018-01-01"), ourResult.getDate(19));                     // year(4)
    assertEquals(Timestamp.valueOf("2018-01-01 00:00:00"), ourResult.getTimestamp(19));  // year(4)

    assertEquals("abc", ourResult.getString(20));             // char
    assertEquals("abc", ourResult.getString(21));             // varchar
    assertArrayEquals(expectedResult.getBytes(22), ourResult.getBytes(22));              // binary
    assertTrue(IOUtils.contentEquals(
        expectedResult.getBinaryStream(22), ourResult.getBinaryStream(22)));             // binary
    assertArrayEquals(expectedResult.getBytes(23), ourResult.getBytes(23));              // varbinary
    assertTrue(IOUtils.contentEquals(
        expectedResult.getBinaryStream(23), ourResult.getBinaryStream(23)));             // varbinary

    assertTrue(IOUtils.contentEquals(
        expectedResult.getBlob(24).getBinaryStream(), ourResult.getBlob(24).getBinaryStream()));  // tinyblob
    assertEquals(expectedResult.getString(25), ourResult.getString(25));      // tinytext
    assertTrue(IOUtils.contentEquals(
        expectedResult.getBlob(26).getBinaryStream(), ourResult.getBlob(26).getBinaryStream()));  // blob
    assertEquals(expectedResult.getString(27), ourResult.getString(27));      // text
    assertTrue(IOUtils.contentEquals(
        expectedResult.getBlob(28).getBinaryStream(), ourResult.getBlob(28).getBinaryStream()));  // mediumblob
    assertEquals(expectedResult.getString(29), ourResult.getString(29));      // mediumtext
    assertTrue(IOUtils.contentEquals(
        expectedResult.getBlob(30).getBinaryStream(), ourResult.getBlob(30).getBinaryStream()));  // longblob
    assertEquals(expectedResult.getString(31), ourResult.getString(31));      // longtext

    assertEquals(expectedResult.getString(32), ourResult.getString(32));      // enum
    assertEquals(expectedResult.getString(33), ourResult.getString(33));      // set

    // null values
    expectedResult.next();
    ourResult.next();
    assertEquals(false, ourResult.getBoolean(1)); // bit
    assertEquals(0, ourResult.getByte(1));        // bit
    assertEquals(0, ourResult.getInt(2));         // tinyint
    assertEquals(0, ourResult.getLong(2));        // tinyint
    assertEquals(0, ourResult.getByte(2));        // tinyint
    assertNotEquals(1, ourResult.getInt(2));      // tinyint
    assertNotEquals(1, ourResult.getLong(2));     // tinyint
    assertNotEquals(1, ourResult.getByte(2));     // tinyint
    assertEquals(false, ourResult.getBoolean(3));  // bool
    assertEquals(0, ourResult.getInt(3));         // bool
    assertEquals(0, ourResult.getLong(3));        // bool
    assertEquals(0, ourResult.getByte(3));        // bool
    assertEquals(false, ourResult.getBoolean(4));  // smallint
    assertEquals(0, ourResult.getInt(4));         // smallint
    assertEquals(0, ourResult.getLong(4));        // smallint
    assertEquals(0, ourResult.getByte(4));        // smallint
    assertEquals(false, ourResult.getBoolean(5));  // mediumint
    assertEquals(0, ourResult.getInt(5));         // mediumint
    assertEquals(0, ourResult.getLong(5));        // mediumint
    assertEquals(0, ourResult.getByte(5));        // mediumint
    assertEquals(false, ourResult.getBoolean(6));  // int
    assertEquals(0, ourResult.getInt(6));         // int
    assertEquals(0, ourResult.getLong(6));        // int
    assertEquals(0, ourResult.getByte(6));        // int
    assertEquals(false, ourResult.getBoolean(7));  // integer
    assertEquals(0, ourResult.getInt(7));         // integer
    assertEquals(0, ourResult.getLong(7));        // integer
    assertEquals(0, ourResult.getByte(7));        // integer
    assertEquals(false, ourResult.getBoolean(8));  // bigint
    assertEquals(0, ourResult.getInt(8));         // bigint
    assertEquals(0, ourResult.getLong(8));        // bigint
    assertEquals(0, ourResult.getByte(8));        // bigint

    assertEquals(false, ourResult.getBoolean(9));           // decimal
    assertEquals(0.0, ourResult.getFloat(9), 1e-6);         // decimal
    assertEquals(0.0, ourResult.getDouble(9), 1e-6);        // decimal
    assertEquals(0.0, ourResult.getByte(9), 1e-6);          // decimal
    assertEquals(0.0, ourResult.getInt(9), 1e-6);           // decimal
    assertEquals(0.0, ourResult.getLong(9), 1e-6);          // decimal
    assertEquals(false, ourResult.getBoolean(10));          // dec
    assertEquals(0.0, ourResult.getFloat(10), 1e-6);         // dec
    assertEquals(0.0, ourResult.getDouble(10), 1e-6);        // dec
    assertEquals(0.0, ourResult.getByte(10), 1e-6);          // dec
    assertEquals(0.0, ourResult.getInt(10), 1e-6);           // dec
    assertEquals(0.0, ourResult.getLong(10), 1e-6);          // dec
    assertEquals(0.0, ourResult.getFloat(11), 1e-6);         // float
    assertEquals(0.0, ourResult.getDouble(11), 1e-6);        // float
    assertEquals(0.0, ourResult.getByte(11), 1e-6);          // float
    assertEquals(0.0, ourResult.getInt(11), 1e-6);           // float
    assertEquals(0.0, ourResult.getLong(11), 1e-6);          // float
    assertEquals(0.0, ourResult.getFloat(12), 1e-6);         // double
    assertEquals(0.0, ourResult.getDouble(12), 1e-6);        // double
    assertEquals(0.0, ourResult.getByte(12), 1e-6);          // double
    assertEquals(0.0, ourResult.getInt(12), 1e-6);           // double
    assertEquals(0.0, ourResult.getLong(12), 1e-6);          // double
    assertEquals(0.0, ourResult.getFloat(13), 1e-6);         // double precision
    assertEquals(0.0, ourResult.getDouble(13), 1e-6);        // double precision
    assertEquals(0.0, ourResult.getByte(13), 1e-6);          // double precision
    assertEquals(0.0, ourResult.getInt(13), 1e-6);           // double precision
    assertEquals(0.0, ourResult.getLong(13), 1e-6);          // double precision

    assertEquals(expectedResult.getTimestamp(14), ourResult.getTimestamp(14));  // date
    assertEquals(expectedResult.getTimestamp(15), ourResult.getTimestamp(15));  // datetime
    assertEquals(expectedResult.getTimestamp(16), ourResult.getTimestamp(16));  // timestamp
    assertEquals(expectedResult.getTime(17),      ourResult.getTime(17));       // time
    assertEquals(expectedResult.getTimestamp(17), ourResult.getTimestamp(17));  // time
    assertEquals(expectedResult.getDate(18),      ourResult.getDate(18));       // year(2)
    assertEquals(expectedResult.getTimestamp(18), ourResult.getTimestamp(18));  // year(2)
    assertEquals(expectedResult.getDate(19),      ourResult.getDate(19));       // year(4)
    assertEquals(expectedResult.getTimestamp(19), ourResult.getTimestamp(19));  // year(4)

    assertEquals(expectedResult.getString(20), ourResult.getString(20));        // char
    assertEquals(expectedResult.getString(21), ourResult.getString(21));        // varchar
    assertArrayEquals(expectedResult.getBytes(22), ourResult.getBytes(22));     // binary
    assertEquals(0, ourResult.getBinaryStream(22).available());                 // binary
    assertArrayEquals(expectedResult.getBytes(23), ourResult.getBytes(23));     // varbinary
    assertEquals(0, ourResult.getBinaryStream(23).available());                 // varbinary

    assertEquals(expectedResult.getBlob(24), ourResult.getBlob(24));            // tinyblob
    assertEquals(expectedResult.getString(25), ourResult.getString(25));        // tinytext
    assertEquals(expectedResult.getBlob(26), ourResult.getBlob(26));            // blob
    assertEquals(expectedResult.getString(27), ourResult.getString(27));        // text
    assertEquals(expectedResult.getBlob(28), ourResult.getBlob(28));            // mediumblob
    assertEquals(expectedResult.getString(29), ourResult.getString(29));        // mediumtext
    assertEquals(expectedResult.getBlob(30), ourResult.getBlob(30));            // longblob
    assertEquals(expectedResult.getString(31), ourResult.getString(31));        // longtext

    assertEquals(expectedResult.getString(32), ourResult.getString(32));        // enum
    assertEquals(expectedResult.getString(33), ourResult.getString(33));        // set

    ourResult.close();
  }

}
