package org.verdictdb.jdbc41;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.sql.*;

import static org.junit.Assert.*;

public class JdbcResultSetMetaDataTestForRedshift {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA = "public";
  //      "resultset_metadata_test_" + RandomStringUtils.randomNumeric(3);

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  private static final String TABLE_NAME = "mytable";

  private static ResultSet expectedRs;

  //  Timestamp test1, test2;

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
    //    System.out.println(REDSHIFT_HOST);
    //    System.out.println(REDSHIFT_USER);
    //    System.out.println(REDSHIFT_PASSWORD);
  }

  @BeforeClass
  public static void setupRedshiftDatabase()
      throws SQLException, VerdictDBDbmsException, InterruptedException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    conn = DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    dbmsConn = JdbcConnection.create(conn);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP TABLE IF EXISTS \"%s\"", TABLE_NAME));
    //    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\"", REDSHIFT_SCHEMA));
    //    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", REDSHIFT_SCHEMA));

    // create a test table
    stmt.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\" ("
                + "smallintCol   SMALLINT, "
                + "int2Col       INT2,"
                + "integerCol    INTEGER,"
                + "int4Col       INT4,"
                + "intCol        INT, "
                + "bigintCol     BIGINT, "
                + "int8Col       INT8,"
                + "numericCol    NUMERIC,"
                + "decimalCol    DECIMAL, "
                + "realCol       REAL, "
                + "float4Col     FLOAT4,"
                + "doubleCol     DOUBLE PRECISION, "
                + "float8Col     FLOAT8,"
                + "floatCol      FLOAT,"
                + "booleanCol    BOOLEAN, "
                + "boolCol       BOOL,"
                + "characterCol  CHARACTER(4),"
                + "ncharCol      NCHAR(4),"
                + "bpcharCol     BPCHAR,"
                + "charCol       CHAR(4), "
                + "varcharCol    VARCHAR(4), "
                + "charvaryingCol CHARACTER VARYING(4),"
                + "nvarcharCol   NVARCHAR(4),"
                + "textCol       TEXT,"
                + "dateCol       DATE, "
                + "timestampCol  TIMESTAMP, "
                + "timestampwCol TIMESTAMP WITHOUT TIME ZONE,"
                + "timestamptzwCol TIMESTAMP WITH TIME ZONE,"
                + "timestamptzCol TIMESTAMPTZ"
                + ")",
            REDSHIFT_SCHEMA, TABLE_NAME));
    stmt.execute(
        String.format(
            "INSERT INTO %s.%s VALUES (1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, true, true, "
                + "'1234', '1234', '1234', '1234', '1234', '1234', '1234', '1234', '2018-12-31', '2018-12-31 00:00:01', "
                + "'2018-12-31 00:00:01', '2018-12-31 00:00:01', '2018-12-31 00:00:01'"
                + ")",
            REDSHIFT_SCHEMA, TABLE_NAME));
    stmt.execute(
        String.format(
            "INSERT INTO %s.%s VALUES (null, null, null, null, null, null, null, null, null, null, "
                + "null, null, null, null, null, null, "
                + "null, null, null, null, null, null, null, null, null, null, "
                + "null, null, null "
                + ")",
            REDSHIFT_SCHEMA, TABLE_NAME));

    expectedRs =
        stmt.executeQuery(String.format("select * from %s.%s", REDSHIFT_SCHEMA, TABLE_NAME));
  }

  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    //    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\"", REDSHIFT_SCHEMA));
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", REDSHIFT_SCHEMA, TABLE_NAME));
    dbmsConn.close();
  }

  @Test
  public void testColumnTypes() throws VerdictDBDbmsException, SQLException {
    expectedRs.next();
    Timestamp test1 = expectedRs.getTimestamp(28);
    Timestamp test2 = expectedRs.getTimestamp(29);
    String testStr = expectedRs.getString(20);
    String sql = String.format("select * from %s.%s", REDSHIFT_SCHEMA, TABLE_NAME);
    VerdictSingleResultFromDbmsQueryResult verdictResult = new VerdictSingleResultFromDbmsQueryResult(dbmsConn.execute(sql));
    ResultSet ourResult = new VerdictResultSet(verdictResult);
    ResultSetMetaData ourMetaData = ourResult.getMetaData();
    assertEquals(29, ourMetaData.getColumnCount());

    ResultSetMetaData expected = stmt.executeQuery(sql).getMetaData();
    assertEquals(expected.getColumnCount(), ourMetaData.getColumnCount());

    for (int i = 1; i <= ourMetaData.getColumnCount(); i++) {
      assertEquals(expected.getColumnType(i), ourMetaData.getColumnType(i));
    }

    while (ourResult.next()) {
      if (ourResult.getInt(2) == 1) {
        assertEquals(1, ourResult.getInt(2)); // int2
        assertEquals(1, ourResult.getLong(2)); // int2
        assertEquals(1, ourResult.getByte(2)); // int2
        assertEquals(true, ourResult.getBoolean(2)); // int2
        assertNotEquals(2, ourResult.getInt(2)); // int2
        assertNotEquals(2, ourResult.getLong(2)); // int2
        assertNotEquals(2, ourResult.getByte(2)); // int2
        assertEquals(true, ourResult.getBoolean(4)); // int4
        assertEquals(1, ourResult.getInt(4)); // int4
        assertEquals(1, ourResult.getLong(4)); // int4
        assertEquals(1, ourResult.getByte(4)); // int4
        assertEquals(true, ourResult.getBoolean(5)); // int
        assertEquals(1, ourResult.getInt(5)); // int
        assertEquals(1, ourResult.getLong(5)); // int
        assertEquals(1, ourResult.getByte(5)); // int
        assertEquals(true, ourResult.getBoolean(7)); // int8
        assertEquals(1, ourResult.getInt(7)); // int8
        assertEquals(1, ourResult.getLong(7)); // int8
        assertEquals(1, ourResult.getByte(7)); // int8
        assertEquals(true, ourResult.getBoolean(1)); // smallint
        assertEquals(1, ourResult.getInt(1)); // smallint
        assertEquals(1, ourResult.getLong(1)); // smallint
        assertEquals(1, ourResult.getByte(1)); // smallint
        assertEquals(true, ourResult.getBoolean(15)); // boolean
        assertEquals(1, ourResult.getInt(15)); // boolean
        assertEquals(1, ourResult.getLong(15)); // boolean
        assertEquals(1, ourResult.getByte(15)); // boolean
        assertEquals(true, ourResult.getBoolean(16)); // bool
        assertEquals(1, ourResult.getInt(16)); // bool
        assertEquals(1, ourResult.getLong(16)); // bool
        assertEquals(1, ourResult.getByte(16)); // bool
        assertEquals(true, ourResult.getBoolean(3)); // integer
        assertEquals(1, ourResult.getInt(3)); // integer
        assertEquals(1, ourResult.getLong(3)); // integer
        assertEquals(1, ourResult.getByte(3)); // integer
        assertEquals(true, ourResult.getBoolean(6)); // bigint
        assertEquals(1, ourResult.getInt(6)); // bigint
        assertEquals(1, ourResult.getLong(6)); // bigint
        assertEquals(1, ourResult.getByte(6)); // bigint

        try {
          assertEquals(true, ourResult.getBoolean(8)); // numeric
          fail();
        } catch (java.sql.SQLException e) {
        }
        assertEquals(1.0, ourResult.getFloat(8), 1e-6); // numeric
        assertEquals(1.0, ourResult.getDouble(8), 1e-6); // numeric
        assertEquals(1.0, ourResult.getByte(8), 1e-6); // numeric
        assertEquals(1.0, ourResult.getInt(8), 1e-6); // numeric
        assertEquals(1.0, ourResult.getLong(8), 1e-6); // numeric
        assertEquals(1.0, ourResult.getFloat(10), 1e-6); // real
        assertEquals(1.0, ourResult.getDouble(10), 1e-6); // real
        assertEquals(1.0, ourResult.getByte(10), 1e-6); // real
        assertEquals(1.0, ourResult.getInt(10), 1e-6); // real
        assertEquals(1.0, ourResult.getLong(10), 1e-6); // real
        assertEquals(1.0, ourResult.getFloat(13), 1e-6); // float8
        assertEquals(1.0, ourResult.getDouble(13), 1e-6); // float8
        assertEquals(1.0, ourResult.getByte(13), 1e-6); // float8
        assertEquals(1.0, ourResult.getInt(13), 1e-6); // float8
        assertEquals(1.0, ourResult.getLong(13), 1e-6); // float8
        assertEquals(1.0, ourResult.getFloat(12), 1e-6); // double precision
        assertEquals(1.0, ourResult.getDouble(12), 1e-6); // double precision
        assertEquals(1.0, ourResult.getByte(12), 1e-6); // double precision
        assertEquals(1.0, ourResult.getInt(12), 1e-6); // double precision
        assertEquals(1.0, ourResult.getLong(12), 1e-6); // double precision
        assertEquals(1.0, ourResult.getFloat(9), 1e-6); // decimal
        assertEquals(1.0, ourResult.getDouble(9), 1e-6); // decimal
        assertEquals(1.0, ourResult.getByte(9), 1e-6); // decimal
        assertEquals(1.0, ourResult.getInt(9), 1e-6); // decimal
        assertEquals(1.0, ourResult.getLong(9), 1e-6); // decimal
        assertEquals(1.0, ourResult.getFloat(14), 1e-6); // float
        assertEquals(1.0, ourResult.getDouble(14), 1e-6); // float
        assertEquals(1.0, ourResult.getByte(14), 1e-6); // float
        assertEquals(1.0, ourResult.getInt(14), 1e-6); // float
        assertEquals(1.0, ourResult.getLong(14), 1e-6); // float
        assertEquals(1.0, ourResult.getFloat(11), 1e-6); // float4
        assertEquals(1.0, ourResult.getDouble(11), 1e-6); // float4
        assertEquals(1.0, ourResult.getByte(11), 1e-6); // float4
        assertEquals(1.0, ourResult.getInt(11), 1e-6); // float4
        assertEquals(1.0, ourResult.getLong(11), 1e-6); // float4
        assertEquals(Timestamp.valueOf("2018-12-31 00:00:00"), ourResult.getTimestamp(25)); // date
        assertEquals(
            Timestamp.valueOf("2018-12-31 00:00:01"), ourResult.getTimestamp(26)); // timestamp
        assertEquals(
            Timestamp.valueOf("2018-12-31 00:00:01"),
            ourResult.getTimestamp(27)); // timestamp without tz
        assertEquals(test1, ourResult.getTimestamp(28)); // timestamptz with tz
        assertEquals(test2, ourResult.getTimestamp(29)); // timestamptz

        assertEquals("1234", ourResult.getString(20)); // char
        assertEquals("1234", ourResult.getString(18)); // nchar
        assertEquals("1234", ourResult.getString(19).replace(" ", "")); // bpchar
        assertEquals("1234", ourResult.getString(21)); // varchar
        assertEquals("1234", ourResult.getString(17)); // character
        assertEquals("1234", ourResult.getString(22)); // character varying
        assertEquals("1234", ourResult.getString(23)); // nvarchar
        assertEquals("1234", ourResult.getString(24)); // text
      } else {
        // NULL value
        assertEquals(0, ourResult.getInt(2)); // int2
        assertEquals(0, ourResult.getLong(2)); // int2
        assertEquals(0, ourResult.getByte(2)); // int2
        assertEquals(false, ourResult.getBoolean(2)); // int2
        assertEquals(false, ourResult.getBoolean(4)); // int4
        assertEquals(0, ourResult.getInt(4)); // int4
        assertEquals(0, ourResult.getLong(4)); // int4
        assertEquals(0, ourResult.getByte(4)); // int4
        assertEquals(false, ourResult.getBoolean(5)); // int
        assertEquals(0, ourResult.getInt(5)); // int
        assertEquals(0, ourResult.getLong(5)); // int
        assertEquals(0, ourResult.getByte(5)); // int
        assertEquals(false, ourResult.getBoolean(7)); // int8
        assertEquals(0, ourResult.getInt(7)); // int8
        assertEquals(0, ourResult.getLong(7)); // int8
        assertEquals(0, ourResult.getByte(7)); // int8
        assertEquals(false, ourResult.getBoolean(1)); // smallint
        assertEquals(0, ourResult.getInt(1)); // smallint
        assertEquals(0, ourResult.getLong(1)); // smallint
        assertEquals(0, ourResult.getByte(1)); // smallint
        assertEquals(false, ourResult.getBoolean(15)); // boolean
        assertEquals(0, ourResult.getInt(15)); // boolean
        assertEquals(0, ourResult.getLong(15)); // boolean
        assertEquals(0, ourResult.getByte(15)); // boolean
        assertEquals(false, ourResult.getBoolean(16)); // bool
        assertEquals(0, ourResult.getInt(16)); // bool
        assertEquals(0, ourResult.getLong(16)); // bool
        assertEquals(0, ourResult.getByte(16)); // bool
        assertEquals(false, ourResult.getBoolean(3)); // integer
        assertEquals(0, ourResult.getInt(3)); // integer
        assertEquals(0, ourResult.getLong(3)); // integer
        assertEquals(0, ourResult.getByte(3)); // integer
        assertEquals(false, ourResult.getBoolean(6)); // bigint
        assertEquals(0, ourResult.getInt(6)); // bigint
        assertEquals(0, ourResult.getLong(6)); // bigint
        assertEquals(0, ourResult.getByte(6)); // bigint

        assertEquals(0, ourResult.getFloat(8), 1e-6); // numeric
        assertEquals(0, ourResult.getDouble(8), 1e-6); // numeric
        assertEquals(0, ourResult.getByte(8), 1e-6); // numeric
        assertEquals(0, ourResult.getInt(8), 1e-6); // numeric
        assertEquals(0, ourResult.getLong(8), 1e-6); // numeric
        assertEquals(0, ourResult.getFloat(10), 1e-6); // real
        assertEquals(0, ourResult.getDouble(10), 1e-6); // real
        assertEquals(0, ourResult.getByte(10), 1e-6); // real
        assertEquals(0, ourResult.getInt(10), 1e-6); // real
        assertEquals(0, ourResult.getLong(10), 1e-6); // real
        assertEquals(0, ourResult.getFloat(13), 1e-6); // float8
        assertEquals(0, ourResult.getDouble(13), 1e-6); // float8
        assertEquals(0, ourResult.getByte(13), 1e-6); // float8
        assertEquals(0, ourResult.getInt(13), 1e-6); // float8
        assertEquals(0, ourResult.getLong(13), 1e-6); // float8
        assertEquals(0, ourResult.getFloat(12), 1e-6); // double precision
        assertEquals(0, ourResult.getDouble(12), 1e-6); // double precision
        assertEquals(0, ourResult.getByte(12), 1e-6); // double precision
        assertEquals(0, ourResult.getInt(12), 1e-6); // double precision
        assertEquals(0, ourResult.getLong(12), 1e-6); // double precision
        assertEquals(0, ourResult.getFloat(9), 1e-6); // decimal
        assertEquals(0, ourResult.getDouble(9), 1e-6); // decimal
        assertEquals(0, ourResult.getByte(9), 1e-6); // decimal
        assertEquals(0, ourResult.getInt(9), 1e-6); // decimal
        assertEquals(0, ourResult.getLong(9), 1e-6); // decimal
        assertEquals(0, ourResult.getFloat(14), 1e-6); // float
        assertEquals(0, ourResult.getDouble(14), 1e-6); // float
        assertEquals(0, ourResult.getByte(14), 1e-6); // float
        assertEquals(0, ourResult.getInt(14), 1e-6); // float
        assertEquals(0, ourResult.getLong(14), 1e-6); // float
        assertEquals(0, ourResult.getFloat(11), 1e-6); // float4
        assertEquals(0, ourResult.getDouble(11), 1e-6); // float4
        assertEquals(0, ourResult.getByte(11), 1e-6); // float4
        assertEquals(0, ourResult.getInt(11), 1e-6); // float4
        assertEquals(0, ourResult.getLong(11), 1e-6); // float4
        assertEquals(null, ourResult.getTimestamp(25)); // date
        assertEquals(null, ourResult.getTimestamp(26)); // timestamp
        assertEquals(null, ourResult.getTimestamp(27)); // timestamp without tz
        assertEquals(null, ourResult.getTimestamp(28)); // timestamptz with tz
        assertEquals(null, ourResult.getTimestamp(29)); // timestamptz

        assertEquals(null, ourResult.getString(20)); // char
        assertEquals(null, ourResult.getString(18)); // nchar
        assertEquals(null, ourResult.getString(19)); // bpchar
        assertEquals(null, ourResult.getString(21)); // varchar
        assertEquals(null, ourResult.getString(17)); // character
        assertEquals(null, ourResult.getString(22)); // character varying
        assertEquals(null, ourResult.getString(23)); // nvarchar
        assertEquals(null, ourResult.getString(24)); // text
      }
    }
  }
}
