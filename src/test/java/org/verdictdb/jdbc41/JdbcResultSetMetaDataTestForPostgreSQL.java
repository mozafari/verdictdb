package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;

public class JdbcResultSetMetaDataTestForPostgreSQL {

  static Connection conn;

  static DbmsConnection dbmsConn;

  private static Statement stmt;

  private static final String POSTGRESQL_HOST;

  private static final String POSTGRESQL_DATABASE = "test";

  private static final String POSTGRESQL_USER = "postgres";

  private static final String POSTGRESQL_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      POSTGRESQL_HOST = "postgres";
    } else {
      POSTGRESQL_HOST = "localhost";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException {
    String mysqlConnectionString =
        String.format("jdbc:postgresql://%s/%s", POSTGRESQL_HOST, POSTGRESQL_DATABASE);
    conn = DriverManager.getConnection(mysqlConnectionString, POSTGRESQL_USER, POSTGRESQL_PASSWORD);
    dbmsConn = new JdbcConnection(conn, new PostgresqlSyntax());

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    stmt.execute(String.format(
        "CREATE TABLE %s ("
            + "bigintCol      bigint, "
            + "bigserialCol   bigserial, "
            + "bitCol         bit(1), "
            + "varbitCol      varbit(4), "
            + "booleanCol     boolean, "
            + "boxCol         box, "
            + "byteaCol       bytea, "
            + "charCol        char(4), "
            + "varcharCol     varchar(4), "
            + "cidrCol        cidr, "
            + "circleCol      circle, "
            + "dateCol        date, "
            + "float8Col      float8, "
            + "inetCol        inet, "
            + "integerCol     integer, "
            + "jsonCol        json, "
            + "lineCol        line, "
            + "lsegCol        lseg, "
            + "macaddrCol     macaddr, "
            + "macaddr8Col    macaddr8, "
            + "moneyCol       money, "
            + "numericCol     numeric(4,2), "
            + "pathCol        path, "
            + "pointCol       point, "
            + "polygonCol     polygon, "
            + "realCol        real, "
            + "smallintCol    smallint, "
            + "smallserialCol smallserial, "
            + "serialCol      serial, "
            + "textCol        text, "
            + "timeCol        time, "
            + "timestampCol   timestamp, "
            + "uuidCol        uuid, "
            + "xmlCol         xml,"
            + "bitvaryCol     bit varying(1),"
            + "int8Col        int8,"
            + "boolCol        bool,"
            + "characterCol   character(4),"
            + "charactervCol  character varying(4),"
            + "intCol         int,"
            + "int4Col        int4,"
            + "doublepCol     double precision,"
            + "decimalCol     decimal(4,2),"
            + "float4Col      float,"
            + "int2Col        int2,"
            + "serial2Col     serial2,"
            + "serial4Col     serial4,"
            + "timetzCol      timetz,"
            + "timestamptzCol timestamptz,"
            + "serial8Col     serial8)"
        , TABLE_NAME));
    stmt.execute(String.format("INSERT INTO %s VALUES ( "
            + "1, 1, '1', '1011', true, '((1,1), (2,2))', '1', '1234', '1234', "
            + "'10', '((1,1),2)', '2018-12-31', 1.0, '88.99.0.0/16', 1, "
            + "'{\"2\":1}', '{1,2,3}', '((1,1),(2,2))', "
            + "'08002b:010203', '08002b:0102030405', '12.34', 1.0, '((1,1))', '(1,1)', "
            + "'((1,1))', 1.0, 1, 1, 1, '1110', '2018-12-31 00:00:01', '2018-12-31 00:00:01', "
            + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11','<foo>bar</foo>', '1', 1, true, '1234', '1234', 1, 1, 1.0, 1.0, 1.0"
            + ", 1, 1, 1, '2018-12-31 00:00:01', '2018-12-31 00:00:01', 1)",
        TABLE_NAME));
    stmt.execute(String.format("INSERT INTO %s VALUES ( "
            + "NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, 1)",
        TABLE_NAME));

  }

  public static void tearDown() throws VerdictDBDbmsException {
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    dbmsConn.close();
  }

  @Test
  public void testColumnTypes() throws VerdictDBDbmsException, SQLException {
    String sql = String.format("select * from %s", TABLE_NAME);
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(dbmsConn.execute(sql));
    ResultSet ourResult = new VerdictResultSet(result);
    ResultSetMetaData ourMetaData = ourResult.getMetaData();
    assertEquals(50, ourMetaData.getColumnCount());

    ResultSetMetaData expected = stmt.executeQuery(sql).getMetaData();
    assertEquals(expected.getColumnCount(), ourMetaData.getColumnCount());

    for (int i = 1; i <= ourMetaData.getColumnCount(); i++) {
      assertEquals(expected.getColumnType(i), ourMetaData.getColumnType(i));
    }

    ourResult.next();
    assertEquals(1, ourResult.getInt(45));         // int2
    assertEquals(1, ourResult.getLong(45));        // int2
    assertEquals(1, ourResult.getByte(45));        // int2
    assertEquals(true, ourResult.getBoolean(45));  // int2
    assertNotEquals(2, ourResult.getInt(45));      // int2
    assertNotEquals(2, ourResult.getLong(45));     // int2
    assertNotEquals(2, ourResult.getByte(45));     // int2
    assertEquals(true, ourResult.getBoolean(41));  // int4
    assertEquals(1, ourResult.getInt(41));         // int4
    assertEquals(1, ourResult.getLong(41));        // int4
    assertEquals(1, ourResult.getByte(41));        // int4
    assertEquals(true, ourResult.getBoolean(40));  // int
    assertEquals(1, ourResult.getInt(40));         // int
    assertEquals(1, ourResult.getLong(40));        // int
    assertEquals(1, ourResult.getByte(40));        // int
    assertEquals(true, ourResult.getBoolean(36));  // int8
    assertEquals(1, ourResult.getInt(36));         // int8
    assertEquals(1, ourResult.getLong(36));        // int8
    assertEquals(1, ourResult.getByte(36));        // int8
    assertEquals(true, ourResult.getBoolean(27));  // smallint
    assertEquals(1, ourResult.getInt(27));         // smallint
    assertEquals(1, ourResult.getLong(27));        // smallint
    assertEquals(1, ourResult.getByte(27));        // smallint
    assertEquals(true, ourResult.getBoolean(5));  // boolean
    assertEquals(1, ourResult.getInt(5));         // boolean
    assertEquals(1, ourResult.getLong(5));        // boolean
    assertEquals(1, ourResult.getByte(5));        // boolean
    assertEquals(true, ourResult.getBoolean(37));  // bool
    assertEquals(1, ourResult.getInt(37));         // bool
    assertEquals(1, ourResult.getLong(37));        // bool
    assertEquals(1, ourResult.getByte(37));        // bool
    assertEquals(true, ourResult.getBoolean(15));  // integer
    assertEquals(1, ourResult.getInt(15));         // integer
    assertEquals(1, ourResult.getLong(15));        // integer
    assertEquals(1, ourResult.getByte(15));        // integer
    assertEquals(true, ourResult.getBoolean(1));  // bigint
    assertEquals(1, ourResult.getInt(1));         // bigint
    assertEquals(1, ourResult.getLong(1));        // bigint
    assertEquals(1, ourResult.getByte(1));        // bigint

    try {
      assertEquals(true, ourResult.getBoolean(22));          // numeric
      fail();
    } catch (java.sql.SQLException e) {}
    assertEquals(1.0, ourResult.getFloat(22), 1e-6);         // numeric
    assertEquals(1.0, ourResult.getDouble(22), 1e-6);        // numeric
    assertEquals(1.0, ourResult.getByte(22), 1e-6);          // numeric
    assertEquals(1.0, ourResult.getInt(22), 1e-6);           // numeric
    assertEquals(1.0, ourResult.getLong(22), 1e-6);          // numeric
    assertEquals(1.0, ourResult.getFloat(26), 1e-6);         // real
    assertEquals(1.0, ourResult.getDouble(26), 1e-6);        // real
    assertEquals(1.0, ourResult.getByte(26), 1e-6);          // real
    assertEquals(1.0, ourResult.getInt(26), 1e-6);           // real
    assertEquals(1.0, ourResult.getLong(26), 1e-6);          // real
    assertEquals(1.0, ourResult.getFloat(13), 1e-6);         // float8
    assertEquals(1.0, ourResult.getDouble(13), 1e-6);        // float8
    assertEquals(1.0, ourResult.getByte(13), 1e-6);          // float8
    assertEquals(1.0, ourResult.getInt(13), 1e-6);           // float8
    assertEquals(1.0, ourResult.getLong(13), 1e-6);          // float8
    assertEquals(1.0, ourResult.getFloat(42), 1e-6);         // double precision
    assertEquals(1.0, ourResult.getDouble(42), 1e-6);        // double precision
    assertEquals(1.0, ourResult.getByte(42), 1e-6);          // double precision
    assertEquals(1.0, ourResult.getInt(42), 1e-6);           // double precision
    assertEquals(1.0, ourResult.getLong(42), 1e-6);          // double precision
    assertEquals(1.0, ourResult.getFloat(43), 1e-6);         // decimal
    assertEquals(1.0, ourResult.getDouble(43), 1e-6);        // decimal
    assertEquals(1.0, ourResult.getByte(43), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getInt(43), 1e-6);           // decimal
    assertEquals(1.0, ourResult.getLong(43), 1e-6);          // decimal
    assertEquals(1.0, ourResult.getFloat(44), 1e-6);         // float
    assertEquals(1.0, ourResult.getDouble(44), 1e-6);        // float
    assertEquals(1.0, ourResult.getByte(44), 1e-6);          // float
    assertEquals(1.0, ourResult.getInt(44), 1e-6);           // float
    assertEquals(1.0, ourResult.getLong(44), 1e-6);          // float
    assertEquals(Timestamp.valueOf("2018-12-31 00:00:00"), ourResult.getTimestamp(12));  // date
    assertEquals(Timestamp.valueOf("2018-12-31 00:00:01"), ourResult.getTimestamp(32));  // timestamp
    assertEquals(Time.valueOf("00:00:01"), ourResult.getTime(31));                       // time
    assertEquals(Timestamp.valueOf("1970-01-01 00:00:01"), ourResult.getTimestamp(31));  // time
    assertEquals(Timestamp.valueOf("2018-12-31 00:00:01"), ourResult.getTimestamp(49));  // timestamptz
    assertEquals(Time.valueOf("00:00:01"), ourResult.getTime(48));                       // timetz
    assertEquals(Timestamp.valueOf("1970-01-01 00:00:01"), ourResult.getTimestamp(48));  // timetz
    assertEquals("1234", ourResult.getString(8));             // char
    assertEquals("1234", ourResult.getString(9));             // varchar
    assertEquals("1234", ourResult.getString(38));             // character
    assertEquals("1234", ourResult.getString(39));             // character varying

    // NULL value
    ourResult.next();
    assertEquals(0, ourResult.getInt(45));         // int2
    assertEquals(0, ourResult.getLong(45));        // int2
    assertEquals(0, ourResult.getByte(45));        // int2
    assertEquals(false, ourResult.getBoolean(45));  // int2
    assertEquals(false, ourResult.getBoolean(41));  // int4
    assertEquals(0, ourResult.getInt(41));         // int4
    assertEquals(0, ourResult.getLong(41));        // int4
    assertEquals(0, ourResult.getByte(41));        // int4
    assertEquals(false, ourResult.getBoolean(40));  // int
    assertEquals(0, ourResult.getInt(40));         // int
    assertEquals(0, ourResult.getLong(40));        // int
    assertEquals(0, ourResult.getByte(40));        // int
    assertEquals(false, ourResult.getBoolean(36));  // int8
    assertEquals(0, ourResult.getInt(36));         // int8
    assertEquals(0, ourResult.getLong(36));        // int8
    assertEquals(0, ourResult.getByte(36));        // int8
    assertEquals(false, ourResult.getBoolean(27));  // smallint
    assertEquals(0, ourResult.getInt(27));         // smallint
    assertEquals(0, ourResult.getLong(27));        // smallint
    assertEquals(0, ourResult.getByte(27));        // smallint
    assertEquals(false, ourResult.getBoolean(5));  // boolean
    assertEquals(0, ourResult.getInt(5));         // boolean
    assertEquals(0, ourResult.getLong(5));        // boolean
    assertEquals(0, ourResult.getByte(5));        // boolean
    assertEquals(false, ourResult.getBoolean(37));  // bool
    assertEquals(0, ourResult.getInt(37));         // bool
    assertEquals(0, ourResult.getLong(37));        // bool
    assertEquals(0, ourResult.getByte(37));        // bool
    assertEquals(false, ourResult.getBoolean(15));  // integer
    assertEquals(0, ourResult.getInt(15));         // integer
    assertEquals(0, ourResult.getLong(15));        // integer
    assertEquals(0, ourResult.getByte(15));        // integer
    assertEquals(false, ourResult.getBoolean(1));  // bigint
    assertEquals(0, ourResult.getInt(1));         // bigint
    assertEquals(0, ourResult.getLong(1));        // bigint
    assertEquals(0, ourResult.getByte(1));        // bigint

    assertEquals(0, ourResult.getFloat(22), 1e-6);         // numeric
    assertEquals(0, ourResult.getDouble(22), 1e-6);        // numeric
    assertEquals(0, ourResult.getByte(22), 1e-6);          // numeric
    assertEquals(0, ourResult.getInt(22), 1e-6);           // numeric
    assertEquals(0, ourResult.getLong(22), 1e-6);          // numeric
    assertEquals(0, ourResult.getFloat(26), 1e-6);         // real
    assertEquals(0, ourResult.getDouble(26), 1e-6);        // real
    assertEquals(0, ourResult.getByte(26), 1e-6);          // real
    assertEquals(0, ourResult.getInt(26), 1e-6);           // real
    assertEquals(0, ourResult.getLong(26), 1e-6);          // real
    assertEquals(0, ourResult.getFloat(13), 1e-6);         // float8
    assertEquals(0, ourResult.getDouble(13), 1e-6);        // float8
    assertEquals(0, ourResult.getByte(13), 1e-6);          // float8
    assertEquals(0, ourResult.getInt(13), 1e-6);           // float8
    assertEquals(0, ourResult.getLong(13), 1e-6);          // float8
    assertEquals(0, ourResult.getFloat(42), 1e-6);         // double precision
    assertEquals(0, ourResult.getDouble(42), 1e-6);        // double precision
    assertEquals(0, ourResult.getByte(42), 1e-6);          // double precision
    assertEquals(0, ourResult.getInt(42), 1e-6);           // double precision
    assertEquals(0, ourResult.getLong(42), 1e-6);          // double precision
    assertEquals(0, ourResult.getFloat(43), 1e-6);         // decimal
    assertEquals(0, ourResult.getDouble(43), 1e-6);        // decimal
    assertEquals(0, ourResult.getByte(43), 1e-6);          // decimal
    assertEquals(0, ourResult.getInt(43), 1e-6);           // decimal
    assertEquals(0, ourResult.getLong(43), 1e-6);          // decimal
    assertEquals(0, ourResult.getFloat(44), 1e-6);         // float
    assertEquals(0, ourResult.getDouble(44), 1e-6);        // float
    assertEquals(0, ourResult.getByte(44), 1e-6);          // float
    assertEquals(0, ourResult.getInt(44), 1e-6);           // float
    assertEquals(0, ourResult.getLong(44), 1e-6);          // float
    assertNull(ourResult.getTimestamp(12));  // date
    assertNull(ourResult.getTimestamp(32));  // timestamp
    assertNull(ourResult.getTime(31));                       // time
    assertNull(ourResult.getTimestamp(31));  // time
    assertNull(ourResult.getTimestamp(49));  // timestamptz
    assertNull(ourResult.getTime(48));                       // timetz
    assertNull(ourResult.getTimestamp(48));  // timetz
    assertNull(ourResult.getString(8));             // char
    assertNull(ourResult.getString(9));             // varchar
    assertNull(ourResult.getString(38));             // character
    assertNull(ourResult.getString(39));             // character varying
  }


}
