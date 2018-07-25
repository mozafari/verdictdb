package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcQueryResult;
import org.verdictdb.coordinator.VerdictSingleResultFromDbmsQueryResult;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.aggresult.AggregateFrameQueryResult;
import org.verdictdb.core.rewriter.aggresult.AggNameAndType;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Tests the correctness of result set object using H2 database.
 * 
 * @author Yongjoo Park
 *
 */
public class JdbcResultSetBasicCorrectnessTest {

  static Connection conn;

  private VerdictResultSet jdbcResultSet;

  private static Statement stmt;

  @BeforeClass
  public static void setupH2Database() throws SQLException {
    final String DB_CONNECTION = "jdbc:h2:mem:resultsettest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    List<List<Object>> contents = new ArrayList<>();
    contents.add(Arrays.<Object>asList(1, "Anju", "female", 15, 170.2, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(2, "Sonia", "female", 17, 156.5, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Asha", "male", 23, 168.1, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Joe", "male", 14, 178.6, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "JoJo", "male", 18, 190.7, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Sam", "male", 18, 190.0, "USA", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Alice", "female", 18, 190.21, "CHN", "2017-10-12 21:22:23"));
    contents.add(Arrays.<Object>asList(3, "Bob", "male", 18, 190.3, "CHN", "2017-10-12 21:22:23"));
    stmt = conn.createStatement();
    stmt.execute("DROP TABLE PEOPLE IF EXISTS");
    stmt.execute("CREATE TABLE PEOPLE(id smallint, name varchar(255), gender varchar(8), age int, height float, nation varchar(8), birth timestamp)");
    for (List<Object> row : contents) {
      String id = row.get(0).toString();
      String name = row.get(1).toString();
      String gender = row.get(2).toString();
      String age = row.get(3).toString();
      String height = row.get(4).toString();
      String nation = row.get(5).toString();
      String birth = row.get(6).toString();
      stmt.execute(String.format("INSERT INTO PEOPLE(id, name, gender, age, height, nation, birth) VALUES(%s, '%s', '%s', %s, %s, '%s', '%s')", id, name, gender, age, height, nation, birth));
    }
  }

  @Test
  public void getStringIndexTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);
    List<String> actual = new ArrayList<>();
    List<String> expected = Arrays.asList("female", "male");
    while (jdbcResultSet.next()) {
      assertEquals(true, expected.contains(jdbcResultSet.getString(1)));
    }
  }

  @Test
  public void getStringLabelTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);
    List<String> actual = new ArrayList<>();
    List<String> expected = Arrays.asList("female", "male");
    while (jdbcResultSet.next()) {
      assertEquals(true, expected.contains(jdbcResultSet.getString("GENDER")));
      assertEquals(true, expected.contains(jdbcResultSet.getString("gender")));
    }
  }

  @Test
  public void getIntegerIndexTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        assertEquals(3, jdbcResultSet.getInt(2));
      } else {
        assertEquals(5, jdbcResultSet.getInt(2));
      }
    }
  }

  @Test
  public void getIntegerLabelTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        assertEquals(3, jdbcResultSet.getInt("CNT"));
        assertEquals(3, jdbcResultSet.getInt("cnt"));
        assertEquals(3, jdbcResultSet.getInt("cNT"));
      } else {
        assertEquals(5, jdbcResultSet.getInt("CNT"));
        assertEquals(5, jdbcResultSet.getInt("cnt"));
        assertEquals(5, jdbcResultSet.getInt("cNT"));
      }
    }
  }

  @Test
  public void getLongIndexTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        assertEquals(3, jdbcResultSet.getLong(2));
      } else assertEquals(5, jdbcResultSet.getLong(2));
    }
  }

  @Test
  public void getLongLabelTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, sum(age) as agesum FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("AGESUM", "SUM"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        assertEquals(3, jdbcResultSet.getLong("CNT"));
        assertEquals(3, jdbcResultSet.getLong("cnt"));
        assertEquals(3, jdbcResultSet.getLong("cNT"));
      } else {
        assertEquals(5, jdbcResultSet.getLong("CNT"));
        assertEquals(5, jdbcResultSet.getLong("cnt"));
        assertEquals(5, jdbcResultSet.getLong("cNT"));
      }
    }
  }

  @Test
  public void getDoubleIndexTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcResultSet.getDouble(3), 0.0001);
      } else if (jdbcResultSet.getString(1).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcResultSet.getDouble(3), 0.0001);
      }
    }
  }

  @Test
  public void getDoubleLabelTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcResultSet.getDouble("A"), 0.0001);
      } else if (jdbcResultSet.getString(1).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcResultSet.getDouble("A"), 0.0001);
      }
    }
  }

  @Test
  public void getFloatIndexTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcResultSet.getFloat(3), 0.0001);
      } else if (jdbcResultSet.getString(1).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcResultSet.getFloat(3), 0.0001);
      }
    }
  }

  @Test
  public void getFloatLabelTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals(expected, jdbcResultSet.getFloat("A"), 0.0001);
      } else if (jdbcResultSet.getString(1).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals(expected, jdbcResultSet.getFloat("A"), 0.0001);
      }
    }
  }

  @Test
  public void DoubleToIntegerTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        double expected = (170.2 + 156.5 + 190.21) / 3;
        assertEquals((int)expected, jdbcResultSet.getInt("A"));
        assertEquals((long)expected, jdbcResultSet.getLong("A"));
        assertEquals((short)expected, jdbcResultSet.getShort("A"));
        assertEquals((long)expected, jdbcResultSet.getBigDecimal("A").longValue());
      } else if (jdbcResultSet.getString(1).equals("male")) {
        double expected = (168.1 + 178.6 + 190.7 + 190.0 + 190.3) / 5;
        assertEquals((int)expected, jdbcResultSet.getInt("A"));
        assertEquals((long)expected, jdbcResultSet.getLong("A"));
        assertEquals((short)expected, jdbcResultSet.getShort("A"));
        assertEquals((long)expected, jdbcResultSet.getBigDecimal("A").longValue());
      }
    }
  }

  @Test
  public void IntegerToDoubleTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      if (jdbcResultSet.getString(1).equals("female")) {
        int expected = 3;
        assertEquals((float) expected, jdbcResultSet.getFloat(2), 0.0001);
        assertEquals((double)expected, jdbcResultSet.getBigDecimal(2).doubleValue(), 0.0001);
        assertEquals((double)expected, jdbcResultSet.getDouble(2), 0.0001);
      } else if (jdbcResultSet.getString(1).equals("male")) {
        int expected = 5;
        assertEquals((float) expected, jdbcResultSet.getFloat(2), 0.0001);
        assertEquals((double)expected, jdbcResultSet.getBigDecimal(2).doubleValue(), 0.0001);
        assertEquals((double)expected, jdbcResultSet.getDouble(2), 0.0001);
      }
    }
  }

  @Test
  public void getTimestampTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT birth, count(*) as cnt, avg(height) as a FROM PEOPLE GROUP BY birth");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("BIRTH");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    agg.add(new AggNameAndType("A", "AVG"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      Timestamp timestamp = Timestamp.valueOf("2017-10-12 21:22:23.0");
      assertEquals(timestamp, jdbcResultSet.getTimestamp(1));
      assertEquals(timestamp, jdbcResultSet.getTimestamp("BIRTH"));
      assertEquals(new Date(timestamp.getTime()), jdbcResultSet.getDate(1));
      assertEquals(new Time(timestamp.getTime()), jdbcResultSet.getTime(1));
    }
  }

  @Test
  public void getBinaryTest() throws SQLException, VerdictDBValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(12 as Binary) as bin FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BIN");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      byte[] bin = jdbcResultSet.getBytes(3);
      assertEquals(12, bin[3]);
      InputStream binStream = jdbcResultSet.getBinaryStream(3);
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(12, binStream.read());
    }
  }

  @Test
  public void getVarbinaryTest() throws SQLException, VerdictDBValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(12 as varbinary(max)) as bin FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BIN");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      byte[] bin = jdbcResultSet.getBytes(3);
      assertEquals(12, bin[3]);
      InputStream binStream = jdbcResultSet.getBinaryStream(3);
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(12, binStream.read());
    }
  }

  @Test
  public void getLongvarbinaryTest() throws SQLException, VerdictDBValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(12 as longvarbinary(max)) as bin FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BIN");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      byte[] bin = jdbcResultSet.getBytes(3);
      assertEquals(12, bin[3]);
      InputStream binStream = jdbcResultSet.getBinaryStream(3);
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(0, binStream.read());
      assertEquals(12, binStream.read());
    }
  }

  @Test
  public void getBitTest() throws SQLException, VerdictDBValueException, IOException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0 as bit) as bool FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("BOOL");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      byte bin = jdbcResultSet.getByte(3);
      assertEquals(0, bin);
      boolean b = jdbcResultSet.getBoolean(3);
      assertEquals(false, b);
      int i = jdbcResultSet.getInt(3);
      assertEquals(0, i);
    }
  }

  @Test
  public void getBlobTest() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0x1234567 as blob) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult, true);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      Blob bin = jdbcResultSet.getBlob(3);
      byte[] b = bin.getBytes(0,4);
      assertEquals(25, b[0]);
      assertEquals(8, b[1]);
      assertEquals(-121, b[2]);
      assertEquals(67, b[3]);
    }
  }

  @Test
  public void getBoolean() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0 as boolean) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      Boolean b = jdbcResultSet.getBoolean(3);
      assertEquals(false, b);
      int i = jdbcResultSet.getInt(3);
      assertEquals(0, i);
    }
  }

  @Test
  public void getClob() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast(0x1234567 as clob) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult, true);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      Clob bin = jdbcResultSet.getClob(3);
      assertEquals(8, bin.length());
    }
  }

  @Test
  public void getArray() throws SQLException, VerdictDBValueException {
    ResultSet rs = stmt.executeQuery("SELECT gender, count(*) as cnt, cast((1, 2, 3) as array) as b FROM PEOPLE GROUP BY gender");
    JdbcQueryResult queryResult = new JdbcQueryResult(rs);
    List<String> nonAgg = new ArrayList<>();
    List<AggNameAndType> agg = new ArrayList<>();
    nonAgg.add("GENDER");
    nonAgg.add("B");
    agg.add(new AggNameAndType("CNT", "COUNT"));
    AggregateFrame aggregateFrame = AggregateFrame.fromDmbsQueryResult(queryResult, nonAgg, agg);
    AggregateFrameQueryResult aggregateFrameQueryResult = (AggregateFrameQueryResult) aggregateFrame.toDbmsQueryResult();
    VerdictSingleResultFromDbmsQueryResult result = new VerdictSingleResultFromDbmsQueryResult(aggregateFrameQueryResult);
    jdbcResultSet = new VerdictResultSet(result);

    while (jdbcResultSet.next()) {
      Array a = jdbcResultSet.getArray(3);
      Object[] arr = (Object[])a.getArray();
      assertEquals(3, arr.length);
      assertEquals(1, arr[0]);
      assertEquals(2, arr[1]);
      assertEquals(3, arr[2]);
    }
  }

}
