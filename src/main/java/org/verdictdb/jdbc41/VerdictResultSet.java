/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.jdbc41;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.verdictdb.VerdictSingleResult;
import org.apache.commons.lang3.time.FastDateFormat;

public class VerdictResultSet implements ResultSet {

  private VerdictSingleResult queryResult;

  private ResultSetMetaData metadata;

  private long rowIndex = 0;

  private HashMap<String, Integer> colNameIdx = new HashMap<>();

  public VerdictResultSet(VerdictSingleResult queryResult) {
    this.queryResult = queryResult;
    for (int i = 0; i < queryResult.getColumnCount(); i++) {
      colNameIdx.put(queryResult.getColumnName(i), i);
    }
    metadata = new VerdictResultSetMetaData(queryResult);
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void afterLast() throws SQLException {
    rowIndex = queryResult.getRowCount() + 1;
    //    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void beforeFirst() throws SQLException {
    rowIndex = 0;
    //    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  private void checkIndex(int index) throws SQLException {
    if (index < 1) {
      throw new SQLException("Column index must be a positive integer.");
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() {
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1;
  }

  @Override
  public boolean first() throws SQLException {
    if (queryResult.getRowCount() == 0) {
      return false;
    } else {
      rowIndex = 1;
      return true;
    }
    //    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    checkIndex(columnIndex);

    return queryResult.getArray(columnIndex - 1);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getArray(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getAsciiStream(columnIndex - 1);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getAsciiStream(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    checkIndex(columnIndex);

    return queryResult.getBigDecimal(columnIndex - 1);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    checkIndex(columnIndex);

    return queryResult.getBigDecimal(columnIndex - 1, scale);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getBigDecimal(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getBigDecimal(standardizedLabel(columnLabel.toLowerCase()), scale);
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getBinaryStream(columnIndex - 1);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getBinaryStream(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    checkIndex(columnIndex);

    return queryResult.getBlob(columnIndex - 1);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getBlob(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getBoolean(columnIndex - 1);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getBoolean(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getByte(columnIndex - 1);

    //    try {
    //      if (isValidType("byte", columnIndex)) {
    //        lastValue = queryResult.getValue(columnIndex-1);
    //
    //        if (lastValue == null) {
    //          return 0;
    //        }
    //
    //        return (byte) TypeCasting.toByte(lastValue);
    //      }
    //      else {
    //        throw new VerdictDBTypeException(queryResult.getValue(columnIndex-1));
    //      }
    //    }
    //    catch (VerdictDBTypeException e) {
    //      throw new SQLException(e.getMessage());
    //    }
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getByte(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getBytes(columnIndex - 1);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getBytes(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    checkIndex(columnIndex);

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    checkIndex(columnIndex);

    return queryResult.getClob(columnIndex - 1);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getClob(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getDate(columnIndex - 1);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    checkIndex(columnIndex);

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getDate(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getDouble(columnIndex - 1);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getDouble(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getFloat(columnIndex - 1);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getFloat(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getInt(columnIndex - 1);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getInt(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getLong(columnIndex - 1);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getLong(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return metadata;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return getCharacterStream(columnIndex);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(standardizedLabel(columnLabel));
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getNClob(columnIndex - 1);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getNClob(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return getString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return getString(standardizedLabel(columnLabel));
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getValue(columnIndex - 1);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return getObject(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return getObject(colNameIdx.get(standardizedLabel(columnLabel.toLowerCase())) + 1);
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return (T) getObject(standardizedLabel(columnLabel));
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return getObject(standardizedLabel(columnLabel));
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return queryResult.getRef(columnIndex - 1);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getRef(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int getRow() throws SQLException {
    return ((Long) rowIndex).intValue();
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return queryResult.getRowId(columnIndex - 1);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getRowId(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getShort(columnIndex - 1);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getShort(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return queryResult.getSQLXML(columnIndex - 1);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getSQLXML(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Statement getStatement() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return queryResult.getString(columnIndex - 1);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getString(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getTime(columnIndex - 1);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getTime(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getTimestamp(columnIndex - 1);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    try {
      FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS", cal.getTimeZone());
      return new Timestamp(dateFormat.parse(queryResult.getTimestamp(columnIndex - 1).toString()).getTime());
    } catch (Exception e) {
      SQLException error = new SQLException("Error parsing time stamp");
      throw error;
    }
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getTimestamp(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      int index = columnLabel.indexOf(standardizedLabel(columnLabel)) + 1;
      return getTimestamp(index, cal);
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public int getType() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    return queryResult.getUnicodeStream(columnIndex - 1);
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getUnicodeStream(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return queryResult.getURL(columnIndex - 1);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    if (colNameIdx.containsKey(standardizedLabel(columnLabel))) {
      return queryResult.getURL(standardizedLabel(columnLabel.toLowerCase()));
    } else throw new SQLException("ColumnLabel does not exist.");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return rowIndex > queryResult.getRowCount();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return rowIndex == 0;
    //    return isBeforefirst;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return rowIndex == 1;
    //    return isFirst;
  }

  @Override
  public boolean isLast() throws SQLException {
    return rowIndex == queryResult.getRowCount();
    //    throw new SQLFeatureNotSupportedException();
  }

  // TODO: what is the purpose of this function?
  private boolean isValidType(String expected, int columnindex) {
    return true;
    //    String actual = DataTypeConverter.typeName(queryResult.getColumnType(columnindex-1));
    //    if (queryResult.getColumnType(columnindex-1) == DOUBLE) actual = "real";
    //    if (expected.equals("boolean")) {
    //      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    //    }
    //    else if (expected.equals("byte")) {
    //      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    //    }
    //    else if (expected.equals("short")) {
    //      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    //    }
    //    else if (expected.equals("int")) {
    //      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    //    }
    //    else if (expected.equals("long")) {
    //      return numericType.contains(actual) || actual.equals("boolean") || actual.equals("bit");
    //    }
    //    else if (expected.equals("float")) {
    //      return numericType.contains(actual);
    //    }
    //    else if (expected.equals("double")) {
    //      return numericType.contains(actual);
    //    }
    //    else if (expected.equals("bigdecimal")) {
    //      return numericType.contains(actual);
    //    }
    //    else if (expected.equals("bytes")) {
    //      return  actual.equals("binary") || actual.equals("varbinary") ||
    // actual.equals("longvarbinary")
    //          || actual.equals("blob");
    //    }
    //    else if (expected.equals("date")) {
    //      return  actual.equals("timestamp") || actual.equals("date") || actual.equals("time");
    //    }
    //    else if (expected.equals("time")) {
    //      return  actual.equals("timestamp") || actual.equals("date") || actual.equals("time");
    //    }
    //    else if (expected.equals("timestamp")) {
    //      return actual.equals("timestamp") || actual.equals("date") || actual.equals("time");
    //    }
    //    else if (expected.equals("asciistream")) {
    //      return actual.equals("clob");
    //    }
    //    else if (expected.equals("binarystream")) {
    //      return actual.equals("blob") || actual.equals("binary") || actual.equals("varbinary")
    //          || actual.equals("longvarbinary");
    //    }
    //    else if (expected.equals("blob")) {
    //      return actual.equals("blob");
    //    }
    //    else if (expected.equals("clob")) {
    //      return actual.equals("clob");
    //    }
    //    else if (expected.equals("array")) {
    //      return actual.equals("array");
    //    }
    //    else if (expected.equals("ref")) {
    //      return actual.equals("ref");
    //    }
    //    else if (expected.equals("sqlxml")) {
    //      return actual.equals("xml");
    //    }
    //    else if (expected.equals("rowid")) {
    //      return actual.equals("rowid");
    //    }
    //    else if (expected.equals("nclob")) {
    //      return actual.equals("nclob");
    //    }
    //    else return false;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean next() throws SQLException {
    boolean hasMore = queryResult.next();
    if (hasMore) {
      rowIndex++;
      return true;
    } else {
      return false;
    }

    //    if (rowCount == 1) {
    //      isFirst = true;
    //    }
    //    else {
    //      isFirst = false;
    //    }
    //
    //    if (isBeforefirst) {
    //      isBeforefirst = false;
    //    }
    //    boolean next = queryResult.next();
    //    rowCount++;
    //
    //    if (!next) {
    //      isAfterLast = true;
    //    }
    //    return next;
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  String standardizedLabel(String label) {
    return label.toLowerCase();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return queryResult.wasNull();
  }
}
