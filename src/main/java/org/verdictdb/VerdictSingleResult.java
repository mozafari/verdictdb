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

package org.verdictdb;

import java.sql.SQLException;

import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.commons.VerdictResultPrinter;
import org.verdictdb.connection.DbmsQueryResultMetaData;

/**
 * Represents the result set returned from VerdictDB to the end user.
 *
 * @author Yongjoo Park
 */
public abstract class VerdictSingleResult extends AttributeValueRetrievalHelper {

  public abstract boolean isEmpty();

  /** @return Meta Data from ResultSet */
  public abstract DbmsQueryResultMetaData getMetaData();

  public abstract int getColumnCount();

  /**
   * @param index zero-based index
   * @return
   */
  public abstract String getColumnName(int index);

  /**
   * @param index zero-based index
   * @return
   */
  public abstract int getColumnType(int index);
  
  public String getColumnTypeName(int index) {
    int typeInt = getColumnType(index);
    return DataTypeConverter.typeName(typeInt);
  }

  /**
   * set the index before the first one; when next() is called, the index will move to the first
   * row.
   */
  public abstract void rewind();

  /**
   * Forward a cursor to rows by one. Similar to JDBC ResultSet.next().
   *
   * @return True if next row exists.
   */
  public abstract boolean next();

  /**
   * Returns the total number of rows.
   *
   * @return
   */
  public abstract long getRowCount();

  /**
   * @param index This is a zero-based index.
   * @return
   */
  public abstract Object getValue(int index);

  public abstract boolean wasNull() throws SQLException;
  
  public String toCsv() {
    return VerdictResultPrinter.SingleResultToCSV(this);
  }
  
  public void printCsv() {
    System.out.println(toCsv());
  }

}
