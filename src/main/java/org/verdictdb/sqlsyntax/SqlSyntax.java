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

package org.verdictdb.sqlsyntax;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class SqlSyntax {

  public abstract boolean doesSupportTablePartitioning();

  public abstract void dropTable(String schema, String tablename);

  // The column index that stored meta information in the original database
  public abstract int getColumnNameColumnIndex();

  public abstract String getColumnsCommand(String schema, String table);

  // The column index that stored meta information in the original database
  public abstract int getColumnTypeColumnIndex();

  // Assumes that each partitioning column is an int-type column that contains values ranging from
  // 0 to partitionCounts.get(i)-1.
  public abstract String getPartitionByInCreateTable(
      List<String> partitionColumns, List<Integer> partitionCounts);

  public abstract String getPartitionCommand(String schema, String table);

  public abstract String getQuoteString();

  public abstract String getSchemaCommand();

  public abstract int getSchemaNameColumnIndex();

  public abstract String getTableCommand(String schema);

  // The column index that stored meta information in the original database
  public abstract int getTableNameColumnIndex();

  public abstract String randFunction();

  public abstract boolean isAsRequiredBeforeSelectInCreateTable();

  public String getStddevPopulationFunctionName() {
    return "stddev_pop";
  }

  public String quoteName(String name) {
    String quoteString = getQuoteString();
    return quoteString + name + quoteString;
  }

  public String substituteTypeName(String type) {
    return type;
  }
  
  public String getGenericStringDataTypeName() {
    return "TEXT";
  }

  /**
   * The drivers returned by methods are loaded explicitly by JdbcConnection (when it makes a JDBC
   * connection to the backend database.) This mechanism is to support legacy library that does not
   * support automatic JDBC driver discovery.
   *
   * @return
   */
  public Collection<String> getCandidateJDBCDriverClassNames() {
    return Collections.emptyList();
  }

  public String getFallbackDefaultSchema() {
    throw new RuntimeException("This function must be implemented for each dbms syntax.");
  }

  /**
   * Hive, Impala, Spark, Redshift have approximate count distinct function
   * @return
   */
  public String getApproximateCountDistinct(String column) {
    return String.format("count (distinct %s)", column);
  }
}
