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

import java.util.List;

public class SparkSyntax extends SqlSyntax {

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public void dropTable(String schema, String tablename) {}

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "DESCRIBE " + quoteName(schema) + "." + quoteName(table);
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getFallbackDefaultSchema() {
    return "default";
  }

  @Override
  public String getPartitionByInCreateTable(
      List<String> partitionColumns, List<Integer> partitionCounts) {
    StringBuilder sql = new StringBuilder();
    sql.append("partitioned by");
    sql.append(" (");
    boolean isFirstColumn = true;
    for (String col : partitionColumns) {
      if (isFirstColumn) {
        sql.append(quoteName(col));
        isFirstColumn = false;
      } else {
        sql.append(", " + quoteName(col));
      }
    }
    sql.append(")");
    return sql.toString();
  }

  /** This command also returns partition information if exists. */
  @Override
  public String getPartitionCommand(String schema, String table) {
    return "DESCRIBE " + quoteName(schema) + "." + quoteName(table);
    //    return "SHOW PARTITIONS " + quoteName(schema) + "." + quoteName(table);
  }

  @Override
  public String getQuoteString() {
    return "`";
  }

  @Override
  public String getSchemaCommand() {
    return "SHOW DATABASES";
  }

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    return "SHOW TABLES IN " + quoteName(schema);
  }

  @Override
  public int getTableNameColumnIndex() {
    return 1;
  }

  @Override
  public String randFunction() {
    return "rand()";
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    return true;
  }

  @Override
  public String getGenericStringDataTypeName() {
    return "STRING";
  }

  @Override
  public String getApproximateCountDistinct(String column) {
    return String.format("approx_count_distinct(%s)", column);
  }
}
