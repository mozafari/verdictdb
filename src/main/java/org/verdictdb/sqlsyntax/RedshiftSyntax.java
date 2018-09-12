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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

public class RedshiftSyntax extends SqlSyntax {

  private static final Map<String, String> typeMap;

  static {
    typeMap = new HashMap<>();
  }

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public void dropTable(String schema, String tablename) {}

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
  public Collection<String> getCandidateJDBCDriverClassNames() {
    List<String> candidates =
        Lists.newArrayList(
            "com.amazon.redshift.jdbc42.Driver", "com.amazon.redshift.jdbc41.Driver");
    return candidates;
  }

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("SET search_path to '%s'; ", schema));
    sql.append(
        String.format(
            "select \"column\", \"type\" "
                + "from PG_TABLE_DEF where tablename = '%s' and schemaname = '%s';",
            table, schema));
    return sql.toString();
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getFallbackDefaultSchema() {
    return "public";
  }

  /**
   * Documentation about SORTKEY:
   * https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
   *
   * <p>If you do not specify any sort keys, the table is not sorted by default. You can define a
   * maximum of 400 COMPOUND SORTKEY columns or 8 INTERLEAVED SORTKEY columns per table.
   *
   * <p>COMPOUND Specifies that the data is sorted using a compound key made up of all of the listed
   * columns, in the order they are listed. A compound sort key is most useful when a query scans
   * rows according to the order of the sort columns. The performance benefits of sorting with a
   * compound key decrease when queries rely on secondary sort columns. You can define a maximum of
   * 400 COMPOUND SORTKEY columns per table.
   *
   * <p>INTERLEAVED Specifies that the data is sorted using an interleaved sort key. A maximum of
   * eight columns can be specified for an interleaved sort key.
   *
   * <p>An interleaved sort gives equal weight to each column, or subset of columns, in the sort
   * key, so queries do not depend on the order of the columns in the sort key. When a query uses
   * one or more secondary sort columns, interleaved sorting significantly improves query
   * performance. Interleaved sorting carries a small overhead cost for data loading and vacuuming
   * operations.
   *
   * <p>Important
   *
   * <p>Don’t use an interleaved sort key on columns with monotonically increasing attributes, such
   * as identity columns, dates, or timestamps.
   */
  @Override
  public String getPartitionByInCreateTable(
      List<String> partitionColumns, List<Integer> partitionCounts) {
    StringBuilder sql = new StringBuilder();
    
    sql.append("COMPOUND SORTKEY");
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

  @Override
  public String getPartitionCommand(String schema, String table) {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("SET search_path to '%s'; ", schema));
    sql.append(
        String.format(
            "SELECT \"column\", \"type\" FROM PG_TABLE_DEF "
                + "WHERE \"schemaname\" = '%s' and \"tablename\" = '%s' "
                + "and \"sortkey\" > 0 "
                + "ORDER BY \"sortkey\";",
            schema, table));
    return sql.toString();
    //    return "select partattrs from pg_partitioned_table join pg_class on pg_class.relname='" +
    // table + "' " +
    //        "and pg_class.oid = pg_partitioned_table.partrelid join information_schema.tables " +
    //        "on table_schema='" + schema + "' and table_name = '" + table + "'";
  }

  @Override
  public String getQuoteString() {
    return "\"";
  }

  @Override
  public String getSchemaCommand() {
    return "select schema_name from information_schema.schemata";
  }

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    return "SELECT table_name FROM information_schema.tables "
        + "WHERE table_schema = '"
        + schema
        + "'";
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }

  @Override
  public String substituteTypeName(String type) {
    String newType = typeMap.get(type.toLowerCase());
    return (newType != null) ? newType : type;
  }

  @Override
  public String randFunction() {
    return "random()";
  }

  @Override
  public String getGenericStringDataTypeName() {
    return "VARCHAR(MAX)";
  }

  @Override
  public String getApproximateCountDistinct(String column) {
    return String.format("approximate count(distinct %s)", column);
  }
}
