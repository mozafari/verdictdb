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

package org.verdictdb.connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.DataTypeConverter;

public class StaticMetaData implements MetaDataProvider {

  public static class TableInfo {
    String schema;
    String tablename;

    public TableInfo(String schema, String tablename) {
      this.schema = schema;
      this.tablename = tablename;
    }

    public static TableInfo getTableInfo(String schema, String tablename) {
      return new TableInfo(schema, tablename);
    }

    @Override
    public boolean equals(Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }
  }

  private String defaultSchema = "";

  // The value pair: left is column name and right is its type
  private HashMap<TableInfo, List<Pair<String, Integer>>> tablesData = new HashMap<>();

  private List<String> schemas = new ArrayList<>();

  private HashMap<String, List<String>> tables = new HashMap<>();

  // TODO: what is the difference from tableData?
  private HashMap<Pair<String, String>, List<Pair<String, Integer>>> columns = new HashMap<>();

  private HashMap<Pair<String, String>, List<String>> partitions = new HashMap<>();

  public StaticMetaData() {}

  public StaticMetaData(HashMap<TableInfo, List<Pair<String, Integer>>> tablesData) {
    this.tablesData = tablesData;
    for (Map.Entry<TableInfo, List<Pair<String, Integer>>> entry : tablesData.entrySet()) {
      if (!schemas.contains(entry.getKey().schema)) {
        schemas.add(entry.getKey().schema);
      }
      tables.get(entry.getKey().schema).add(entry.getKey().tablename);
      columns.put(
          new ImmutablePair<>(entry.getKey().schema, entry.getKey().tablename), entry.getValue());
    }
  }

  public void addTableData(TableInfo table, List<Pair<String, Integer>> column) {
    tablesData.put(table, column);
    if (!schemas.contains(table.schema)) {
      schemas.add(table.schema);
      tables.put(table.schema, new ArrayList<String>());
    }
    tables.get(table.schema).add(table.tablename);
    columns.put(new ImmutablePair<>(table.schema, table.tablename), column);
  }

  public void addPartition(TableInfo table, List<String> column) {
    partitions.put(new ImmutablePair<>(table.schema, table.tablename), column);
  }

  public void setDefaultSchema(String schema) {
    defaultSchema = schema;
  }

  @Override
  public List<String> getSchemas() {
    return schemas;
  }

  @Override
  public List<String> getTables(String schema) {
    return tables.get(schema);
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table) {
    List<Pair<String, String>> nameAndType = new ArrayList<>();
    List<Pair<String, Integer>> nameAndIntType = columns.get(new ImmutablePair<>(schema, table));
    for (Pair<String, Integer> a : nameAndIntType) {
      nameAndType.add(Pair.of(a.getLeft(), DataTypeConverter.typeName(a.getRight())));
    }
    return nameAndType;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table) {
    return partitions.get(new ImmutablePair<>(schema, table));
  }

  public String getDefaultSchema() {
    return defaultSchema;
  }

  public HashMap<TableInfo, List<Pair<String, Integer>>> getTablesData() {
    return tablesData;
  }
}
