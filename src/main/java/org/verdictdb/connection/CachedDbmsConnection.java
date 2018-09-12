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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SqlSyntax;

/**
 * Offers the same functionality as DbmsConnection; however, returns cached metadata whenever
 * possible to speed up query processing.
 *
 * @author Yongjoo Park
 */
public class CachedDbmsConnection extends DbmsConnection implements MetaDataProvider {

  DbmsConnection originalConn;

  public DbmsConnection getOriginalConn() {
    return originalConn;
  }

  public CachedDbmsConnection(DbmsConnection conn) {
//    super(conn);
    this.originalConn = conn;
  }

  @Override
  public DbmsQueryResult execute(String query) throws VerdictDBDbmsException {
    return originalConn.execute(query);
  }

  @Override
  public SqlSyntax getSyntax() {
    return originalConn.getSyntax();
  }
  
  @Override
  public void abort() {
    originalConn.abort();
  }

  @Override
  public void close() {
    originalConn.close();
  }

  public DbmsConnection getOriginalConnection() {
    return originalConn;
  }

  @Override
  public DbmsConnection copy() {
    CachedDbmsConnection newConn = new CachedDbmsConnection(originalConn.copy());
    return newConn;
  }

  //  private String defaultSchema = null;

  private List<String> schemaCache = new ArrayList<>();

  private HashMap<String, List<String>> tablesCache = new HashMap<>();

  private HashMap<Pair<String, String>, List<String>> partitionCache = new HashMap<>();

  // Get column name and type
  private HashMap<Pair<String, String>, List<Pair<String, String>>> columnsCache = new HashMap<>();

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    if (!schemaCache.isEmpty()) {
      return schemaCache;
    }
    schemaCache.addAll(originalConn.getSchemas());
    return schemaCache;
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    if (tablesCache.containsKey(schema) && !tablesCache.get(schema).isEmpty()) {
      return tablesCache.get(schema);
    }
    tablesCache.put(schema, originalConn.getTables(schema));
    return tablesCache.get(schema);
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table)
      throws VerdictDBDbmsException {
    Pair<String, String> key = new ImmutablePair<>(schema, table);
    if (columnsCache.containsKey(key) && !columnsCache.get(key).isEmpty()) {
      return columnsCache.get(key);
    }
    columnsCache.put(key, originalConn.getColumns(schema, table));
    return columnsCache.get(key);
  }

  /**
   * Only needed for the DBMS that supports partitioning.
   *
   * @param schema
   * @param table
   * @return
   * @throws VerdictDBDbmsException
   */
  @Override
  public List<String> getPartitionColumns(String schema, String table)
      throws VerdictDBDbmsException {
    //    if (!syntax.doesSupportTablePartitioning()) {
    //      throw new VerdictDBDbmsException("Database does not support table partitioning");
    //    }
    Pair<String, String> key = new ImmutablePair<>(schema, table);
    if (columnsCache.containsKey(key) && !partitionCache.isEmpty()) {
      return partitionCache.get(key);
    }
    partitionCache.put(key, originalConn.getPartitionColumns(schema, table));
    return partitionCache.get(key);
  }

  public String getDefaultSchema() {
    String schema = originalConn.getDefaultSchema();
    //    if (defaultSchema == null) {
    //      defaultSchema = metaProvider.getDefaultSchema();
    //    }
    return schema;
  }

  public void setDefaultSchema(String schema) {
    originalConn.setDefaultSchema(schema);
  }
}
