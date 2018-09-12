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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntaxList;

/**
 * Maintains a pool of multiple java.sql.Connections to provide concurrent execution of queries to
 * the backend database.
 *
 * @author Yongjoo Park
 */
public class ConcurrentJdbcConnection extends DbmsConnection {

  private static final int CONNECTION_POOL_SIZE = 10;

  private List<JdbcConnection> connections = new ArrayList<>();

  private int nextConnectionIndex = 0;

  private String url;

  private Properties info;

  private VerdictDBLogger logger = VerdictDBLogger.getLogger(getClass());

  public ConcurrentJdbcConnection(List<JdbcConnection> connections) {
    this.connections.addAll(connections);
  }

  public ConcurrentJdbcConnection(String url, Properties info, SqlSyntax syntax)
      throws VerdictDBDbmsException {
    logger.debug(
        String.format("Creating %d JDBC connections with this url: " + url, CONNECTION_POOL_SIZE));
    this.url = url;
    this.info = info;
    for (int i = 0; i < CONNECTION_POOL_SIZE; i++) {
      try {
        Connection c;
        if (info == null) {
          c = DriverManager.getConnection(url);
        } else {
          c = DriverManager.getConnection(url, info);
        }
        connections.add(JdbcConnection.create(c));
      } catch (SQLException e) {
        throw new VerdictDBDbmsException(e);
      }
    }
  }

  public static ConcurrentJdbcConnection create(String connectionString, Properties info)
      throws VerdictDBDbmsException {
    SqlSyntax syntax = SqlSyntaxList.getSyntaxFromConnectionString(connectionString);
    return new ConcurrentJdbcConnection(connectionString, info, syntax);
  }

  public static ConcurrentJdbcConnection create(String connectionString)
      throws VerdictDBDbmsException {
    SqlSyntax syntax = SqlSyntaxList.getSyntaxFromConnectionString(connectionString);
    return new ConcurrentJdbcConnection(connectionString, null, syntax);
  }

  public JdbcConnection getNextConnection() {
    JdbcConnection c = connections.get(nextConnectionIndex);
    nextConnectionIndex++;
    if (nextConnectionIndex >= connections.size()) {
      nextConnectionIndex = 0;
    }
    return c;
  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    return getNextConnection().getSchemas();
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    return getNextConnection().getTables(schema);
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table)
      throws VerdictDBDbmsException {
    return getNextConnection().getColumns(schema, table);
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table)
      throws VerdictDBDbmsException {
    return getNextConnection().getPartitionColumns(schema, table);
  }

  @Override
  public String getDefaultSchema() {
    return getNextConnection().getDefaultSchema();
  }

  @Override
  public void setDefaultSchema(String schema) {
    for (JdbcConnection c : connections) {
      c.setDefaultSchema(schema);
    }
  }

  @Override
  public DbmsQueryResult execute(String query) throws VerdictDBDbmsException {
    return getNextConnection().execute(query);
  }

  @Override
  public SqlSyntax getSyntax() {
    return getNextConnection().getSyntax();
  }

  @Override
  public void abort() {
    for (JdbcConnection c : connections) {
      c.abort();
    }
  }

  @Override
  public void close() {
    for (JdbcConnection c : connections) {
      c.close();
    }
  }

  @Override
  public DbmsConnection copy() {
    ConcurrentJdbcConnection copy = new ConcurrentJdbcConnection(connections);
    copy.url = url;
    copy.info = info;
    return copy;
  }

  public void reinitiateConnection() throws VerdictDBDbmsException {
    for (JdbcConnection connection:connections) {
      try {
        // Timeout 1s
        if (!connection.getConnection().isValid(1)) {
          Connection c;
          if (info != null) {
            c = DriverManager.getConnection(url, info);
          } else {
            c = DriverManager.getConnection(url);
          }
          connections.set(connections.indexOf(connection), JdbcConnection.create(c));
        }
      } catch (SQLException e) {
        logger.info("Failed to reinitiate connection");
      }
    }
  }
}
