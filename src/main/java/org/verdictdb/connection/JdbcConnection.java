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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.StringSplitter;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.HiveSyntax;
import org.verdictdb.sqlsyntax.ImpalaSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.PrestoSyntax;
import org.verdictdb.sqlsyntax.RedshiftSyntax;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntaxList;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcConnection extends DbmsConnection {

  Connection conn;

  SqlSyntax syntax;

  String currentSchema = null;

  JdbcQueryResult jrs = null;

  protected boolean outputDebugMessage = false;

  protected Statement runningStatement = null;

  protected VerdictDBLogger log;

  protected boolean isAborting = false;

  public static JdbcConnection create(String jdbcConnectionString, Properties info)
      throws VerdictDBDbmsException {
    try {
      Connection c;
      if (info == null) {
        c = DriverManager.getConnection(jdbcConnectionString);
      } else {
        c = DriverManager.getConnection(jdbcConnectionString, info);
      }
      return JdbcConnection.create(c);
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    }
  }

  public static JdbcConnection create(String jdbcConnectionString) throws VerdictDBDbmsException {
    try {
      Connection c = DriverManager.getConnection(jdbcConnectionString);
      return JdbcConnection.create(c);
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    }
  }

  public static JdbcConnection create(Connection conn) throws VerdictDBDbmsException {
    String connectionString = null;
    SqlSyntax syntax = null;

    try {
      if (conn.getClass().getName() == "org.apache.hive.jdbc.HiveConnection") {
        syntax = SqlSyntaxList.getSyntaxFor("Hive2");
      } else {
        connectionString = conn.getMetaData().getURL();

        syntax = SqlSyntaxList.getSyntaxFromConnectionString(connectionString);
      }
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    }

    JdbcConnection jdbcConn = null;
    if (syntax instanceof PrestoSyntax) {
      // To handle that Presto's JDBC driver is not compatible with JDK7,
      // we use Java's reflection-based instantiation.
      try {
        Class<?> prestoConnClass = Class.forName("org.verdictdb.connection.PrestoJdbcConnection");
        Constructor<?> prestoConnClsConstructor =
            prestoConnClass.getConstructor(Connection.class, SqlSyntax.class);
        jdbcConn = (JdbcConnection) prestoConnClsConstructor.newInstance(conn, syntax);
        Method ensureMethod = prestoConnClass.getMethod("ensureCatalogSet");
        ensureMethod.invoke(jdbcConn);
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | SecurityException
          | InstantiationException
          | IllegalAccessException
          | IllegalArgumentException e) {
        throw new RuntimeException("Instantiating PrestoJdbcConnection failed.");

      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof VerdictDBDbmsException) {
          throw new VerdictDBDbmsException(e.getMessage());
        } else {
          throw new RuntimeException("Instantiating PrestoJdbcConnection failed.");
        }
      }
    } else {
      jdbcConn = new JdbcConnection(conn, syntax);
    }

    return jdbcConn;
  }

  public JdbcConnection(Connection conn, SqlSyntax syntax) {
    this.conn = conn;
    try {
      this.currentSchema = conn.getSchema();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    // set a default value if an inappropriate value is set.
    if (currentSchema == null || currentSchema.length() == 0) {
      currentSchema = syntax.getFallbackDefaultSchema();
    }

    this.syntax = syntax;
    this.log = VerdictDBLogger.getLogger(this.getClass());
  }

  @Override
  public void abort() {
    log.trace("Aborts a statement if running.");
    isAborting = true;
    try {
      synchronized (this) {
        // having isClosed() check seems to block this statement.
        if (runningStatement != null) {
          //        if (runningStatement != null && !runningStatement.isClosed()) {
          log.trace("Aborts a running statement.");
          runningStatement.cancel();
          runningStatement.close();
          runningStatement = null;
        }
      }

      isAborting = false;
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    log.debug("Closes a JDBC connection.");
    abort();
    try {
      this.conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public DbmsQueryResult execute(String sql) throws VerdictDBDbmsException {
    String quoteChars = "'\"";
    List<String> sqls = StringSplitter.splitOnSemicolon(sql, quoteChars);
    DbmsQueryResult finalResult = null;
    for (String s : sqls) {
      finalResult = executeSingle(s);
    }
    return finalResult;
  }

  protected void setRunningStatement(Statement stmt) {
    synchronized (this) {
      runningStatement = stmt;
    }
  }

  protected Statement getRunningStatement() {
    synchronized (this) {
      return runningStatement;
    }
  }

  public DbmsQueryResult executeSingle(String sql) throws VerdictDBDbmsException {
    log.debug("Issues the following query to DBMS: " + sql);

    try {
      Statement stmt = conn.createStatement();
      setRunningStatement(stmt);
      JdbcQueryResult jrs = null;
      boolean doesResultExist = stmt.execute(sql);
      if (doesResultExist) {
        ResultSet rs = stmt.getResultSet();
        jrs = new JdbcQueryResult(rs);
        rs.close();
      } else {
        jrs = null;
      }
      setRunningStatement(null);
      stmt.close();
      return jrs;
    } catch (SQLException e) {
      if (isAborting) {
        return null;
      } else {
        String msg = "Issued the following query: " + sql + "\n" + e.getMessage();
        throw new VerdictDBDbmsException(msg);
      }
    }
  }

  public DbmsQueryResult executeQuery(String sql) throws VerdictDBDbmsException {
    return execute(sql);
  }

  @Override
  public SqlSyntax getSyntax() {
    return syntax;
  }

  public Connection getConnection() {
    return conn;
  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    List<String> schemas = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getSchemaCommand());

    while (queryResult.next()) {
      schemas.add(queryResult.getString(syntax.getSchemaNameColumnIndex()));
    }

    return schemas;
  }

  @Override
  public List<String> getTables(String schema) {
    List<String> tables = new ArrayList<>();
    try {
      DbmsQueryResult queryResult = executeQuery(syntax.getTableCommand(schema));
      while (queryResult.next()) {
        tables.add(queryResult.getString(syntax.getTableNameColumnIndex()));
      }
    } catch (VerdictDBDbmsException e) {
      log.debug(e.getMessage());
    }
    return tables;
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<Pair<String, String>> columns = new ArrayList<>();
    String sql = syntax.getColumnsCommand(schema, table);

    try {
      DbmsQueryResult queryResult = executeQuery(sql);
      while (queryResult.next()) {
        String type;
        if (syntax instanceof PostgresqlSyntax) {
          type = queryResult.getString(syntax.getColumnTypeColumnIndex());
          if (queryResult.getInt(((PostgresqlSyntax) syntax).getCharacterMaximumLengthColumnIndex())
              != 0) {
            type =
                type
                    + "("
                    + queryResult.getInt(
                        ((PostgresqlSyntax) syntax).getCharacterMaximumLengthColumnIndex())
                    + ")";
          }
        } else {
          type = queryResult.getString(syntax.getColumnTypeColumnIndex());
        }
        type = type.toLowerCase();

        columns.add(
            new ImmutablePair<>(queryResult.getString(syntax.getColumnNameColumnIndex()), type));
      }

    } catch (Exception e) {
      if (syntax instanceof RedshiftSyntax
          && e.getMessage().matches("(?s).*schema .* does not exist;.*")) {
        return columns;
      } else {
        throw e;
      }
    }

    return columns;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<String> partition = new ArrayList<>();

    if (!syntax.doesSupportTablePartitioning()) {
      return partition;
    }

    DbmsQueryResult queryResult;
    if (syntax instanceof ImpalaSyntax) {
      try {
        queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
        for (int i = 0; i < queryResult.getColumnCount(); i++) {
          String columnName = queryResult.getColumnName(i);
          if (columnName.equalsIgnoreCase("#rows")) {
            break;
          } else {
            partition.add(columnName);
          }
        }
        return partition;
      } catch (Exception e) {
        if (e.getMessage().contains("Table is not partitioned")) {
          return partition;
        } else {
          throw e;
        }
      }
    } else if (syntax instanceof RedshiftSyntax) {
      try {
        queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
      } catch (Exception e) {
        if (e.getMessage().matches("(?s).*schema .* does not exist;.*")) {
          return partition;
        } else {
          throw e;
        }
      }
    } else {
      queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
    }

    // the result of postgresql is a vector of column index
    if (syntax instanceof PostgresqlSyntax) {
      if (queryResult.next()) {
        Object o = queryResult.getValue(0);
        String[] arr = o.toString().split(" ");
        List<Pair<String, String>> columns = getColumns(schema, table);
        for (int i = 0; i < arr.length; i++) {
          partition.add(columns.get(Integer.valueOf(arr[i]) - 1).getKey());
        }
      }
    }
    // Hive and Spark append partition information at the end of the "DESCRIBE TABLE" statement.
    else if (syntax instanceof HiveSyntax
        || syntax instanceof SparkSyntax
        || syntax instanceof PrestoSyntax) {
      boolean hasPartitionInfoStarted = false;
      while (queryResult.next()) {
        String name = queryResult.getString(0);
        if (hasPartitionInfoStarted && (name.equalsIgnoreCase("# col_name") == false)) {
          partition.add(name);
        } else if (name.equalsIgnoreCase("# Partition Information")) {
          hasPartitionInfoStarted = true;
        }
      }
    } else {
      while (queryResult.next()) {
        String column = queryResult.getString(0);
        if (column != null) {
          partition.add(queryResult.getString(0));
        }
      }
    }

    return partition;
  }

  @Override
  public String getDefaultSchema() {
    return currentSchema;
  }

  @Override
  public void setDefaultSchema(String schema) throws VerdictDBDbmsException {
    try {
      // these database have a different meaning for catalog; thus, does not change.
      if (syntax instanceof PrestoSyntax
          || syntax instanceof PostgresqlSyntax
          || syntax instanceof RedshiftSyntax) {

      } else {
        conn.setCatalog(schema);
      }
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    }
    currentSchema = schema;
  }

  public DatabaseMetaData getMetadata() throws VerdictDBDbmsException {
    try {
      return conn.getMetaData();
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    }
  }

  public boolean isOutputDebugMessage() {
    return outputDebugMessage;
  }

  public void setOutputDebugMessage(boolean outputDebugMessage) {
    this.outputDebugMessage = outputDebugMessage;
  }

  @Override
  public DbmsConnection copy() throws VerdictDBDbmsException {
    JdbcConnection newConn = new JdbcConnection(conn, syntax);
    newConn.setDefaultSchema(currentSchema);
    newConn.jrs = this.jrs;
    newConn.outputDebugMessage = this.outputDebugMessage;
    return newConn;
  }

  /** @return a list of column names of primary key columns. (0-indexed) */
  @Override
  public List<String> getPrimaryKey(String schema, String table) throws VerdictDBDbmsException {
    List<Integer> primaryKeyIndexList = new ArrayList<>();
    List<String> primaryKeyColumnName = new ArrayList<>();
    SqlSyntax syntax = getSyntax();
    if (syntax.getPrimaryKey(schema, table) != null) {
      DbmsQueryResult result = execute(syntax.getPrimaryKey(schema, table));
      while (result.next()) {
        primaryKeyIndexList.add(result.getInt(3) - 1);
      }
      List<String> columns = new ArrayList<>();
      result = execute(syntax.getColumnsCommand(schema, table));
      while (result.next()) {
        columns.add(result.getString(0));
      }
      for (int idx : primaryKeyIndexList) {
        primaryKeyColumnName.add(columns.get(idx));
      }
    }

    return primaryKeyColumnName;
  }
}
