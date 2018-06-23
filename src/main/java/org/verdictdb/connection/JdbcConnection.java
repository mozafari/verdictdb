package org.verdictdb.connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.jdbc.JdbcResultSet;
import org.verdictdb.sql.syntax.PostgresqlSyntax;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import com.google.common.base.Optional;

public class JdbcConnection implements DbmsConnection {
  
  Connection conn;
  
  SyntaxAbstract syntax;
  
  public JdbcConnection(Connection conn, SyntaxAbstract syntax) {
    this.conn = conn;
    this.syntax = syntax;
  }
  
  @Override
  public void close() {
    try {
      this.conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public DbmsQueryResult executeQuery(String query) {
    System.out.println("About to issue this query: " + query);
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      JdbcQueryResult jrs = new JdbcQueryResult(rs);
      rs.close();
      stmt.close();
      return jrs;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public int executeUpdate(String query) {
    System.out.println("About to issue this query: " + query);
    try {
      Statement stmt = conn.createStatement();
      int r = stmt.executeUpdate(query);
      stmt.close();
      return r;
    } catch (SQLException e) {
      e.printStackTrace();
      return 0;
    }
  }

  @Override
  public SyntaxAbstract getSyntax() {
    return syntax;
  }

  @Override
  public Connection getConnection() {return conn;}

  @Override
  public List<String> getSchemas() throws SQLException{
    List<String> schemas = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getSchemaCommand());
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    while (queryResult.next()) {
      schemas.add(jdbcQueryResult.getString(syntax.getSchemaNameColumnIndex()+1));
    }
    return schemas;
  }

  @Override
  public List<String> getTables(String schema) throws SQLException{
    List<String> tables = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getTableCommand(schema));
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    while (queryResult.next()) {
      tables.add(jdbcQueryResult.getString(syntax.getTableNameColumnIndex()+1));
    }
    return tables;
  }

  @Override
  public List<Pair<String, Integer>> getColumns(String schema, String table) throws SQLException{
    List<Pair<String, Integer>> columns = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getColumnsCommand(schema, table));
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    while (queryResult.next()) {
      String type = jdbcQueryResult.getString(syntax.getColumnTypeColumnIndex()+1);
      // remove the size of type
      type = type.replaceAll("\\(.*\\)", "");
      columns.add(new ImmutablePair<>(jdbcQueryResult.getString(syntax.getColumnNameColumnIndex()+1),
          DataTypeConverter.typeInt(type)));
    }
    return columns;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table) throws SQLException{
    List<String> partition = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    // the result of postgresql is a vector of column index
    if (syntax instanceof PostgresqlSyntax) {
      queryResult.next();
      Object o = jdbcQueryResult.getObject(1);
      String[] arr = o.toString().split(" ");
      List<Pair<String, Integer>> columns = getColumns(schema, table);
      for (int i=0; i<arr.length; i++) {
        partition.add(columns.get(Integer.valueOf(arr[i])-1).getKey());
      }
    }
    else {
      while (queryResult.next()) {
        partition.add(jdbcQueryResult.getString(1));
      }
    }
    return partition;
  }

  @Override
  public String getDefaultSchema() {
    // TODO Auto-generated method stub
    return null;
  }


}