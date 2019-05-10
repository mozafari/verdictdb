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

import java.util.Map;
import java.util.TreeMap;

public class SqlSyntaxList {

  // case-insensitive key:
  // https://stackoverflow.com/questions/8236945/case-insensitive-string-as-hashmap-key
  static Map<String, SqlSyntax> nameToSyntax = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

  static {
    nameToSyntax.put("Impala", new ImpalaSyntax());
    nameToSyntax.put("H2", new H2Syntax());
    nameToSyntax.put("Hive2", new HiveSyntax());
    nameToSyntax.put("Presto", new PrestoHiveSyntax()); // uses Hive only for now.
    nameToSyntax.put("MySQL", new MysqlSyntax());
    nameToSyntax.put("PostgreSQL", new PostgresqlSyntax());
    nameToSyntax.put("Redshift", new RedshiftSyntax());
    nameToSyntax.put("Spark", new SparkSyntax());
    nameToSyntax.put("SQLite", new SqliteSyntax());
  }

  public static SqlSyntax getSyntaxFor(String dbName) {
    return nameToSyntax.get(dbName);
  }

  public static SqlSyntax getSyntaxFromConnectionString(String connectionString) {
    String dbName = connectionString.split(":")[1];
    
    SqlSyntax syntax = SqlSyntaxList.getSyntaxFor(dbName);
    if (syntax == null) {
      dbName = connectionString.split(":")[0];
      syntax = SqlSyntaxList.getSyntaxFor(dbName);
    }
    return syntax;
  }
}
