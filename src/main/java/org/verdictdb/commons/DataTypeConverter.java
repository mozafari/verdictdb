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

package org.verdictdb.commons;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATALINK;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DISTINCT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NULL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.REF;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.STRUCT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

public class DataTypeConverter {

  static final Map<Integer, String> intToStringMap;

  static final Map<String, Integer> stringToIntMap;

  //  static final Map<String, String> typeNameToStandardName;

  static {
    intToStringMap = new HashMap<>();
    intToStringMap.put(ARRAY, "array");
    intToStringMap.put(BIGINT, "bigint");
    intToStringMap.put(BINARY, "binary");
    intToStringMap.put(BIT, "bit");
    intToStringMap.put(BLOB, "blob");
    intToStringMap.put(BOOLEAN, "boolean");
    intToStringMap.put(CHAR, "char");
    intToStringMap.put(CLOB, "clob");
    intToStringMap.put(DATALINK, "datalink");
    intToStringMap.put(DATE, "date");
    intToStringMap.put(DECIMAL, "decimal");
    intToStringMap.put(DISTINCT, "distinct");
    intToStringMap.put(FLOAT, "float");
    intToStringMap.put(INTEGER, "integer");
    intToStringMap.put(LONGNVARCHAR, "longnvarchar");
    intToStringMap.put(LONGVARBINARY, "longvarbinary");
    intToStringMap.put(LONGVARCHAR, "longvarJdchar");
    intToStringMap.put(NCHAR, "nchar");
    intToStringMap.put(NCLOB, "nclob");
    intToStringMap.put(NULL, "null");
    intToStringMap.put(NUMERIC, "numeric");
    intToStringMap.put(NVARCHAR, "nvarchar");
    intToStringMap.put(REAL, "real");
    intToStringMap.put(REF, "ref");
    intToStringMap.put(ROWID, "rowid");
    intToStringMap.put(SMALLINT, "smallint");
    intToStringMap.put(SQLXML, "xml");
    intToStringMap.put(STRUCT, "struct");
    intToStringMap.put(TIME, "time");
    intToStringMap.put(TIMESTAMP, "timestamp");
    intToStringMap.put(TINYINT, "tinyint");
    intToStringMap.put(VARBINARY, "varbinary");
    intToStringMap.put(VARCHAR, "varchar");
    intToStringMap.put(DOUBLE, "double"); // h2 will convert float to double type

    stringToIntMap = new HashMap<>();
    //    stringToIntMap.put("array", ARRAY);
    //    stringToIntMap.put("bigint", BIGINT);
    //    stringToIntMap.put("binary", BINARY);
    //    stringToIntMap.put("bit", BIT);
    //    stringToIntMap.put("blob", BLOB);
    //    stringToIntMap.put("boolean", BOOLEAN);
    //    stringToIntMap.put("char", CHAR);
    //    stringToIntMap.put("clob", CLOB);
    //    stringToIntMap.put("datalink", DATALINK);
    //    stringToIntMap.put("date", DATE);
    //    stringToIntMap.put("decimal", DECIMAL);
    //    stringToIntMap.put("distinct", DISTINCT);
    //    stringToIntMap.put("double", DOUBLE);
    //    stringToIntMap.put("float", FLOAT);
    //    stringToIntMap.put("integer", INTEGER);
    //    stringToIntMap.put("longnvarchar", LONGNVARCHAR);
    //    stringToIntMap.put("longvarbinary", LONGVARBINARY);
    //    stringToIntMap.put("longvarJdchar", LONGVARCHAR);
    //    stringToIntMap.put("nchar", NCHAR);
    //    stringToIntMap.put("nclob", NCLOB);
    //    stringToIntMap.put("null", NULL);
    //    stringToIntMap.put("numeric", NUMERIC);
    //    stringToIntMap.put("nvarchar", NVARCHAR);
    //    stringToIntMap.put("real", REAL);
    //    stringToIntMap.put("ref", REF);
    //    stringToIntMap.put("rowid", ROWID);
    //    stringToIntMap.put("smallint", SMALLINT);
    //    stringToIntMap.put("xml", SQLXML);
    //    stringToIntMap.put("struct", STRUCT);
    //    stringToIntMap.put("time", TIME);
    //    stringToIntMap.put("timestamp", TIMESTAMP);
    //    stringToIntMap.put("tinyint", TINYINT);
    //    stringToIntMap.put("varbinary", VARBINARY);
    //    stringToIntMap.put("varchar", VARCHAR);
    for (Entry<Integer, String> pair : intToStringMap.entrySet()) {
      stringToIntMap.put(pair.getValue(), pair.getKey());
    }
    stringToIntMap.put("character varying", VARCHAR);
    stringToIntMap.put("character", CHAR);
    stringToIntMap.put("int", INTEGER);
    stringToIntMap.put("mediumint", INTEGER);
    stringToIntMap.put("text", LONGNVARCHAR);
    stringToIntMap.put("double precision", DOUBLE);
    stringToIntMap.put("smallserial", INTEGER);
    stringToIntMap.put("serial", INTEGER);
    stringToIntMap.put("bigserial", BIGINT);
    stringToIntMap.put("bigint unsigned", BIGINT);
    stringToIntMap.put("string", VARCHAR);
    stringToIntMap.put("datetime", TIMESTAMP);
    stringToIntMap.put("timestamp", TIMESTAMP);
    stringToIntMap.put("timestamp without time zone", TIMESTAMP);
    // MySql
    stringToIntMap.put("tinyblob", BLOB);
    stringToIntMap.put("mediumblob", BLOB);
    stringToIntMap.put("longblob", BLOB);
    stringToIntMap.put("tinytext", VARCHAR);
    stringToIntMap.put("mediumtext", VARCHAR);
    stringToIntMap.put("longtext", VARCHAR);
    stringToIntMap.put("enum", CHAR);
    stringToIntMap.put("set", CHAR);
    stringToIntMap.put("year", DATE);
    // Postgresql
    stringToIntMap.put("bit varying", BINARY);
    stringToIntMap.put("box", BLOB); // Assign BLOB to geometric objects for now.
    stringToIntMap.put("bytea", LONGVARBINARY);
    stringToIntMap.put("cidr", VARCHAR);
    stringToIntMap.put("circle", BLOB);
    stringToIntMap.put("inet", VARCHAR);
    stringToIntMap.put("json", CLOB);
    stringToIntMap.put("line", BLOB);
    stringToIntMap.put("lseg", BLOB);
    stringToIntMap.put("macaddr", VARCHAR);
    stringToIntMap.put("macaddr8", VARCHAR);
    stringToIntMap.put("money", DOUBLE);
    stringToIntMap.put("path", BLOB);
    stringToIntMap.put("point", BLOB);
    stringToIntMap.put("polygon", BLOB);
    stringToIntMap.put("time without time zone", TIME);
    stringToIntMap.put("time with time zone", TIME);
    stringToIntMap.put("timestamp with time zone", TIMESTAMP);
    stringToIntMap.put("uuid", VARCHAR);
  }

  public static String typeName(int inttype) {
    return intToStringMap.get(inttype);
  }

  public static int typeInt(String typename) {
    //    System.out.println(typename);
    String type = typename.toLowerCase().replaceAll("\\(.*\\)", "");
    //    return stringToIntMap.get(typename.toLowerCase().replaceAll("\\(.*\\)", ""));
    return stringToIntMap.get(type);
  }

  private static HashSet<Integer> numericTypes =
      new HashSet<>(
          Arrays.asList(DECIMAL, FLOAT, DOUBLE, REAL, NUMERIC, INTEGER, TINYINT, SMALLINT, BIGINT));

  public static boolean isNumeric(String typename) {
    int type = typeInt(typename);
    return numericTypes.contains(type);
  }

  //  public static String standardize(String type) {
  //    // TODO Auto-generated method stub
  //    return null;
  //  }
}
