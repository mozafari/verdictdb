# Quickstart Guide (Java)

We will install VerdictDB, create a connection, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.


## Install

Create an [empty Maven project](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) and
place the following dependency in the `<dependencies>` of your pom.xml.
```pom
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>{{verdictdb.version}}</version>
</dependency>
```

To use MySQL, add the following entry as well:
```pom
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.46</version>
</dependency>
```


## Insert Data

We will first generate small data to play with.

```java
// Suppose username is root and password is rootpassword.
Connection mysqlConn =
    DriverManager.getConnection("jdbc:mysql://localhost", "root", "rootpassword");
Statement stmt = mysqlConn.createStatement();
stmt.execute("create schema myschema");
stmt.execute("create table myschema.sales (" +
             "  product   varchar(100)," +
             "  price     double)");

// insert 1000 rows
List<String> productList = Arrays.asList("milk", "egg", "juice");
for (int i = 0; i < 1000; i++) {
  int randInt = ThreadLocalRandom.current().nextInt(0, 3);
  String product = productList.get(randInt);
  double price = (randInt+2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
  stmt.execute(String.format(
      "INSERT INTO myschema.sales (product, price) VALUES('%s', %.0f)",
      product, price));
}
```



## Test VerdictDB

Create a JDBC connection to VerdictDB.

```java
Connection verdict =
    DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "rootpassword");
Statement vstmt = verdict.createStatement();
```

Create a special table called a "scramble", which is the replica of the original table with extra information VerdictDB uses for speeding up query processing.

```java
vstmt.execute("create scramble myschema.sales_scrambled from myschema.sales");
```

Run just a regular query to the original table.

```java
ResultSet rs = vstmt.executeQuery(
    "select product, avg(price) "+
    "from myschema.sales " +
    "group by product " +
    "order by product");
```

Internally, VerdictDB rewrites the above query to use the scramble. It is equivalent to explicitly specifying the scramble in the from clause of the above query.


## Complete Example Java File

*(Yongjoo: update this according to the above code)*

```java
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class FirstVerdictDBExample {


  public static void main(String args[]) throws SQLException {
    // Suppose username is root and password is rootpassword.
    Connection mysqlConn =
        DriverManager.getConnection("jdbc:mysql://localhost", "root", "rootpassword");
    Statement stmt = mysqlConn.createStatement();
    stmt.execute("create schema myschema");
    stmt.execute("create table myschema.sales (" +
                 "  product   varchar(100)," +
                 "  price     double)");

    // insert 1000 rows
    List<String> productList = Arrays.asList("milk", "egg", "juice");
    for (int i = 0; i < 1000; i++) {
      int randInt = ThreadLocalRandom.current().nextInt(0, 3)
      String product = productList.get(randInt);
      double price = (randInt+2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
      stmt.execute(String.format(
          "INSERT INTO myschema.sales (product, price) VALUES('%s', %.0f)",
          product, price));
    }

    Connection verdict =
        DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "rootpassword");
    Statement vstmt = verdict.createStatement();

    // Use CREATE SCRAMBLE syntax to create scrambled tables.
    vstmt.execute("create scramble myschema.sales_scrambled from myschema.sales");

    ResultSet rs = vstmt.executeQuery(
        "select product, avg(price) "+
        "from myschema.sales_scrambled " +
        "group by product " +
        "order by product");

    // Do something after getting the results.
  }
}
```
