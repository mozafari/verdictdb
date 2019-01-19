# Quickstart Guide

We will install VerdictDB, create a connection, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.


## Install

### Java
Create an [empty Maven project](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) and
place the verdictdb dependency in the `<dependencies>` of your pom.xml.

### Python
`pyverdict` is distributed with [PyPI](https://pypi.org/project/pyverdict/). No installation of VerdictDB is required.

To insert data into MySQL in Python without `pyverdict`, we could use [pymysql](https://pymysql.readthedocs.io/en/latest/).

!!! warn "Note: Prerequisites"
    `pyverdict` requires [miniconda](https://conda.io/docs/user-guide/install/index.html) for Python 3.7,
    which can be installed for local users (i.e., without sudo access).


```xml tab='Java'
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>{{verdictdb.version}}</version>
</dependency>

<!-- To use MySQL, add the following entry as well: -->
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.46</version>
</dependency>
```

```bash tab='Python'
pip install pyverdict
# use the following line for upgrading:
# pip install pyverdict --upgrade

# install pymysql to use MySQL
pip install pymysql
```


## Insert Data

We will first generate small data to play with. Suppose MySQL is set up as described [here](/tutorial/setup/mysql/).

```java tab='Java'
Connection mysqlConn =
    DriverManager.getConnection("jdbc:mysql://localhost", "root", "");
Statement stmt = mysqlConn.createStatement();
stmt.execute("CREATE SCHEMA myschema");
stmt.execute("CREATE TABLE myschema.sales (" +
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

```python tab='Python'
mysql_conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    passwd='',
    autocommit=True
)
cur = mysql_conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS myschema')
cur.execute('CREATE SCHEMA myschema')
cur.execute(
    'CREATE TABLE myschema.sales (' +
    '   product varchar(100),' +
    '   price   double)'
)
# insert 1000 rows
product_list = ['milk', 'egg', 'juice']
random.seed(0)
for i in range(1000):
    rand_idx = random.randint(0, 2)
    product = product_list[rand_idx]
    price = (rand_idx + 2) * 10 + random.randint(0, 10)
    cur.execute(
        'INSERT INTO myschema.sales (product, price)' +
        '   VALUES ("{:s}", {:f})'.format(product, price)
    )
cur.close()
```


## Test VerdictDB

Create a JDBC connection to VerdictDB. It may take a few seconds to launch the VerdictDB JVM in `PyVerdict`.

```java tab='Java'
Connection verdict =
    DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "");
Statement vstmt = verdict.createStatement();
```

```python tab='Python'
verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='',
    port=3306
)
```

Create a special table called a "scramble", which is the replica of `sales` with extra information VerdictDB uses for speeding up query processing.

```java tab='Java'
vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales");
```

```python tab='Python'
verdict_conn.sql('CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales')
```

Run a regular query to the original table. In `PyVerdict`, The query result is stored in a pandas DataFrame. The values may vary.

```java tab='Java'
ResultSet rs = vstmt.executeQuery(
    "SELECT product, AVG(price) "+
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product");
```

```python tab='Python'
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product")
# df
#   product                 a2
# 0     egg  34.82142857142857
# 1   juice  44.96363636363636
# 2    milk  24.97005988023952
```

Internally, VerdictDB rewrites the above query to use the scramble. It is equivalent to explicitly specifying the scramble in the `FROM` clause of the above query.


## Complete Example Script


```java tab='Java'
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class FirstVerdictDBExample {


  public static void main(String args[]) throws SQLException {
    // Suppose username is root and password is rootpassword.
    Connection mysqlConn =
        DriverManager.getConnection("jdbc:mysql://localhost", "root", "");
    Statement stmt = mysqlConn.createStatement();
    stmt.execute("CREATE SCHEMA myschema");
    stmt.execute("CREATE TABLE myschema.sales (" +
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
        DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "");
    Statement vstmt = verdict.createStatement();

    // Use CREATE SCRAMBLE syntax to create scrambled tables.
    vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales");

    ResultSet rs = vstmt.executeQuery(
        "SELECT product, AVG(price) "+
        "FROM myschema.sales " +
        "GROUP BY product " +
        "ORDER BY product");
    ```

    // Do something after getting the results.
  }
}
```

```python tab='Python'
import random
import pymysql
import pyverdict

mysql_conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    passwd='',
    autocommit=True
)
cur = mysql_conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS myschema')
cur.execute('CREATE SCHEMA myschema')
cur.execute(
    'CREATE TABLE myschema.sales (' +
    '   product varchar(100),' +
    '   price   double)'
)

# insert 1000 rows
product_list = ['milk', 'egg', 'juice']
random.seed(0)
for i in range(1000):
    rand_idx = random.randint(0, 2)
    product = product_list[rand_idx]
    price = (rand_idx + 2) * 10 + random.randint(0, 10)
    cur.execute(
        'INSERT INTO myschema.sales (product, price)' +
        '   VALUES ("{:s}", {:f})'.format(product, price)
    )

cur.close()

# create connection
verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='',
    port=3306
)

# create scramble table
verdict_conn.sql('CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales')

# run query
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product")
```
