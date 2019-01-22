# VerdictDB on MySQL

On this page, we will demonstrate an example of a Java/Python application with a VerdictDB library that creates a scrambled table for your database, then executes a query that enable VerdictDB to utilize the scrambled table.

Here we assume:

1. Your MySQL database is running
1. TPC-H data has been loaded into your database following the instructions in the previous [Setup TPC-H Data](/tutorial/tpch/) page.


## Getting VerdictDB Example Application

Before we start, you need to get our example Java/Python application that we will be referring to throughout this entire tutorial. To do so, you need `git` and run the following command in your working directory:

```bash
$ git clone git@github.com:verdictdb/verdictdb-tutorial.git
```

This will clone the current VerdictDB tutorial application into `your_working_directory/verdictdb-tutorial`.

Move into the directory with the example we will use.

```bash tab='Java'
cd verdictdb_on_mysql
```

```bash tab='Python'
cd pyverdict_on_mysql
```

## Compiling the Application

The tutorial java application has been set up with Apache Maven and you should be able to build a runnable jar file under `your_working_directory/verdictdb-tutorial/target`.

For the python application, install the required packages with pip.


```bash tab='Java'
mvn package
```

```bash tab='Python'
pip install -r requirements.txt
```


## Running the Application

We included a shell script `run.sh` that you can invoke to execute the example Java application.
The script takes 4 arguments: `hostname`, `port`, `name of database/schema`, and `command`.

## Creating a Scrambled Table

In this tutorial, we are going to create a scrambled table on `lineitem` table. Assume you are running the MySQL database following [this page](/tutorial/setup/mysql/):

```bash tab='Java'
./run.sh localhost 3306 tpch1g create
```

```bash tab='Python'
python tutorial.py create
```

It prints something like this:

```
Creating a scrambled table for lineitem...
Scrambled table for lineitem has been created.
Time Taken = 67 s
```

This command executes the following SQL statement via VerdictDB:

```SQL
CREATE SCRAMBLE tpch1g.lineitem_scramble FROM tpch1g.lineitem
```

Snippet of the corresponding source code (simplified for brevity):

```java tab='Java'
...
Statement stmt = verdictConn.createStatement();
String createQuery =
    String.format(
        "CREATE SCRAMBLE %s.lineitem_scramble " + "FROM %s.lineitem",
        database, database);
System.out.println("Creating a scrambled table for lineitem...");
stmt.execute(createQuery);
stmt.close();
System.out.println("Scrambled table for lineitem has been created.");
...
```

```python tab='Python'
...
print('Creating a scrambled table for lineitem...')
start = time.time()
verdict_conn.sql('CREATE SCRAMBLE tpch1g.lineitem_scramble FROM tpch1g.lineitem')
duration = time.time() - start
verdict_conn.close()
print('Scrambled table for lineitem has been created.')
print('Time Taken = {:f} s'.format(duration))
...
```

It's just like running SQL statement via any JDBC connection.

After the scrambled table being created, VerdictDB will automatically utilize this scrambled table for any query that involves the `lineitem` table.

## Running a Sample Query

The example application can execute one very simple aggregation query with and without VerdictDB and compare their performance:

```SQL
SELECT avg(l_extendedprice) FROM tpch1g.lineitem
```

To run this query with/without VerdictDB, you can simply run the following command:

```bash tab='Java'
./run.sh localhost 3306 tpch1g run
```

```bash tab='Python'
python tutorial.py run
```

The result looks like this:
```
Without VerdictDB: average(l_extendedprice) = 38255.138485
Time Taken = 43 s
With VerdictDB: average(l_extendedprice) = 38219.8871659
Time Taken = 10 s
```

You can see that VerdictDB achieves more than 4x speedup with a 99.9% accurate result even with a relatively small dataset that we use in this tutorial (i.e., 1GB).
