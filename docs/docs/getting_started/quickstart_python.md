# Quickstart Guide (Python)

We will install `pyverdict` (a Python interface to VerdictDB), create a connection, and issue a simple query. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.


## Install

`pyverdict` is distributed with [PyPI](https://pypi.org/project/pyverdict/). No installation of VerdictDB is required. Use the following command for installation.

```
pip install pyverdict
```
or
```
pip install pyverdict --upgrade
```

!!! warn "Note: Prerequisites"
    `pyverdict` requires [miniconda](https://conda.io/docs/user-guide/install/index.html) for Python 3.7,
    which can be installed for local users (i.e., without sudo access).

To insert data into MySQL in Python without `pyverdict`, we could use [pymysql](https://pymysql.readthedocs.io/en/latest/)
```
pip install pymysql
```

## Insert Data
We will first generate some data to play with. Suppose MySQL is setup as described [here](/tutorial/setup/mysql/).
```python
mysql_conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    passwd='',
    autocommit=True
)
cur = mysql_conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS toy_schema')
cur.execute('CREATE SCHEMA toy_schema')
cur.execute(
    'CREATE TABLE toy_schema.sales (' +
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
        'INSERT INTO toy_schema.sales (product, price)' +
        '   VALUES ("{:s}", {:f})'.format(product, price)
    )
cur.close()

```

## Test PyVerdict

Create a connection to VerdictDB. It may take a few seconds to launch the VerdictDB JVM.
```python
verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='',
    port=3306
)
```
Create a scramble table, which is the replica of `sales` with extra information VerdictDB uses for speeding up query processing.
```python
verdict_conn.sql('CREATE SCRAMBLE toy_schema.sales_scrambled from toy_schema.sales')
```
Run a regular query to the original table. The query result is stored in pandas DataFrame. The values may vary.
```python
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM toy_schema.sales " +
    "GROUP BY product " +
    "ORDER BY product")
# df
#   product                 a2
# 0     egg  34.82142857142857
# 1   juice  44.96363636363636
# 2    milk  24.97005988023952
```
Internally, VerdictDB rewrites the above query to use the scramble. It is equivalent to explicitly specifying the scramble in the from clause of the above query.

## Complete Example Python File
```python
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
cur.execute('DROP SCHEMA IF EXISTS toy_schema')
cur.execute('CREATE SCHEMA toy_schema')
cur.execute(
    'CREATE TABLE toy_schema.sales (' +
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
        'INSERT INTO toy_schema.sales (product, price)' +
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
verdict_conn.sql('CREATE SCRAMBLE toy_schema.sales_scrambled from toy_schema.sales')

# run query
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM toy_schema.sales " +
    "GROUP BY product " +
    "ORDER BY product")

```
