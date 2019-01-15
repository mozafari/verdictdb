# pyVerdict Documentation

`pyverdict` is a Python interface to VerdictDB.

## Install pyVerdict

The easiest way to get `pyverdict` is installing from PyPI. You can also compile from source code for developing purpose.

!!! warn "Note: Prerequisites"
    `pyverdict` requires [miniconda](https://conda.io/docs/user-guide/install/index.html) for Python 3.7,
    which can be installed for local users (i.e., without sudo access).

### Install from PyPI

`pyverdict` is distributed with PyPI. Use the following command for installation.

```
pip install pyverdict
```
or
```
pip install pyverdict --upgrade
```

!!! warn "Note: Dependencies"
    `pyverdict` ships with a latest VerdictDB jar in it, so no separate installation is necessary.

### Compile from source code

Get the newest version of VerdictDB from our repo.
```
git clone git@github.com:mozafari/verdictdb.git
cd verdictdb
```
Switch to `pyverdict` root directory and install `pyverdict`. You may need extra configurations to compile the VerdictDB jar.
```
python setup.py install
```

## Connect to Databases

`pyverdict.VerdictContext(url, extra_class_path=None)`

`pyverdict` connects to databases using JDBC Driver Connection URL strings, and returns a `VerdictContext` object. The initializer also provides a way to specify the location of VerdictDB Jar file.

For MySQL

```
connection_url = \
    'jdbc:mysql://{:s}:{:d}?user={:s}&password={:s}'.format(
        host,
        port,
        user,
        password
    )
```
For PostgreSQL
```
connection_url = \
    'jdbc:postgresql://{:s}:{:s}/{:s}?user={:s}&password={:s}'.format(
            host,
            port,
            dbname,
            user,
            password
    )
```
```
import pyverdict
verdict_conn = pyverdict.VerdictContext(connection_string)
```

## Making Queries

`pyverdict.VerdictContext@sql(query)`

`pyverdict` provides a simple api to make queries. It returns a pandas DataFrame object which contains the query result. For data type conversions, refer to the doc strings in source code.

```
df = verdict_conn('SHOW SCHEMAS')
```

## Close Connection

`pyverdict.VerdictContext@close()`

`pyverdict` will close all connections automatically when the python process exits. You can also call this method to close a connection manually.
```
verdict_conn.close()
```


!!! warn "Note: Dependencies"
    `pyverdict` supports MySQL, PostgreSQL, Redshift, Impala and Presto currently, and ships with the JDBC drivers for them. More databases will be supported in the future.
