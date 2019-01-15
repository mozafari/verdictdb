# Quickstart Guide (Python)

We will install `pyverdict`, a Python interface to VerdictDB, and issue a simple query.
In this Quickstart Guide, we will use MySQL for VerdictDB's backend database.


## Install

`pyverdict` is distributed with PyPI. Use the following command for installation.

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


## Connect

Suppose MySQL is set as described on [this page](/tutorial/setup/mysql/).

```python
import pyverdict

connection_url = 'jdbc:mysql://localhost:3306?user=root&password='
verdict = pyverdict.VerdictContext(connection_url)
df = verdict.sql('show schemas')     # df is a pandas DataFrame containing schema names
```

!!! warn "Note: Supported Databases"
    `pyverdict` supports MySQL, PostgreSQL, Redshift, Impala and Presto currently.
