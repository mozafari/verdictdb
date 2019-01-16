# PyVerdict Documentation

`pyverdict` is a Python interface for VerdictDB.

## Install PyVerdict

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

    `pyverdict` ships with the JDBC drivers for MySQL, PostgreSQL, Redshift, Impala and Presto currently. More database supports will be added in the future.

### Compile from source code

Get the newest version of VerdictDB from our repo.
```
git clone git@github.com:mozafari/verdictdb.git
cd verdictdb
```
Switch to `pyverdict` root directory and install. You may need extra configurations to compile the VerdictDB jar.
```
cd pyverdict
python setup.py install
```

## Connect to Databases

`pyverdict.{CONNECT_METHOD}(host, user, password, port, ...)`

`pyverdict` uses different methods to connect to different databases, and returns a `pyverdict.VerdictContext` object. See [Connecting to Databases](/reference/connection/) for more details about `CONNECT_METHOD`.
```
import pyverdict
verdict_conn = pyverdict.mysql(host, user, password, port)
```

## Make Queries

`pyverdict.VerdictContext@sql(query)`

`pyverdict` provides a simple api to make queries. It returns a pandas DataFrame object which contains the query result.
```
df = verdict_conn.sql('SHOW SCHEMAS')
```

## Close Connection

`pyverdict.VerdictContext@close()`

`pyverdict` will close all connections automatically when the python process exits. You can also call this method to close a connection manually.
```
verdict_conn.close()
```

## Data Type Conversion Rules
Here lists the conversion rules `pyverdict` uses for different databases. Refer to the `pyverdict` source code for more details.
### MySQL
```
'bit'                     => boolean,
'tinyint'                 => int,
'bool'                    => boolean,
'smallint'                => int,
'medimumInteger'          => int,
'int'                     => int,
'integer                  => int,
'bigint'                  => int,
'decimal'                 => decimal.Decimal,
'dec'                     => decimal.Decimal,
'real'                    => float,
'double'                  => float,
'doubleprecision'         => float,
'date'      => JavaObject => datetime.date,
'datetime'  => JavaObject => datetime.datetime,
'timestamp' => JavaObject => datetime.datetime,
'time'      => JavaObject => datetime.timedelta,
'year(2)'   => JavaObject => datetime.date,
'year(4)'   => JavaObject => datetime.date,
'char'                    => str,
'varchar'                 => str,
'binary'                  => bytes,
'varbinary'               => bytes,
'tinyblob'                => bytes,
'tinytext'                => str,
'blob'                    => bytes,
'text'                    => str,
'mediumBlob'              => bytes,
'medimumText'             => str,
'longBlob'                => bytes,
'longText'                => str,
'enumCol'                 => str,
'setCol'                  => str
```

### PostgreSQL:
```
'bigint'                    => int,
'bigserial'                 => int,
'bit'               => bool => str,
'varbit'      => JavaObject => str,
'boolean'                   => bool,
'box'         => JavaObject => str,
'bytea'            => bytes => memoryview,
'char'                      => str,
'varchar'                   => str,
'cidr'        => JavaObject => str,
'circle'      => JavaObject => str,
'date'        => JavaObject => datetime.date,
'float8'                    => float,
'inet'        => JavaObject => str,
'integer'                   => int,
'json'        => JavaObject => dict,
'line'        => JavaObject => str,
'lseg'        => JavaObject => str,
'macaddr'     => JavaObject => str,
'macaddr8'    => JavaObject => str,
'money'            => float => str,
'numeric'                   => decimal.Decimal,
'path'        => JavaObject => str,
'point'       => JavaObject => str,
'polygon'     => JavaObject => str,
'real'                      => float,
'smallint'                  => int,
'smallserial'               => int,
'serial'                    => int,
'text'                      => str,
'time'        => JavaObject => datetime.time,
'timestamp'   => JavaObject => datetime.datetime,
'uuid'        => JavaObject => str,
'xml'         => JavaObject => str,
'bit'         => JavaObject => str,
'int8'                      => int,
'bool'                      => bool,
'character'                 => str,
'character'                 => str,
'int'                       => int,
'int4'                      => int,
'double'                    => float,
'decimal'                   => decimal.Decimal,
'float'                     => float,
'int2'                      => int,
'serial2'                   => int,
'serial4'                   => int,
'timetz'      => JavaObject => datetime.time,
'timestamptz' => JavaObject => datetime.datetime,
'serial8'                   => int
```

### Redshift:
```
BIGINT                                    => int,
BOOLEAN                                   => bool,
BOOL                                      => bool,
BPCHAR                                    => str,
CHAR                                      => str,
CHARACTER                                 => str,
CHARACTER VARYING                         => str,
DATE                        => JavaObject => datetime.datetime,
DECIMAL                                   => decimal.Decimal,
DOUBLE PRECISION                          => float,
FLOAT                                     => float,
FLOAT4                                    => float,
FLOAT8                                    => float,
INT                                       => int,
INT2                                      => int,
INT4                                      => int,
INTEGER                                   => int,
NCHAR                                     => str,
NUMERIC                                   => decimal.Decimal,
NVARCHAR                                  => str,
REAL                                      => float,
SMALLINT                                  => int,
TEXT                                      => str,
TIMESTAMP                   => JavaObject => datetime.datetime,
TIMESTAMP WITHOUT TIME ZONE => JavaObject => datetime.datetime,
TIMESTAMPTZ                 => JavaObject => datetime.datetime,
TIMESTAMP WITH TIME ZONE    => JavaObject => datetime.datetime,
VARCHAR                                   => str
```

### Impala
```
BIGINT           => int,
BOOLEAN          => bool,
CHAR             => str,
DECIMAL          => decimal.Decimal,
DOUBLE           => float,
FLOAT            => float,
REAL             => float,
SMALLINT         => int,
STRING           => str,
TIMESTAMP        => datetime.datetime,
TINYINT          => int,
VARCHAR          => str
```

### Presto
```
'tinyint'    => int
'boolean'    => int
'smallint'   => int
'integer'    => int
'bigint'     => int
'decimal'    => float
'real'       => float
'double'     => float
'date'       => str
'timestamp'  => str
'char'       => str
'varchar'    => str
```
