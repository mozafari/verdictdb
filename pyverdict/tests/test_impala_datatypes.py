from datetime import datetime, date
import os
import pyverdict
from impala import dbapi


test_schema = 'pyverdict_datatype_test_impala_schema'
test_table = 'pyverdict_datatype_test_impala_table' # ensure that this table does not exist


def test_impala_data_types():
    (impala_conn, verdict_conn) = setup_sandbox()

    result = verdict_conn.sql('select * from {}.{}'.format(test_schema, test_table))
    int_types = result.typeJavaInt()
    types = result.types()
    rows = result.rows()
    # print(int_types)
    # print(types)
    # print(rows)
    # print([type(x) for x in rows[0]])

    cur = impala_conn.cursor()
    cur.execute('select * from {}.{}'.format(test_schema, test_table))
    expected_rows = cur.fetchall()
    cur.close()

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j])

    tear_down(impala_conn)


def compare_value(expected, actual):
    if isinstance(expected, bytes):
        if isinstance(actual, bytes):
            assert expected == actual
        else:
            assert int.from_bytes(expected, byteorder='big') == actual
    elif isinstance(expected, int) and isinstance(actual, date):
        # due to the limitation of the underlying MySQL JDBC driver, both year(2) and year(4) are
        # returned as the 'date' type; thus, we check the equality in this hacky way.
        assert expected % 100 == actual.year % 100
    else:
        assert expected == actual


def setup_sandbox():
    url = 'salat2.eecs.umich.edu'
    port = 9050

    # create table and populate data
    impala_conn = impala_connect(url, port)
    cur = impala_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema + 'CASCADE')
    cur.execute('CREATE SCHEMA IF NOT EXISTS ' + test_schema)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {}.{} (
          tinyintCol    TINYINT,
          smallintCol   SMALLINT,
          intCol        INT,
          bigintCol     BIGINT,
          decimalCol    DECIMAL,
          boolCol       BOOLEAN,
          floatCol      FLOAT,
          doubleCol     DOUBLE,
          stringCol     STRING,
          timestampCol  TIMESTAMP
        )""".format(test_schema, test_table)
    )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
          1, 1, 1, 1, 1, true,
          1.0, 1.0, 'abc',
          '2018-12-31 00:00:01'
        )""".format(test_schema, test_table)
    )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
          NULL, NULL, NULL, NULL, NULL, NULL,
          NULL, NULL, NULL, NULL
        )""".format(test_schema, test_table)
    )
    cur.close()

    # create verdict connection
    thispath = os.path.dirname(os.path.realpath(__file__))
    impala_jar = os.path.join(thispath, 'lib', 'ImpalaJDBC41-2.6.3.jar')
    verdict_conn = verdict_connect(url, port, impala_jar)

    return (impala_conn, verdict_conn)


def tear_down(impala_conn):
    cur = impala_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema + 'CASCADE')
    cur.close()
    impala_conn.close()


def verdict_connect(host, port, class_path):
    connection_string = 'jdbc:impala://{:s}:{:d}'.format(host, port)
    return pyverdict.VerdictContext(connection_string, class_path)


def impala_connect(host, port):
    return dbapi.connect(host=host, port=port)
