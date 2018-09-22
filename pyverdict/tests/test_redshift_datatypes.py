from datetime import datetime, date
import os
import pyverdict
import psycopg2

REDSHIFT_DATABASE = 'dev'
test_schema = 'pyverdict_datatype_test_redshift_schema'
test_table = 'pyverdict_datatype_test_redshift_table'


def test_redshift_data_types():
    (redshift_conn, verdict_conn) = setup_sandbox()

    result = verdict_conn.sql('select * from {}.{}'.format(test_schema, test_table))
    int_types = result.typeJavaInt()
    types = result.types()
    rows = result.rows()

    print(int_types)
    print(types)
    print(rows)
    print([type(x) for x in rows[0]])

    cur = redshift_conn.cursor()
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

    tear_down(redshift_conn)


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
    elif isinstance(expected, datetime) and expected.tzinfo:
        print(expected)
    else:
        assert expected == actual


def setup_sandbox():
    (url, port) = os.environ['VERDICTDB_TEST_REDSHIFT_ENDPOINT'].split(':')
    port = int(port)
    user = os.environ['VERDICTDB_TEST_REDSHIFT_USER']
    pswd = os.environ['VERDICTDB_TEST_REDSHIFT_PASSWORD']
    # create table and populate data
    redshift_conn = redshift_connect(url, port, user, pswd)
    '''
    cur = redshift_conn.cursor()
    #cur.execute('SET autocommit to ON;')
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema + 'CASCADE')
    cur.execute('CREATE SCHEMA IF NOT EXISTS ' + test_schema)
    cur.execute('DROP TABLE IF EXISTS ' + test_table)
    cur.execute("""
        CREATE TABLE {}.{} (
          smallintCol   SMALLINT,
          intCol        INT,
          bigintCol     BIGINT,
          decimalCol    DECIMAL,
          realCol       REAL,
          doubleCol     DOUBLE PRECISION,
          boolCol       BOOLEAN,
          charCol       CHAR(4),
          varcharCol    VARCHAR(10),
          dateCol       DATE,
          timestampCol  TIMESTAMP,
          timstamptzCol TIMESTAMPTZ
        )""".format(test_schema, test_table)
    )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
          1, 1, 1, 
          1.0, 1.0, 1.0, 
          true,
          'abc',
          'test',
          '1997-12-17',
          '1997-12-17 07:37:16',
          '1997-12-17 07:37:16-08'
        )""".format(test_schema, test_table)
    )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
          NULL, NULL, NULL, 
          NULL, NULL, NULL,
          NULL,
          NULL, 
          NULL, 
          NULL, 
          NULL,
          NULL
        )""".format(test_schema, test_table)
    )
    cur.close()
    '''
    # create verdict connection
    thispath = os.path.dirname(os.path.realpath(__file__))
    redshift_jar = os.path.join(thispath, 'lib', 'RedshiftJDBC42-1.2.16.1027.jar')
    verdict_conn = verdict_connect(url, port, user, pswd, redshift_jar)
    return (redshift_conn, verdict_conn)


def tear_down(redshift_conn):
    cur = redshift_conn.cursor()
    cur.execute('DROP TABLE IF EXISTS ' + test_table)
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema + 'CASCADE')
    cur.close()
    redshift_conn.close()


def verdict_connect(host, port, user, pswd, class_path):
    connection_string = \
        'jdbc:redshift://{:s}:{:d}/{:s}?user={:s}&password={:s}'.format(
            host, port, REDSHIFT_DATABASE, user, pswd
        )
    return pyverdict.VerdictContext(connection_string, class_path)


def redshift_connect(host, port, user, pswd):
    return psycopg2.connect(
        dbname=REDSHIFT_DATABASE, 
        host=host, 
        port=port,
        user=user,
        password=pswd
    )
