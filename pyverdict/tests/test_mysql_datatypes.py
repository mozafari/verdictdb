from datetime import datetime, date
import os
import pyverdict
import pymysql


test_schema = 'pyverdict_datatype_test_schema'
test_table = 'pyverdict_datatype_test_table'


def test_data_types():
    (mysql_conn, verdict_conn) = setup_sandbox()

    result = verdict_conn.sql('select * from {}.{}'.format(test_schema, test_table))
    int_types = result.typeJavaInt()
    types = result.types()
    rows = result.rows()
    # print(int_types)
    # print(types)
    print(rows)
    # print([type(x) for x in rows[0]])

    cur = mysql_conn.cursor()
    cur.execute('select * from {}.{}'.format(test_schema, test_table))
    expected_rows = cur.fetchall()
    print(expected_rows)
    cur.close()

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j])

    tear_down(mysql_conn)


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
    url = 'localhost'
    port = 3306
    user = 'root'
    password = ''

    # create table and populate data
    mysql_conn = mysql_connect(url, port, user, password)
    cur = mysql_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema)
    cur.execute('CREATE SCHEMA IF NOT EXISTS ' + test_schema)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {}.{} (
          bitCol              BIT(1),
          tinyintCol          TINYINT(2),
          boolCol             BOOL,
          smallintCol         SMALLINT(3),
          mediumintCol        MEDIUMINT(4),
          intCol              INT(4),
          integerCol          INTEGER(4),
          bigintCol           BIGINT(8),
          decimalCol          DECIMAL(4,2),
          decCol              DEC(4,2),
          floatCol            FLOAT(4,2),
          doubleCol           DOUBLE(8,2),
          doubleprecisionCol  DOUBLE PRECISION(8,2),
          dateCol             DATE,
          datetimeCol         DATETIME,
          timestampCol        TIMESTAMP,
          timeCol             TIME,
          yearCol2            YEAR(4),
          charCol             CHAR(4),
          varcharCol          VARCHAR(4),
          binaryCol           BINARY(4),
          varbinaryCol        VARBINARY(4),
          tinyblobCol         TINYBLOB,
          tinytextCol         TINYTEXT,
          blobCol             BLOB(4),
          textCol             TEXT(100),
          medimumblobCol      MEDIUMBLOB,
          medimumtextCol      MEDIUMTEXT,
          longblobCol         LONGBLOB,
          longtextCol         LONGTEXT,
          enumCol             ENUM('1', '2'),
          setCol              SET('1', '2')
        )""".format(test_schema, test_table)
        )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
          1, 2, 1, 1, 1, 1, 1, 1,
          1.0, 1.0, 1.0, 1.0, 1.0,
          '2018-12-31', '2018-12-31 01:00:00', '2018-12-31 00:00:01', '10:59:59',
          2018, 'abc', 'abc', '10', '10',
          '10', 'a', '10', 'abc', '1110', 'abc', '1110', 'abc', '1', '2'
        )""".format(test_schema, test_table)
        )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )""".format(test_schema, test_table)
        )
    cur.close()

    # create verdict connection
    thispath = os.path.dirname(os.path.realpath(__file__))
    mysql_jar = os.path.join(thispath, 'lib', 'mysql-connector-java-5.1.46.jar')
    verdict_conn = verdict_connect(url, port, user, password, mysql_jar)

    return (mysql_conn, verdict_conn)


def tear_down(mysql_conn):
    cur = mysql_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema)
    cur.close()
    mysql_conn.close()


def verdict_connect(host, port, usr, pwd, class_path):
    connection_string = \
        'jdbc:mysql://{:s}:{:d}?user={:s}&password={:s}'.format(host, port, usr, pwd)
    return pyverdict.VerdictContext(connection_string, class_path)


def mysql_connect(host, port, usr, pwd):
    return pymysql.connect(host=host, port=port, user=usr, passwd=pwd, autocommit=True)
