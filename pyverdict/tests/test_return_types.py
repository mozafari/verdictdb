import os
import pyverdict
import pymysql
from decimal import Decimal

def test_numerical_values():
    # set up connection to mysql
    conn_mysql = mysql_connect('localhost', 3306, 'root', '')
    cur = conn_mysql.cursor()

    cur.execute('DROP SCHEMA IF EXISTS pyverdict_return_types_test')
    cur.execute('CREATE SCHEMA IF NOT EXISTS pyverdict_return_types_test')

    # create columns based on different types
    cur.execute('CREATE TABLE IF NOT EXISTS pyverdict_return_types_test.test \
        (bitCol BIT(1), tinyintCol TINYINT(2), boolCol BOOL, smallintCol SMALLINT(3), mediumintCol MEDIUMINT(4),\
        intCol INT(4), integerCol INTEGER(4), bigintCol BIGINT(8), decimalCol DECIMAL(4,2), decCol DEC(4,2),\
        floatCol FLOAT(4,2), doubleCol DOUBLE(8,2), doubleprecisionCol DOUBLE PRECISION(8,2))')
    # insert basic values
    cur.execute('INSERT INTO pyverdict_return_types_test.test \
        (bitCol, tinyintCol, boolCol, smallintCol, mediumintCol, intCol, integerCol, bigintCol, decimalCol,\
        decCol, floatCol, doubleCol, doubleprecisionCol) \
        VALUES (1, 2, 3, 4, 5, 6, 7, 8,\
                8.1, 8.2, 8.3, 8.4, 8.5)')
    # insert empty values
    cur.execute('INSERT INTO pyverdict_return_types_test.test VALUES ()')
    # insert max values
    cur.execute('INSERT INTO pyverdict_return_types_test.test \
        (bitCol, tinyintCol, boolCol, smallintCol, mediumintCol, intCol, integerCol, bigintCol, decimalCol,\
        decCol, floatCol, doubleCol, doubleprecisionCol) \
        VALUES (0, 127, 0, 32767, 8388607, 2147483647, 2147483647, 2147483648,\
                0.111, 0.222, 0.333, 0.444, 0.555)')

    # set up connection through java wrapper
    thispath = os.path.dirname(os.path.realpath(__file__))
    mysql_jar = os.path.join(thispath, 'lib', 'mysql-connector-java-5.1.46.jar')
    conn_verdict = verdict_connect('localhost', 3306, 'root', '', mysql_jar)

    # get table ResultSet
    result = conn_verdict.sql('SELECT * FROM pyverdict_return_types_test.test')
    cur.execute('SELECT * FROM pyverdict_return_types_test.test')

    # get first row
    verdict_row = result.fetchone()
    regular_row = cur.fetchone()
    assert result.rowcount == 3
    for i in range(len(verdict_row)):
        if i == 2:
            continue
        assert verdict_row[i] == regular_row[i]

    verdict_row = result.fetchone()
    regular_row = cur.fetchone()
    for i in range(len(verdict_row)):
        assert verdict_row[i] == regular_row[i]

    verdict_row = result.fetchone()
    regular_row = cur.fetchone()
    for i in range(len(verdict_row)):
        if i == 2:
            continue
        assert verdict_row[i] == regular_row[i]

    verdict_row = result.fetchone()
    regular_row = cur.fetchone()
    assert verdict_row == regular_row

    # tear down table and close connection
    cur.execute('DROP SCHEMA IF EXISTS pyverdict_return_types_test')
    cur.close()
    conn_mysql.close()


def verdict_connect(host, port, usr, pwd, class_path):
    connection_string = \
        'jdbc:mysql://{:s}:{:d}?user={:s}&password={:s}'.format(host, port, usr, pwd)
    return pyverdict.VerdictContext(connection_string, class_path)


def mysql_connect(host, port, usr, pwd):
    return pymysql.connect(host=host, port=port, user=usr, passwd=pwd, autocommit=True)
