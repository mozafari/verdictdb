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

    # set up connection through java wrapper
    thispath = os.path.dirname(os.path.realpath(__file__))
    mysql_jar = os.path.join(thispath, 'lib', 'mysql-connector-java-5.1.46.jar')
    conn_verdict = verdict_connect('localhost', 3306, 'root', '', mysql_jar)

    # get table ResultSet
    result = conn_verdict.sql('SELECT * FROM pyverdict_return_types_test.test')

    # get first row
    row = result.fetchone()
    #print(row)
    assert result.rowcount == 2
    assert row[0] == 1
    assert row[1] == 2
    assert row[2] == True
    assert row[3] == 4
    assert row[4] == 5
    assert row[5] == 6
    assert row[6] == 7
    assert row[7] == 8
    assert row[8] == Decimal('8.10')
    assert row[9] == Decimal('8.20')
    assert row[10] == 8.3
    assert row[11] == 8.4
    assert row[12] == 8.5

    row = result.fetchone()
    for i in range(len(row)):
        assert row[i] == None

    row = result.fetchone()
    assert row == None

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
