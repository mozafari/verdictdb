import os
import pyverdict
import pymysql

def test_count():
    conn_mysql = mysql_connect('localhost', 3306, 'root', '')
    cur = conn_mysql.cursor()
    cur.execute('DROP SCHEMA IF EXISTS pyverdict_simple_test')
    cur.execute('CREATE SCHEMA IF NOT EXISTS pyverdict_simple_test')
    cur.execute('CREATE TABLE IF NOT EXISTS pyverdict_simple_test.test (id INT)')
    cur.execute('INSERT INTO pyverdict_simple_test.test SELECT 1')

    thispath = os.path.dirname(os.path.realpath(__file__))
    mysql_jar = os.path.join(thispath, 'lib', 'mysql-connector-java-5.1.46.jar')
    # print(mysql_jar)
    conn_verdict = verdict_connect('localhost', 3306, 'root', '', mysql_jar)
    verdict_result = conn_verdict.sql('SELECT COUNT(1) from pyverdict_simple_test.test')
    cur.execute('SELECT * FROM pyverdict_simple_test.test')
    regular_result = cur

    assert verdict_result.fetchone()[0] == regular_result.fetchone()[0]
    assert verdict_result.fetchone() == regular_result.fetchone()

    cur.execute('DROP SCHEMA IF EXISTS pyverdict_simple_test')
    cur.close()
    conn_mysql.close()

def verdict_connect(host, port, usr, pwd, class_path):
    connection_string = \
        'jdbc:mysql://{:s}:{:d}?user={:s}&password={:s}'.format(host, port, usr, pwd)
    return pyverdict.VerdictContext(connection_string, class_path)

def mysql_connect(host, port, usr, pwd):
    return pymysql.connect(host=host, port=port, user=usr, passwd=pwd, autocommit=True)
