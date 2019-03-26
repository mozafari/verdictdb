import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        l_partkey,
        l_suppkey,
        0.5 * sum(l_quantity) as total
from
        tpch10g.lineitem_scramble
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1995-01-01'
group by
        l_partkey,
        l_suppkey
order by
        total desc
limit 10;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("20 " + str(end_time - start_time) + " ")


query = """bypass select
        l_partkey,
        l_suppkey,
        0.5 * sum(l_quantity) as total
from
        tpch10g.lineitem
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1995-01-01'
group by
        l_partkey,
        l_suppkey
order by
        total desc
limit 10;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
