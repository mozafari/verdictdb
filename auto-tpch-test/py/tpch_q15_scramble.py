import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
from
        tpch10g.lineitem_scramble
        where
        l_shipdate >= date '1995-01-01'
        and l_shipdate < date '1996-01-01'
group by
        l_suppkey;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("15 " + str(end_time - start_time) + " ")


query = """bypass select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
from
        tpch10g.lineitem
        where
        l_shipdate >= date '1995-01-01'
        and l_shipdate < date '1996-01-01'
group by
        l_suppkey;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
