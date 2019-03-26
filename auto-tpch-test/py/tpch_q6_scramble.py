import pyverdict
import time
import sys

filename = sys.argv[1]
size = sys.argv[2]
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        sum(l_extendedprice * l_discount) as revenue
from
        tpch{}g.lineitem_scramble
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1995-01-01'
        and l_discount between 0.05 and 0.07
        and l_quantity < 24;""".format(size)


start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("6  " + str(end_time - start_time) + " ")


query = """bypass select
        sum(l_extendedprice * l_discount) as revenue
from
        tpch{}g.lineitem
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1995-01-01'
        and l_discount between 0.05 and 0.07
        and l_quantity < 24;""".format(size)

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
