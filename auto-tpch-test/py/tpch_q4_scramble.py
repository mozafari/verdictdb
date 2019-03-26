import pyverdict
import time
import sys

filename = sys.argv[1]
size = sys.argv[2]
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        o_orderpriority,
        count(*) as order_count
from
        tpch{}g.orders_scramble join tpch{}g.lineitem_scramble on l_orderkey = o_orderkey
where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1998-12-01'
        and l_commitdate < l_receiptdate
group by
        o_orderpriority
order by
        o_orderpriority;""".format(size, size)


start_time = time.time()
df = verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("4  " + str(end_time - start_time) + " ")


query = """bypass select
        o_orderpriority,
        count(*) as order_count
from
        tpch{}g.orders join tpch{}g.lineitem on l_orderkey = o_orderkey
where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1998-12-01'
        and l_commitdate < l_receiptdate
group by
        o_orderpriority
order by
        o_orderpriority;""".format(size, size)

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
