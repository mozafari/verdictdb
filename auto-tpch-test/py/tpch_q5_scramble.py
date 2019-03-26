import pyverdict
import time
import sys

filename = sys.argv[1]
size = sys.argv[2]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        tpch{}g.customer,
        tpch{}g.orders_scramble,
        tpch{}g.lineitem_scramble,
        tpch{}g.supplier,
        tpch{}g.nation,
        tpch{}g.region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-12-01'
        and o_orderdate < date '1995-12-01'
group by
        n_name
order by
        revenue desc;""".format(size, size, size, size, size, size)


start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("5  " + str(end_time - start_time) + " ")


query = """bypass select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        tpch{}g.customer,
        tpch{}g.orders,
        tpch{}g.lineitem,
        tpch{}g.supplier,
        tpch{}g.nation,
        tpch{}g.region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-12-01'
        and o_orderdate < date '1995-12-01'
group by
        n_name
order by
        revenue desc;""".format(size, size, size, size, size, size)

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
