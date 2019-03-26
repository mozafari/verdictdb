import pyverdict
import time
import sys

filename = sys.argv[1]
sz = sys.argv[2]
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        tpch{}g.customer,
        tpch{}g.orders_scramble,
        tpch{}g.lineitem_scramble,
        tpch{}g.nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-12-01'
        and o_orderdate < date '1997-03-01'
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
order by
        revenue desc
limit 20;""".format(sz, sz, sz, sz)


start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("10 " + str(end_time - start_time) + " ")


query = """bypass select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        tpch{}g.customer,
        tpch{}g.orders,
        tpch{}g.lineitem,
        tpch{}g.nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-12-01'
        and o_orderdate < date '1997-03-01'
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
order by
        revenue desc
limit 20;""".format(sz, sz, sz, sz)

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
