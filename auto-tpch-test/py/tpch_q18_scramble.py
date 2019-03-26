import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
from
        tpch10g.lineitem_scramble l
        inner join tpch10g.orders_scramble o
                on o.o_orderkey = l.l_orderkey
        inner join
        (
        select l_orderkey, sum(l_quantity) as t_sum_quantity
        from tpch10g.lineitem_scramble
        group by l_orderkey) t
               on o.o_orderkey = t.l_orderkey
        inner join tpch10g.customer c
               on c.c_custkey = o.o_custkey
where t.t_sum_quantity > 300
group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
order by
        o_totalprice desc,
        o_orderdate
limit 10;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("18 " + str(end_time - start_time) + " ")


query = """bypass select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
from
        tpch10g.lineitem l
        inner join tpch10g.orders o
                on o.o_orderkey = l.l_orderkey
        inner join
        (
        select l_orderkey, sum(l_quantity) as t_sum_quantity
        from tpch10g.lineitem_scramble
        group by l_orderkey) t
               on o.o_orderkey = t.l_orderkey
        inner join tpch10g.customer c
               on c.c_custkey = o.o_custkey
where t.t_sum_quantity > 300
group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
order by
        o_totalprice desc,
        o_orderdate
limit 10;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
