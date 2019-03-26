import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        c_count,
        count(*) as custdist
from
        (
        select
                c_custkey,
                count(o_orderkey) as c_count
        from
                tpch10g.customer left outer join tpch10g.orders_scramble on
                c_custkey = o_custkey
        where o_comment not like '%special%requests%'
        group by
                c_custkey
        ) as c_orders
group by
        c_count
order by
        custdist desc,
        c_count desc;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("13 " + str(end_time - start_time) + " ")


query = """bypass select
        c_count,
        count(*) as custdist
from
        (
        select
                c_custkey,
                count(o_orderkey) as c_count
        from
                tpch10g.customer left outer join tpch10g.orders on
                c_custkey = o_custkey
        where o_comment not like '%special%requests%'
        group by
                c_custkey
        ) as c_orders
group by
        c_count
order by
        custdist desc,
        c_count desc;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
