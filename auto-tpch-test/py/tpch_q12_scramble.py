import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
       l_shipmode,
       sum(case
               when o_orderpriority = '1-URGENT'
               or o_orderpriority = '2-HIGH'
               then 1
               else 0
       end) as high_line_count,
       sum(case
               when o_orderpriority <> '1-URGENT'
               and o_orderpriority <> '2-HIGH'
               then 1
               else 0
       end) as low_line_count
from
       tpch10g.orders_scramble,
       tpch10g.lineitem_scramble
where
       o_orderkey = l_orderkey
       and l_shipmode in ('MAIL', 'SHIP')
       and l_commitdate < l_receiptdate
       and l_shipdate < l_commitdate
       and l_receiptdate >= date '1994-01-01'
       and l_receiptdate < date '1995-01-01'
group by
       l_shipmode
order by
       l_shipmode;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("12 " + str(end_time - start_time) + " ")


query = """bypass select
       l_shipmode,
       sum(case
               when o_orderpriority = '1-URGENT'
               or o_orderpriority = '2-HIGH'
               then 1
               else 0
       end) as high_line_count,
       sum(case
               when o_orderpriority <> '1-URGENT'
               and o_orderpriority <> '2-HIGH'
               then 1
               else 0
       end) as low_line_count
from
       tpch10g.orders,
       tpch10g.lineitem
where
       o_orderkey = l_orderkey
       and l_shipmode in ('MAIL', 'SHIP')
       and l_commitdate < l_receiptdate
       and l_shipdate < l_commitdate
       and l_receiptdate >= date '1994-01-01'
       and l_receiptdate < date '1995-01-01'
group by
       l_shipmode
order by
       l_shipmode;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
