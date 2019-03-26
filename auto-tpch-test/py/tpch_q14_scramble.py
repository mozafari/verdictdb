import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        100.00 * sum(case
                when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
                else 0
        end) as numerator,
        sum(l_extendedprice * (1 - l_discount)) as denominator
from
        tpch10g.lineitem_scramble,
        tpch10g.part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-10-01';"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("14 " + str(end_time - start_time) + " ")


query = """bypass select
        100.00 * sum(case
                when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
                else 0
        end) as numerator,
        sum(l_extendedprice * (1 - l_discount)) as denominator
from
        tpch10g.lineitem,
        tpch10g.part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-10-01';"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
