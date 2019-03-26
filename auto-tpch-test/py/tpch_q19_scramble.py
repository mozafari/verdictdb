import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
       sum(l_extendedprice* (1 - l_discount)) as revenue
from
       tpch10g.lineitem_scramble,
       tpch10g.part
where
       (
       p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
       and l_quantity >= 1 and l_quantity <= 11
       and p_size between 1 and 5
       and l_shipmode in ('AIR', 'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
      p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      and l_quantity >= 10 and l_quantity <= 20
      and p_size between 1 and 10
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
      p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      and l_quantity >= 20 and l_quantity <= 30
      and p_size between 1 and 15
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
     );"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("19 " + str(end_time - start_time) + " ")


query = """bypass select
       sum(l_extendedprice* (1 - l_discount)) as revenue
from
       tpch10g.lineitem,
       tpch10g.part
where
       (
       p_partkey = l_partkey
       and p_brand = 'Brand#12'
       and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
       and l_quantity >= 1 and l_quantity <= 11
       and p_size between 1 and 5
       and l_shipmode in ('AIR', 'AIR REG')
       and l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
      p_partkey = l_partkey
      and p_brand = 'Brand#23'
      and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      and l_quantity >= 10 and l_quantity <= 20
      and p_size between 1 and 10
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
      p_partkey = l_partkey
      and p_brand = 'Brand#34'
      and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      and l_quantity >= 20 and l_quantity <= 30
      and p_size between 1 and 15
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
     );"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
