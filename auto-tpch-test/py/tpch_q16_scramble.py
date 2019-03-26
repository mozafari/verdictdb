import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
  p_brand,
  p_type,
  p_size,
  count(distinct ps_suppkey) as supplier_cnt
from
  tpch10g.partsupp,
  tpch10g.part
where
  p_partkey = ps_partkey
  and p_brand <> 'Brand#34'
  and p_type not like 'ECONOMY BRUSHED%'
  and p_size in (22, 14, 27, 49, 21, 33, 35, 28)
  and ps_suppkey not in (
    select
      s_suppkey
    from
      tpch10g.supplier
    where
      s_comment like '%Customer%Complaints%'
  )
group by
  p_brand,
  p_type,
  p_size
order by
  supplier_cnt desc,
  p_brand,
  p_type,
  p_size;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("16 " + str(end_time - start_time) + " ")


query = """bypass select
  p_brand,
  p_type,
  p_size,
  count(distinct ps_suppkey) as supplier_cnt
from
  tpch10g.partsupp,
  tpch10g.part
where
  p_partkey = ps_partkey
  and p_brand <> 'Brand#34'
  and p_type not like 'ECONOMY BRUSHED%'
  and p_size in (22, 14, 27, 49, 21, 33, 35, 28)
  and ps_suppkey not in (
    select
      s_suppkey
    from
      tpch10g.supplier
    where
      s_comment like '%Customer%Complaints%'
  )
group by
  p_brand,
  p_type,
  p_size
order by
  supplier_cnt desc,
  p_brand,
  p_type,
  p_size;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
