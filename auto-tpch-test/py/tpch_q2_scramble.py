import pyverdict
import time
import sys

filename = sys.argv[1]
size = sys.argv[2]
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
verdict.sql('use tpch{}g'.format(size))
query = """select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  part,
  supplier,
  partsupp,
  nation,
  region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 37
  and p_type like '%COPPER'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost = (
    select
      min(ps_supplycost)
    from
      partsupp,
      supplier,
      nation,
      region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
  )
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 100;"""


start_time = time.time()
df = verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("2  " + str(end_time - start_time) + " ")


query = """bypass select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  tpch{}g.part,
  tpch{}g.supplier,
  tpch{}g.partsupp,
  tpch{}g.nation,
  tpch{}g.region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 37
  and p_type like '%COPPER'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'EUROPE'
  and ps_supplycost = (
    select
      min(ps_supplycost)
    from
      tpch{}g.partsupp,
      tpch{}g.supplier,
      tpch{}g.nation,
      tpch{}g.region
    where
      p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'EUROPE'
  )
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey
limit 100;""".format(size, size, size, size, size, size, size, size, size)

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
