import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
verdict.sql('use tpch10g')
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
  tpch10g.part,
  tpch10g.supplier,
  tpch10g.partsupp,
  tpch10g.nation,
  tpch10g.region
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
      tpch10g.partsupp,
      tpch10g.supplier,
      tpch10g.nation,
      tpch10g.region
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

df = verdict.sql(query)
print(df)
