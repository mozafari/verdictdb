import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
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

df = verdict.sql(query)
# print(df)
