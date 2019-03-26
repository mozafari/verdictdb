import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
query = """bypass select
  ps_partkey,
  sum(ps_supplycost * ps_availqty) as value
from
  tpch10g.partsupp,
  tpch10g.supplier,
  tpch10g.nation
where
  ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'GERMANY'
group by
  ps_partkey having
    sum(ps_supplycost * ps_availqty) > 10
order by
  value desc;"""

df = verdict.sql(query)
# print(df)
