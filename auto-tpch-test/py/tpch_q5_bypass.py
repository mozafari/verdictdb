import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """bypass select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        tpch10g.customer,
        tpch10g.orders,
        tpch10g.lineitem,
        tpch10g.supplier,
        tpch10g.nation,
        tpch10g.region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-12-01'
        and o_orderdate < date '1995-12-01'
group by
        n_name
order by
        revenue desc;"""

df = verdict.sql(query)
print(df)
