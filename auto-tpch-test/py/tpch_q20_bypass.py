import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
query = """bypass select
        l_partkey,
        l_suppkey,
        0.5 * sum(l_quantity) as total
from
        tpch10g.lineitem
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1995-01-01'
group by
        l_partkey,
        l_suppkey
order by
        total desc
limit 10;"""

verdict.sql(query)
# print(df)
