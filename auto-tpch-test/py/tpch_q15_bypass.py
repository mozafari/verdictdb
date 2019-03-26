import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
query = """bypass select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
from
        tpch10g.lineitem
        where
        l_shipdate >= date '1995-01-01'
        and l_shipdate < date '1996-01-01'
group by
        l_suppkey;"""

df = verdict.sql(query)
# print(df)
