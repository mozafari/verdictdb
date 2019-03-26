import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """bypass select
        o_orderpriority,
        count(*) as order_count
from
        tpch10g.orders join tpch10g.lineitem on l_orderkey = o_orderkey
where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1998-12-01'
        and l_commitdate < l_receiptdate
group by
        o_orderpriority
order by
        o_orderpriority;"""

df = verdict.sql(query)
print(df)
