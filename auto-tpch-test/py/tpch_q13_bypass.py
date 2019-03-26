import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
query = """bypass select
        c_count,
        count(*) as custdist
from
        (
        select
                c_custkey,
                count(o_orderkey) as c_count
        from
                tpch10g.customer left outer join tpch10g.orders on
                c_custkey = o_custkey
        where o_comment not like '%special%requests%'
        group by
                c_custkey
        ) as c_orders
group by
        c_count
order by
        custdist desc,
        c_count desc;"""

df = verdict.sql(query)
print(df)
