import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
query = """bypass select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
from
        tpch10g.lineitem l
        inner join tpch10g.orders o
                on o.o_orderkey = l.l_orderkey
        inner join
        (
        select l_orderkey, sum(l_quantity) as t_sum_quantity
        from tpch10g.lineitem_scramble
        group by l_orderkey) t
               on o.o_orderkey = t.l_orderkey
        inner join tpch10g.customer c
               on c.c_custkey = o.o_custkey
where t.t_sum_quantity > 300
group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
order by
        o_totalprice desc,
        o_orderdate
limit 10;"""

verdict.sql(query)
# print(df)
