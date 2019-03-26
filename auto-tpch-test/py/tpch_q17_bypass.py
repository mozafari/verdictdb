import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
query = """bypass select
        sum(extendedprice) / 7.0 as avg_yearly
from
        (
        select
                l_quantity as quantity,
                l_extendedprice as extendedprice,
                t_avg_quantity
        from
                (select
                        l_partkey as t_partkey,
                        0.2 * avg(l_quantity) as t_avg_quantity
                from
                        tpch10g.lineitem
                group by l_partkey) as q17_lineitem_tmp_cached
                Inner Join
                (select
                        l_quantity,
                        l_partkey,
                        l_extendedprice
                from
                        tpch10g.part,
                        tpch10g.lineitem
                where
                        p_partkey = l_partkey
                        and p_brand = 'Brand#23'
                        and p_container = 'MED BOX'
                ) as l1 on l1.l_partkey = t_partkey
        ) a
where quantity < t_avg_quantity;"""

df = verdict.sql(query)
# print(df)
