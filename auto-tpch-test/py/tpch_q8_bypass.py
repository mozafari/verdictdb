import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """bypass select
        o_year,
        sum(case
                when nation = 'BRAZIL' then volume
                else 0
        end) as numerator,
        sum(volume) as denominator
from
        (
        select
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
        from
               tpch10g.part,
               tpch10g.supplier,
               tpch10g.lineitem,
               tpch10g.orders,
               tpch10g.customer,
               tpch10g.nation n1,
               tpch10g.nation n2,
               tpch10g.region
        where
               p_partkey = l_partkey
               and s_suppkey = l_suppkey
               and l_orderkey = o_orderkey
               and o_custkey = c_custkey
               and c_nationkey = n1.n_nationkey
               and n1.n_regionkey = r_regionkey
               and r_name = 'AMERICA'
               and s_nationkey = n2.n_nationkey
               and o_orderdate between date '1995-01-01' and date '1996-12-31'
               and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year;"""

df = verdict.sql(query)
print(df)
