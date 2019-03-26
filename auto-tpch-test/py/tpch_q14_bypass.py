import pyverdict


verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g;')
query = """bypass select
        100.00 * sum(case
                when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
                else 0
        end) as numerator,
        sum(l_extendedprice * (1 - l_discount)) as denominator
from
        tpch10g.lineitem,
        tpch10g.part
where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-10-01';"""

df = verdict.sql(query)
print(df)
