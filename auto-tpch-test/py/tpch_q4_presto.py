import prestodb


conn=prestodb.dbapi.connect(
    host='localhost',
    port=9080,
    user='zjcsjtu',
    catalog='hive',
    schema='tpch10g',
)
query = """select
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
        o_orderpriority"""

cur = conn.cursor()
cur.execute(query)
rows = cur.fetchall()
print(rows)
