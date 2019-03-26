import pyverdict
import time
import sys

filename = sys.argv[1]
sz = sys.argv[2]
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			tpch{}g.part,
			tpch{}g.supplier,
			tpch{}g.lineitem_scramble,
			tpch{}g.partsupp,
			tpch{}g.orders_scramble,
			tpch{}g.nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;""".format(sz, sz, sz, sz, sz, sz)


start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("9  " + str(end_time - start_time) + " ")


query = """bypass select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			tpch{}g.part,
			tpch{}g.supplier,
			tpch{}g.lineitem,
			tpch{}g.partsupp,
			tpch{}g.orders,
			tpch{}g.nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;""".format(sz, sz, sz, sz, sz, sz)

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
