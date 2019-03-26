import pyverdict
import time
import sys

filename = sys.argv[1]
size = sys.argv[2]
print(filename)
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
verdict.sql('use tpch10g')
query = """select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        tpch{}g.lineitem_scramble
where
        l_shipdate <= date '1998-12-01'
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;""".format(size)

start_time = time.time()
df = verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("   scramble            bypass             speed\n")
f.write("1  " + str(time_scramble) + " ")


query = """bypass select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        tpch{}g.lineitem
where
        l_shipdate <= date '1998-12-01'
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;""".format(size)


start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
