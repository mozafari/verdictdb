import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
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
                        tpch10g.lineitem_scramble
                group by l_partkey) as q17_lineitem_tmp_cached
                Inner Join
                (select
                        l_quantity,
                        l_partkey,
                        l_extendedprice
                from
                        tpch10g.part,
                        tpch10g.lineitem_scramble
                where
                        p_partkey = l_partkey
                        and p_brand = 'Brand#23'
                        and p_container = 'MED BOX'
                ) as l1 on l1.l_partkey = t_partkey
        ) a
where quantity < t_avg_quantity;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_scramble = end_time - start_time

f = open(filename, 'a')
f.write("17 " + str(end_time - start_time) + " ")


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

start_time = time.time()
verdict.sql(query)
end_time = time.time()
time_bypass = end_time - start_time

f = open(filename, 'a')
f.write(str(time_bypass) + " ")

speed = time_bypass / time_scramble
f.write(str(speed) + "\n")
