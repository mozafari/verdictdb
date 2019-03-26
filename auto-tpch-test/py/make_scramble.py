import pyverdict
import time
import sys

size = sys.argv[1]
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
print('make scramble for lineitem and orders...')
verdict.sql('use tpch{}g;'.format(size))
df = verdict.sql('show tables;')
if ['lineitem_scramble'] not in df.values:
    verdict.sql('CREATE SCRAMBLE tpch{}g.lineitem_scramble FROM tpch{}g.lineitem'.format(size, size))
if ['orders_scramble'] not in df.values:
    verdict.sql('CREATE SCRAMBLE tpch{}g.orders_scramble FROM tpch{}g.orders'.format(size, size))
