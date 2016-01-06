import pyximport; pyximport.install()

from libhdfs3 import chdfs
from pywebhdfs.webhdfs import PyWebHdfsClient

from timer import Timer

import os
os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

host = "localhost"
client = chdfs.HDFSClient(host=host, port=8020)
webhdfs = PyWebHdfsClient(host=host, port=50070, timeout=None, user_name='hdfs')

client.create_dir('/tmp/cylibhdfs3/')
client.create_dir('/tmp/ctypes/')
client.create_dir('/tmp/pywebhdfs/')

#####

n = 1
data = b'0' * 250 * 2 ** 20
data += b'1' * 250 * 2 ** 20

# with Timer('cylibhdfs3 write 500mb x {}'.format(n)):
#     for i in range(n):
#         name = "/tmp/cylibhdfs3/500mb-{}".format(i)
#         f = client.open(name, 'w', replication=1)
#         f.write(data)
#         f.close()

# with Timer('pywebhdfs write 500mb x {}'.format(n)):
#     for i in range(n):
#         name = "/tmp/pywebhdfs/500mb-{}".format(i)
#         webhdfs.create_file(name, data, overwrite=True)

#####

# with Timer('cylibhdfs3 readfile 500mb x {}'.format(n)) as t:
#     for i in range(n):
#         name = '/tmp/cylibhdfs3/500mb-{}'.format(i)
#         f = client.open(name, 'r')
#
#         content = f.readfile()
#         print(len(content), content == data)
#         f.close()

# with Timer('cylibhdfs3 readfile 500mb x {}'.format(n)) as t:
#     for i in range(n):
#         name = '/tmp/cylibhdfs3/500mb-{}'.format(i)
#         f = client.open(name, 'r')
#
#         content = f.read(len(data))
#         print(len(content), content == data)
#         f.close()

# with Timer('pywebhdfs read 500mb x {}'.format(n)):
#     for i in range(n):
#         name = "/tmp/pywebhdfs/500mb-{}".format(i)
#         content = webhdfs.read_file(name)
#         print(len(content), content == data)

######

# with Timer('cylibhdfs3 iris readline') as t:
#     name = '/tmp/iris.csv'
#     f = client.open(name, 'r')
#
#     print f.readline()
#     print f.readline()
#     print f.readline()
#     print f.readline()
#     print f.readline()
#     f.close()

######

# with Timer(summary=False) as t:
#     for i in range(n):
#         name = '/tmp/cylibhdfs3/500mb-{}'.format(i)
#         f = client.open(name, 'r')
#
#         content = f.readfile()
#         s = t.elapsed / 1000
#         mb = len(content) / 2 ** 20
#         print 'Bandwidth (Mb/s):', (mb / s)
#         f.close()

with Timer(summary=False) as t:
    f = client.open('/tmp/cylibhdfs3/500mb-0', 'r')
    blocks = f.blocks

    for block in f.blocks:
        f.seek(block.offset)
        block_content = f.read(length=block.length)
        assert block.length == len(block_content)

        s = t.elapsed / 1000
        mb = len(block_content) / 2 ** 20
        bw = (mb / s)
        print 'Block %s (Mb/s):' % block, bw
        t.restart()
    f.close()
