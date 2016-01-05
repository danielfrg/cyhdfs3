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

# with Timer('pywebhdfs read 500mb x {}'.format(n)):
#     for i in range(n):
#         name = "/tmp/pywebhdfs/500mb-{}".format(i)
#         content = webhdfs.read_file(name)
#         print(len(content), content == data)

######

with Timer('Total') as t:
    for i in range(n):
        name = '/tmp/cylibhdfs3/500mb-{}'.format(i)
        f = client.open(name, 'r')

        content = f.readfile()
        s = t.elapsed / 1000
        mb = len(content) / 2 ** 20
        print 'Bandwidth (Mb/s):', (mb / s)
        f.close()
