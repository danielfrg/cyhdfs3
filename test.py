import pyximport; pyximport.install()

import os
os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

from libhdfs3 import chdfs

client = chdfs.HDFSClient()

# print(chdfs.O_RDONLY)
# print(chdfs.O_WRONLY)
# print(chdfs.O_APPEND)
# print(chdfs.O_WRONLY | chdfs.O_APPEND)

# print client.exists('/tmp')
# print client.exists('/fake')

# print client.rename('/tmp/hadoop-yarn', '/tmp/hhh')
# print client.rename('/tmp/test', '/tmp/test2')

# print client.createDirectory('/tmp/mydir')

# print client.delete('/tmp/test2')
# print client.delete('/tmp/mydir', recursive=True)

# l = client.ls("/tmp")
# print l[0]
# print l[1]

# FILE OPERATIONS

# f = client.open('/tmp/test', 'w')
# msg = b"a" * 100
# f.write(msg)
# f.close()

# f = client.open('/tmp/test', 'r')
# print f.read()
# f.close()

# f = client.open('/tmp/test', 'r')
# print f.tell()
# print f.seek(10)
# print f.tell()
# f.close()

# f = client.open('/tmp/test2', 'w')
# msg = b"a" * 10
# f.write(msg)
# msg = b"b" * 10
# f.write(msg)
# msg = b"c" * 10
# f.write(msg)
# f.close()

# f = client.open('/tmp/test2', 'r')
# print f.read(10)
# print f.read(10)
# print f.read(10)
# f.close()

# f = client.open('/tmp/test2', 'r')
# print f.seek(10)
# print f.tell()
# print f.read(10)
# f.close()

# f = client.open('/tmp/test2', 'r')
# print f.seek(20)
# print f.tell()
# print f.read(10)
# f.close()

# f = client.open('/tmp/test', 'a')
# print client.getLastError()
# msg = b"b" * 100
# f.write(msg)
# msg = b"c" * 100
# f.write(msg)
# f.close()

print client.getLastError()
