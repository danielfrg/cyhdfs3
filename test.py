import pyximport; pyximport.install()

import os
os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

from libhdfs3 import chdfs

print('-----------------')

client = chdfs.HDFSClient()

# print(client.list_dir('/', recurse=True))

# print client.exists('/tmp')

# print client.exists('/tmp/mydir')
# print client.create_dir('/tmp/mydir')
# print client.exists('/tmp/mydir')
# print client.delete('/tmp/mydir', recursive=True)
# print client.exists('/tmp/mydir')

# l = client.ls("/tmp")
# print l[0]
# print l[1]

# FILE OPERATIONS

# print(chdfs.O_RDONLY)
# print(chdfs.O_WRONLY)
# print(chdfs.O_APPEND)
# print(chdfs.O_WRONLY | chdfs.O_APPEND)

# f = client.open('/tmp/test', 'w')
# msg = b"a" * 100
# f.write(msg)
# f.close()

# f = client.open('/tmp/test', 'r')
# print f.read()
# f.close()


# f = client.open('/tmp/test', 'a')
# msg = b"z" * 100
# f.write(msg)
# f.close()

f = client.open('/tmp/test', 'r')
print f.tell()
print f.seek(800)
print f.tell()
print f.read(100)
f.close()

# print client.getLastError()
