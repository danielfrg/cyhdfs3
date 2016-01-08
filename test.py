import pyximport; pyximport.install()

import os
os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

import cyhdfs3

print('-----------------')

client = cyhdfs3.HDFSClient()

# print client.host

# print client.get_capacity()
# print client.get_used()

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

# f = client.open('/tmp/test', 'r')
# print f.tell()
# print f.seek(800)
# print f.tell()
# print f.read(100)
# f.close()

# with client.open('/tmp/test', 'r') as f:
#     print f.tell()
#     print f.seek(800)
#     print f.tell()
#     print f.read(100)

# print client.path_info('/tmp/iris.csv')

# with client.open('/tmp/cyhdfs3/500mb-0', 'r') as f:
with client.open('/tmp/quickstop.db', 'r') as f:
    reader = f.read_avro()
    print reader
    reader.init_buffers()
    print 2
    print reader.read_chunk()


# with client.open('/tmp/iris.csv', 'r') as f:
    # print f.info
    # print f.blocks
    # print f.info.size

    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)
    # print f.read_bytes(buffersize=2**4)

    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)
    # print f.readline(buffersize=2**4)

    # print f.read()

# print client.getLastError()
