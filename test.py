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
# print client.rename('/tmp/test2', '/tmp/test3')

# print client.createDirectory('/tmp/mydir')

# print client.delete('/tmp/test3')
# print client.delete('/tmp/mydir', recursive=True)

# l = client.ls("/tmp")
# print l[0]
# print l[1]

# msg = b"IF THIS WORKS I CAN SLEEP!"
# client.write("/tmp/test2", msg)
# client.read("/tmp/test2", len(msg))
bl = client.get_blocks('/tmp/test2')
print bl[0]

# Bigger file
# msg = b"A" * 1000000000
# client.write("/tmp/testbig", msg)
# client.get_blocks('/tmp/testbig', length=1000000000)

print client.getLastError()
