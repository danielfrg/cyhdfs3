import pyximport; pyximport.install()

import os
os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"
os.environ["LD_LIBRARY_PATH"] = "/usr/local/lib"

from libhdfs3 import chdfs

client = chdfs.HDFSClient()

client.ls("/tmp")

# msg = b"IF THIS WORKS I CAN SLEEP!"
# client.write("/tmp/test2", msg)
# client.read("/tmp/test2", len(msg))

# client.blocks('/tmp/test2')

# Bigger file
# msg = b"A" * 1000000000
# client.write("/tmp/testbig", msg)
# client.blocks('/tmp/testbig', length=1000000000)
