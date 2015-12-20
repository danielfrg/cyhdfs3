import pyximport; pyximport.install()

from chdfs import *

import os
os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

client = HDFSClient()

client.ls("/tmp")

msg = b"IF THIS WORKS I CAN SLEEP!"
client.write("/tmp/test2", msg)
client.read("/tmp/test2", len(msg))
