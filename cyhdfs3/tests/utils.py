import os
import pytest

import pyximport; pyximport.install()
import cyhdfs3


TEST_DIR = '/tmp/cyhdfs3-test'


@pytest.yield_fixture(scope="module")
def hdfs():
    if 'LIBHDFS3_CONF' not in os.environ:
        os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

    host = os.environ.get('HDFS_HOSTNAME', 'localhost')
    port = os.environ.get('HDFS_port', 8020)
    hdfs = cyhdfs3.HDFSClient(host=host, port=port)

    if hdfs.exists(TEST_DIR):
        hdfs.delete(TEST_DIR, recursive=True)
    hdfs.create_dir(TEST_DIR)

    yield hdfs

    # if hdfs.exists(TEST_DIR):
    #     hdfs.delete(TEST_DIR, recursive=True)
