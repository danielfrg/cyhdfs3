from __future__ import print_function, absolute_import

import posixpath

from utils import *


def test_block_locations(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    data = b'a' * 2 * 2 ** 20
    data += b'b' * 2 * 2 ** 19
    with hdfs.open(fname, 'w', block_size=1 * 2 ** 20) as f:
        f.write(data)

    blocks = hdfs.get_block_locations(fname)
    assert len(blocks) == 3
    assert blocks[0].length == 1 * 2 ** 20
    assert blocks[1].length == 1 * 2 ** 20
    assert blocks[2].length == 2 * 2 ** 19


def test_path_info_file(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    replication = 1
    block_size = 1 * 2 ** 20
    data = b'a' * 2 * 2 ** 20
    with hdfs.open(fname, 'w', block_size=block_size, replication=replication) as f:
        f.write(data)

    fileinfo = hdfs.path_info(fname)
    assert fileinfo.kind == 'f'
    assert fileinfo.name == fname
    assert fileinfo.size == len(data)
    assert fileinfo.block_size == block_size
    assert fileinfo.replication == replication
    assert fileinfo.owner == 'root'
    assert fileinfo.group == 'supergroup'


def test_path_info_dir(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    hdfs.create_dir(fname)

    n = 5
    data = b'a' * 2 * 2 ** 20
    for i in range(n):
        tfname = posixpath.join(fname, str(i))
        with hdfs.open(tfname, 'w') as f:
            f.write(data)

    fileinfo = hdfs.path_info(fname)
    assert fileinfo.kind == 'd'
    assert fileinfo.name == fname
    assert fileinfo.size == 0
    assert fileinfo.replication == 0
    assert fileinfo.replication == 0
    assert fileinfo.owner == 'root'
    assert fileinfo.group == 'supergroup'
