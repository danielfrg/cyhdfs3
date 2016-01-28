from __future__ import print_function, absolute_import

from utils import *

import posixpath

def test_is_pickeable(hdfs):
    import pickle
    hdfs2 = pickle.loads(pickle.dumps(hdfs))

    assert hdfs.host == hdfs2.host
    assert hdfs.port == hdfs2.port


def multiprocess_func(section):
    import os
    import cyhdfs3
    host = os.environ.get('HDFS_HOSTNAME', 'localhost')
    port = os.environ.get('HDFS_port', 8020)
    hdfs = cyhdfs3.HDFSClient(host=host, port=port)

    # Hardcoded
    data1 = b'0' * 1 * 2**20
    data2 = b'1' * 1 * 2**20
    data3 = b'2' * 1 * 2**20
    data4 = b'3' * 1 * 2**20
    data5 = b'4' * 1 * 2**20
    data_l = [data1, data2, data3, data4, data5]
    data = b''.join(data_l)
    fname = '/tmp/cyhdfs3-test/test_multiprocess'
    #

    with hdfs.open(fname, 'r') as f:
        f.seek(section * 2**20)
        read = f.read(2**20)
        assert len(read) == 2**20
        assert read == data_l[section]


def test_multiprocess(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    data = b'0' * 1 * 2**20
    data += b'1' * 1 * 2**20
    data += b'2' * 1 * 2**20
    data += b'3' * 1 * 2**20
    data += b'4' * 1 * 2**20

    with hdfs.open(fname, 'w', replication=1) as f:
        f.write(data)

    from multiprocessing import Pool
    p = Pool(3)
    p.map(multiprocess_func, [0, 1, 2, 3, 4])


def multiprocess_func_2(hdfs, section):
    # Hardcoded
    data1 = b'0' * 1 * 2**20
    data2 = b'1' * 1 * 2**20
    data3 = b'2' * 1 * 2**20
    data4 = b'3' * 1 * 2**20
    data5 = b'4' * 1 * 2**20
    data_l = [data1, data2, data3, data4, data5]
    data = b''.join(data_l)
    fname = '/tmp/cyhdfs3-test/test_stress_read'
    #

    with hdfs.open(fname, 'r') as f:
        f.seek(section * 2**20)
        read = f.read(2**20)
        assert len(read) == 2**20
        assert read == data_l[section]


def test_stress_read(hdfs, request):
    import multiprocessing
    from threading import Thread

    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    data = b'0' * 1 * 2**20
    data += b'1' * 1 * 2**20
    data += b'2' * 1 * 2**20
    data += b'3' * 1 * 2**20
    data += b'4' * 1 * 2**20

    with hdfs.open(fname, 'w') as f:
        f.write(data)

    for T in (Thread, multiprocessing.Process,):
        threads = [T(target=multiprocess_func_2, args=(hdfs, i)) for i in range(5)]
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
