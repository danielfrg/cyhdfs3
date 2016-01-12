import posixpath

import pickle
import numpy as np
import numpy.testing as npt

from utils import *


def test_bytes(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    data = b'a' * 10 * 2**20
    data += b'b' * 10 * 2**20
    data += b'c' * 10 * 2**20

    with hdfs.open(fname, 'w', replication=1) as f:
        f.write(data)

    with hdfs.open(fname, 'r') as f:
        read = f.read(len(data))
        assert len(data) == len(read)
        assert read == data


def test_pickle(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    arr = np.random.normal(10, 2, size=(100, 100))
    data = pickle.dumps(arr)

    with hdfs.open(fname, 'w') as f:
        f.write(data)

    with hdfs.open(fname, 'r') as f:
        read = f.read(len(data))
        assert len(data) == len(read)
        assert data == read
        read = pickle.loads(read)
        npt.assert_equal(arr, read)


def test_read_nonexistent(hdfs, request):
    with pytest.raises(IOError):
        f = hdfs.open('/tmp/NOFILE', 'r')


def test_open_for_write_read(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    f = hdfs.open(fname, 'w')
    with pytest.raises(IOError):
        f.read()
    f.close()


def test_open_for_read_write(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    data = b'a' * 10 * 2**20
    with hdfs.open(fname, 'w') as f:
        f.write(data)


    f = hdfs.open(fname, 'r')
    with pytest.raises(IOError):
        f.write(data)
    f.close()
