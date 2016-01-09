import inspect
import posixpath

from utils import *
import numpy as np
import numpy.testing as npt
import pickle


def test_io_bytes(hdfs):
    testname = inspect.stack()[0][3]
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


def test_io_pickle(hdfs):
    testname = inspect.stack()[0][3]
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
