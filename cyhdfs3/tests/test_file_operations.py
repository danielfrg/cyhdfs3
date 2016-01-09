import inspect
import posixpath

from utils import *


def test_create_dir_remove_exists(hdfs):
    testname = inspect.stack()[0][3]
    fname = posixpath.join(TEST_DIR, testname)

    assert hdfs.exists(fname) == False

    hdfs.create_dir(fname)
    assert hdfs.exists(fname)

    hdfs.delete(fname)
    assert hdfs.exists(fname) == False


def test_create_dir_list(hdfs):
    testname = inspect.stack()[0][3]
    fname = posixpath.join(TEST_DIR, testname)

    files = hdfs.list_dir(TEST_DIR)
    filenames = [f.name for f in files]
    assert fname not in filenames

    hdfs.create_dir(fname)
    files = hdfs.list_dir(TEST_DIR)
    filenames = [f.name for f in files]
    assert fname in filenames


def test_rename(hdfs):
    testname = inspect.stack()[0][3]
    fname = posixpath.join(TEST_DIR, testname)
    fname2 = posixpath.join(TEST_DIR, testname + '.renamed')

    assert hdfs.exists(fname) == False
    hdfs.create_dir(fname)
    assert hdfs.exists(fname)

    assert hdfs.rename(fname, fname2)
    assert hdfs.exists(fname) == False
    assert hdfs.exists(fname2)
