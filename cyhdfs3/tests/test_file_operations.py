import posixpath

from utils import *


def test_create_dir_remove_exists(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    assert hdfs.exists(fname) == False

    assert hdfs.create_dir(fname) == True
    assert hdfs.exists(fname) == True

    assert hdfs.delete(fname) == True
    assert hdfs.exists(fname) == False


def test_create_dir_list(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    files = hdfs.list_dir(TEST_DIR)
    filenames = [f.name for f in files]
    assert fname not in filenames

    assert hdfs.create_dir(fname)
    files = hdfs.list_dir(TEST_DIR)
    filenames = [f.name for f in files]
    assert fname in filenames


def test_rename(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)
    fname2 = posixpath.join(TEST_DIR, testname + '.renamed')

    assert hdfs.exists(fname) == False
    hdfs.create_dir(fname)
    assert hdfs.exists(fname)

    assert hdfs.rename(fname, fname2)
    assert hdfs.exists(fname) == False
    assert hdfs.exists(fname2)


def test_chmod(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    with hdfs.open(fname, 'w') as f:
        f.write(b'a')
        assert int(str('777'), 8) == 511
        assert f.info.permissions == int(str('777'), 8)

    new_mode = int(str('611'), 8)
    assert new_mode == 393
    hdfs.chmod(fname, new_mode)
    assert hdfs.path_info(fname).permissions == new_mode

    # Change using cmod_s
    new_mode = '444'
    hdfs.chmod_s(fname, new_mode)
    assert hdfs.path_info(fname).permissions == int(str(new_mode), 8)


@pytest.mark.skipif(True, reason="This test requires to be ran as the `hdfs` user")
def test_chown(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    with hdfs.open(fname, 'w') as f:
        f.write(b'a')
        assert f.info.owner == "root"
        assert f.info.group == "supergroup"

    rval = hdfs.chown(fname, 'ubuntu')
    assert rval == True

    assert hdfs.path_info(fname).owner == 'ubuntu'
