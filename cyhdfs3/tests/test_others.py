from utils import *
import pickle


def test_is_pickeable(hdfs):
    hdfs2 = pickle.loads(pickle.dumps(hdfs))

    assert hdfs.host == hdfs2.host
    assert hdfs.port == hdfs2.port
