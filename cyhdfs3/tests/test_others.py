import inspect
import posixpath

from utils import *
import pickle


def test_is_pickeable(hdfs):
    hdfs2 = pickle.loads(pickle.dumps(hdfs))
