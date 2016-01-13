from __future__ import print_function, absolute_import

# import pyximport; pyximport.install()
from cyhdfs3.cyhdfs3 import HDFSClient, File, FileInfo, BlockLocation

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
