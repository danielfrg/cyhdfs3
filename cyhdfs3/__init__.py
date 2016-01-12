from __future__ import absolute_import

from cyhdfs3.cyhdfs3 import HDFSClient, File, FileInfo, BlockLocation

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
