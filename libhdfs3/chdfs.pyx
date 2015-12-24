from libc.stdlib cimport malloc
from cpython cimport array
import array

cimport libhdfs3

O_RDONLY = libhdfs3.O_RDONLY
O_WRONLY = libhdfs3.O_WRONLY
O_APPEND = libhdfs3.O_APPEND


cdef class HDFSClient:
  cdef libhdfs3.hdfsBuilder* builder
  cdef libhdfs3.hdfsFS fs

  def __cinit__(self, host='localhost', port=8020):
    self.builder = libhdfs3.hdfsNewBuilder()

    libhdfs3.hdfsBuilderSetNameNode(self.builder, host)
    libhdfs3.hdfsBuilderSetNameNodePort(self.builder, port)

    self.fs = libhdfs3.hdfsBuilderConnect(self.builder)

  def __dealloc__(self):
    libhdfs3.hdfsDisconnect(self.fs)
    libhdfs3.hdfsFreeBuilder(self.builder)

  def getLastError(self):
    return libhdfs3.hdfsGetLastError()

  def exists(self, path):
    return libhdfs3.hdfsExists(self.fs, path) == 0

  def copy(self, src, dst):
    srcFS, dstFS = self.fs, self.fs
    return libhdfs3.hdfsCopy(srcFS, src, dstFS, dst) == 0

  def move(self, src, dst):
    srcFS, dstFS = self.fs, self.fs
    return libhdfs3.hdfsMove(srcFS, src, dstFS, dst) == 0

  def rename(self, src, dst):
    return libhdfs3.hdfsRename(self.fs, src, dst) == 0

  def delete(self, path, recursive=False):
    _recursive = 0 if recursive is False else 1
    return libhdfs3.hdfsDelete(self.fs, path, _recursive) == 0

  def createDirectory(self, path):
    return libhdfs3.hdfsCreateDirectory(self.fs, path) == 0

  def ls(self, path='/'):
    cdef int numEntries = 0
    cdef libhdfs3.hdfsFileInfo* files = libhdfs3.hdfsListDirectory(self.fs, path, &numEntries)

    ret = []
    for i in range(numEntries):
      fInfo = files[i]
      new = FileInfo(name=fInfo.mName, owner=fInfo.mOwner, group=fInfo.mGroup,
                      replication=fInfo.mReplication, permissions=fInfo.mPermissions,
                      size=fInfo.mSize, lastMod=fInfo.mLastMod, lastAccess=fInfo.mLastAccess,
                      blockSize=fInfo.mBlockSize, kind=fInfo.mKind)
      ret.append(new)

    libhdfs3.hdfsFreeFileInfo(files, numEntries)
    return ret

  def get_blocks(self, path, start=0, length=10):
    cdef int numOfBlocks = 0
    cdef libhdfs3.BlockLocation* blocks = libhdfs3.hdfsGetFileBlockLocations(self.fs, path, start, length, &numOfBlocks)

    ret = []
    for i in range(numOfBlocks):
      block = blocks[i]
      new = BlockLocation(length=block.length, offset=block.offset, corrupt=block.corrupt)

      for j in range(block.numOfNodes):
        new.append_host(hostname=block.hosts[j], datanode=block.names[j],
                        topology_path=block.topologyPaths[j])
      ret.append(new)

    libhdfs3.hdfsFreeFileBlockLocations(blocks, numOfBlocks)
    return ret

  def read(self, path, length=100):
    print "Path to read:", path
    O_RDONLY = 0
    flags = O_RDONLY
    cdef libhdfs3.hdfsFile fin = libhdfs3.hdfsOpenFile(self.fs, path, flags, 0, 0, 0)
    cdef isopen = libhdfs3.hdfsFileIsOpenForRead(fin)
    print "IsOpen:", isopen

    cdef void* buffer = malloc(length * sizeof(char))
    cdef int nbytes = libhdfs3.hdfsRead(self.fs, fin, buffer, length)
    print "Read:", nbytes, "bytes"

    if(nbytes < 0):
      print libhdfs3.hdfsGetLastError()
    else:
      print <char*> buffer

    libhdfs3.hdfsCloseFile(self.fs, fin)

  def write(self, path, char* content):
    print "Path to write:", path
    O_WRONLY = 1
    flags = O_WRONLY
    cdef libhdfs3.hdfsFile fout = libhdfs3.hdfsOpenFile(self.fs, path, flags, 0, 0, 0)
    cdef isopen = libhdfs3.hdfsFileIsOpenForWrite(fout)
    print "IsOpen:", isopen

    length = len(content)
    cdef int nbytes = libhdfs3.hdfsWrite(self.fs, fout, <void*> content, length)
    cdef int flushed = libhdfs3.hdfsFlush(self.fs, fout)
    print 'Flush:', flushed
    print "Wrote:", nbytes, "bytes"

    if(nbytes < 0):
      print libhdfs3.hdfsGetLastError()

    libhdfs3.hdfsCloseFile(self.fs, fout)

class FileInfo(object):

  def __init__(self, name, owner, group, replication,
               permissions, size, lastMod, lastAccess, blockSize, kind):
    self.name = name
    self.owner = owner
    self.group = group
    self.replication = replication
    self.permissions = permissions
    self.size = size
    self.lastMod = lastMod
    self.lastAccess = lastAccess
    self.blockSize = blockSize
    self.kind = 'f' if kind == 70 else 'd'

  def todict(self):
    dic = {}
    dic['name'] = self.name
    dic['owner'] = self.owner
    dic['group'] = self.group
    dic['replication'] = self.replication
    dic['permissions'] = self.permissions
    dic['size'] = self.size
    dic['lastMod'] = self.lastMod
    dic['lastAccess'] = self.lastAccess
    dic['blockSize'] = self.blockSize
    dic['kind'] = self.kind
    return dic

  def __str__(self):
    return str(self.todict())

class BlockLocation(object):

  def __init__(self, length, offset, corrupt):
    self.length = length
    self.offset = offset
    self.corrupt = corrupt
    self.hostanames = []
    self.names = []
    self.topology_paths = []

  def append_host(self, hostname, datanode, topology_path):
    self.hostanames.append(host)
    self.names.append(datanode)
    self.topology_paths.append(topology_path)

  def todict(self):
    dic = {}
    dic['length'] = self.length
    dic['offset'] = self.offset
    dic['corrupt'] = self.corrupt
    dic['hosts'] = self.hosts
    dic['names'] = self.names
    dic['topology_paths'] = self.topology_paths
    return dic

  def __str__(self):
    return str(self.todict())
