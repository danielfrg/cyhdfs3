from libc.stdlib cimport malloc, free
from cpython cimport array
import array

cimport libhdfs3

O_RDONLY = libhdfs3.O_RDONLY
O_WRONLY = libhdfs3.O_WRONLY
O_APPEND = libhdfs3.O_APPEND


cdef class HDFSClient:
    cdef libhdfs3.hdfsBuilder* builder
    cdef libhdfs3.hdfsFS fs
    cdef public char* host
    cdef public int port

    def __cinit__(self, host='localhost', port=8020):
        self.host = host
        self.port = port

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

    def create_dir(self, path):
        return libhdfs3.hdfsCreateDirectory(self.fs, path) == 0

    def list_dir(self, path='/', recurse=False, max_depth=5):
        ret = []
        depth = 1 if recurse is False else max_depth
        self.list_dir_recursive(path, ret=ret, depth=depth)
        return ret

    def list_dir_recursive(self, path='/', ret=None, depth=1):
        cdef int numEntries = 0
        cdef libhdfs3.hdfsFileInfo* files = libhdfs3.hdfsListDirectory(self.fs, path, &numEntries)

        for i in range(numEntries):
            fInfo = files[i]
            new = FileInfo(name=fInfo.mName, owner=fInfo.mOwner, group=fInfo.mGroup,
                                            replication=fInfo.mReplication, permissions=fInfo.mPermissions,
                                            size=fInfo.mSize, lastMod=fInfo.mLastMod, lastAccess=fInfo.mLastAccess,
                                            blockSize=fInfo.mBlockSize, kind=fInfo.mKind)
            ret.append(new)

            if new.kind == 'd' and depth > 1:
                path = new.name
                self.list_dir_recursive(path=path, ret=ret, depth=(depth - 1))

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

    def get_default_block_size(self):
        return libhdfs3.hdfsGetDefaultBlockSize(self.fs)

    def get_capacity(self):
        return libhdfs3.hdfsGetCapacity(self.fs)

    def get_used(self):
        return libhdfs3.hdfsGetUsed(self.fs)

    def open(self, path, mode='r',  *args, **kwargs):
        return File(self, path, mode, *args, **kwargs)

    def path_info(self, path):
        cdef libhdfs3.hdfsFileInfo* fInfo = libhdfs3.hdfsGetPathInfo(self.fs, path)
        f = FileInfo(name=fInfo.mName, owner=fInfo.mOwner, group=fInfo.mGroup,
                     replication=fInfo.mReplication, permissions=fInfo.mPermissions,
                     size=fInfo.mSize, lastMod=fInfo.mLastMod, lastAccess=fInfo.mLastAccess,
                     blockSize=fInfo.mBlockSize, kind=fInfo.mKind)
        libhdfs3.hdfsFreeFileInfo(fInfo, 1)
        return f


cdef class File:
    cdef HDFSClient client
    cdef char* path
    cdef char* mode
    cdef libhdfs3.hdfsFile _file

    def __cinit__(self, client, path, mode, buffer_size=0, replication=0, block_size=0):
        self.client = client
        self.path = path
        self.mode = mode
        flags = O_RDONLY
        flags = O_WRONLY if mode == 'w' else flags
        flags = O_WRONLY | O_APPEND if mode == 'a' else flags
        replication = 1 if mode == 'a' else replication  # Trust in the force Luke
        self._file = libhdfs3.hdfsOpenFile(self.client.fs, self.path, flags, buffer_size, replication, block_size)

        is_ok = <int> self._file
        if is_ok == 0:
            raise IOError("File open failed: " + self.client.getLastError())

    def close(self):
        self.flush()
        libhdfs3.hdfsCloseFile(self.client.fs, self._file)

    def write(self, char* content):
        cdef isopen = libhdfs3.hdfsFileIsOpenForWrite(self._file)
        if isopen != 1:
            raise IOError("File not open for write:", self.client.getLastError())

        length = len(content)
        cdef int nbytes = libhdfs3.hdfsWrite(self.client.fs, self._file, <void*> content, length)
        if nbytes < 0:
            raise IOError("Could not write contents to file:", libhdfs3.hdfsGetLastError())

    def read(self, length=2**16):
        cdef isopen = libhdfs3.hdfsFileIsOpenForRead(self._file)
        if isopen != 1:
            raise IOError("File not open for read:", self.client.getLastError())

        cdef void* buffer = malloc(length * sizeof(char))
        cdef int nbytes = libhdfs3.hdfsRead(self.client.fs, self._file, buffer, length)
        if nbytes < 0:
            raise IOError("Could not read file:", libhdfs3.hdfsGetLastError())

        ret = <char*> buffer
        free(buffer)
        return ret[:nbytes]

    def flush(self):
        cdef int flushed = 0
        if self.mode == b'w':
            flushed = libhdfs3.hdfsFlush(self.client.fs, self._file)
            if flushed != 0:
                raise IOError("Could not flush file:", libhdfs3.hdfsGetLastError())
        return True

    def seek(self, pos):
        out = libhdfs3.hdfsSeek(self.client.fs, self._file, pos)
        if out != 0:
            raise IOError('Seek Failed:', self.client.getLastError())
        return True

    def tell(self):
        out = libhdfs3.hdfsTell(self.client.fs, self._file)
        if out == -1:
            raise IOError('Tell Failed:', self.client.getLastError())
        return out

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    property info:
        "A `FileInfo` reference"
        def __get__(self):
            return self.client.path_info(self.path)

    property blocks:
        "A `FileInfo` reference"
        def __get__(self):
            return self.client.get_blocks(self.path)


class FileInfo(object):

    def __init__(self, name, owner, group, replication,
                 permissions, size, lastMod, lastAccess, blockSize, kind):
        self.name = '/' + name.lstrip('/')
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
        self.hostnames = []
        self.names = []
        self.topology_paths = []

    def append_host(self, hostname, datanode, topology_path):
        self.hostnames.append(hostname)
        self.names.append(datanode)
        self.topology_paths.append(topology_path)

    def todict(self):
        dic = {}
        dic['length'] = self.length
        dic['offset'] = self.offset
        dic['corrupt'] = self.corrupt
        dic['hosts'] = self.hostnames
        dic['names'] = self.names
        dic['topology_paths'] = self.topology_paths
        return dic

    def __str__(self):
        return str(self.todict())
