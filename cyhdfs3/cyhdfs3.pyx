from libc cimport stdlib
from cpython cimport array
import array

cimport libhdfs3

cimport cyavro._cyavro as cyavro

O_RDONLY = libhdfs3.O_RDONLY
O_WRONLY = libhdfs3.O_WRONLY
O_APPEND = libhdfs3.O_APPEND


cdef class HDFSClient:
    cdef public char* host
    cdef public int port
    cdef libhdfs3.hdfsBuilder* builder
    cdef libhdfs3.hdfsFS fs

    def __cinit__(self, host='localhost', port=8020):
        self.host = host
        self.port = port

        self.builder = libhdfs3.hdfsNewBuilder()
        libhdfs3.hdfsBuilderSetNameNode(self.builder, self.host)
        libhdfs3.hdfsBuilderSetNameNodePort(self.builder, self.port)

        self.fs = libhdfs3.hdfsBuilderConnect(self.builder)

    def __dealloc__(self):
        libhdfs3.hdfsDisconnect(self.fs)
        libhdfs3.hdfsFreeBuilder(self.builder)

    def __reduce__(self):
        return (rebuild_client, (self.host, self.port))

    def getLastError(self):
        return libhdfs3.hdfsGetLastError()

    def exists(self, path):
        return libhdfs3.hdfsExists(self.fs, path) == 0

    def _copy(self, src, dst):
        srcFS, dstFS = self.fs, self.fs
        return libhdfs3.hdfsCopy(srcFS, src, dstFS, dst) == 0

    def _move(self, src, dst):
        srcFS, dstFS = self.fs, self.fs
        return libhdfs3.hdfsMove(srcFS, src, dstFS, dst) == 0

    def rename(self, src, dst):
        return libhdfs3.hdfsRename(self.fs, src, dst) == 0

    def delete(self, path, recursive=False):
        cdef int _recursive = 0 if recursive is False else 1
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
                                            block_size=fInfo.mBlockSize, kind=fInfo.mKind)
            ret.append(new)

            if new.kind == b'd' and depth > 1:
                path = new.name
                self.list_dir_recursive(path=path, ret=ret, depth=(depth - 1))

        libhdfs3.hdfsFreeFileInfo(files, numEntries)
        return ret

    def get_block_locations(self, path, start=0, length=None):
        cdef int numOfBlocks = 0
        length = self.path_info(path).size if length is None  else length
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
                     block_size=fInfo.mBlockSize, kind=fInfo.mKind)
        libhdfs3.hdfsFreeFileInfo(fInfo, 1)
        return f


def rebuild_client(host, port):
    c = HDFSClient(host, port)
    return c


cdef class File:
    cdef HDFSClient client
    cdef libhdfs3.hdfsFile _file
    cdef public char* path
    cdef public char* mode
    cdef public char* encoding
    cdef public short replication
    cdef public FileInfo _info
    cdef public list _blocks
    cdef bytes linebuff

    def __cinit__(self, client, path, mode, buffer_size=0, replication=0, block_size=0,
                  encoding='utf-8'):
        self.client = client
        self.path = path
        self.mode = mode
        self.encoding = encoding

        flags = O_RDONLY
        flags = O_WRONLY if mode == 'w' else flags
        flags = O_WRONLY | O_APPEND if mode == 'a' else flags
        self.replication = 1 if mode == 'a' else replication  # Trust in the force Luke
        self._file = libhdfs3.hdfsOpenFile(self.client.fs, self.path, flags, buffer_size, self.replication, block_size)

        is_ok = <int> self._file
        if is_ok == 0:
            raise IOError("File open failed: " + self.client.getLastError())

        self.linebuff = b""
        self._info = None
        self._blocks = []

    def close(self):
        self.flush()
        libhdfs3.hdfsCloseFile(self.client.fs, self._file)

    def write(self, bytes content):
        cdef char* c_char =  content

        cdef isopen = libhdfs3.hdfsFileIsOpenForWrite(self._file)
        if isopen != 1:
            raise IOError("File '{}' not open for write".format(self.path))

        length = len(content)
        cdef int nbytes = libhdfs3.hdfsWrite(self.client.fs, self._file, <void*> c_char, length)
        if nbytes < 0:
            raise IOError("Could not write contents to file:", libhdfs3.hdfsGetLastError())
        return nbytes

    def read(self, length=None, buffersize=1*2**20):
        cdef isopen = libhdfs3.hdfsFileIsOpenForRead(self._file)
        if isopen != 1:
            raise IOError("File '{}' not open for read".format(self.path))

        length = self.info.size if length is None else length
        cdef void* buffer = stdlib.malloc(length * sizeof(char))

        tempbuffer_length = min(length, buffersize)
        cdef void* tempbuffer = stdlib.malloc(tempbuffer_length * sizeof(char))

        self._read(buffer, length, tempbuffer, tempbuffer_length)

        cdef bytes py_string
        cdef char* c_string = <char*> buffer
        try:
            py_bytes_string = c_string[:length]
        finally:
            stdlib.free(buffer)
        return py_bytes_string

    cdef _read(self, void* buffer, int buffer_length, void* tempbuffer, int tempbuffer_length):
        """
        Reads to a buffer.
        Caller must: free(buffer)
        """
        pos = 0
        cdef int nbytesread = 0
        remaining = buffer_length
        while remaining > 0:
            readbuffer_length = min(tempbuffer_length, remaining)
            nbytesread = libhdfs3.hdfsRead(self.client.fs, self._file, tempbuffer, readbuffer_length)
            if nbytesread < 0:
                raise IOError("Could not read file:", libhdfs3.hdfsGetLastError())
            elif nbytesread == 0:
                break  # EOF
            buffer[pos:pos + nbytesread] = tempbuffer
            pos = pos + nbytesread
            remaining = remaining - nbytesread

    def readline(self, step=1*2**19, buffersize=1*2**19):
        index = self.linebuff.find("\n")
        if index >= 0:
            line = self.linebuff[:index]
            linebuff = self.linebuff[index + 1:]
            self.linebuff = linebuff
            return line

        while self.tell() < self.info.size:
            lastbytesread = self.read(length=step, buffersize=buffersize)
            linebuff = self.linebuff + lastbytesread
            self.linebuff = linebuff
            return self.readline(step=step, buffersize=buffersize)

        return self.linebuff

    def read_avro(self, length=None, buffersize=1*2**20):
        length = self.info.size if length is None else length
        cdef void* buffer = stdlib.malloc(length * sizeof(char))

        tempbuffer_length = min(length, buffersize)
        cdef void* tempbuffer = stdlib.malloc(tempbuffer_length * sizeof(char))

        self._read(buffer, length, tempbuffer, tempbuffer_length)
        cdef cyavro.AvroReader reader = cyavro.reader_from_bytes_c(buffer, length)
        return reader

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
            if getattr(self, '_info', None) is None:
                self._info = self.client.path_info(self.path)
            return self._info

    property blocks:
        "A `FileInfo` reference"
        def __get__(self):
            if getattr(self, '_info', None) is None:
                self._blocks = self.client.get_block_locations(self.path)
            return self._blocks


cdef class FileInfo(object):
    cdef public str name
    cdef public str owner
    cdef public str group
    cdef public short replication
    cdef public short permissions
    cdef public libhdfs3.tOffset size
    cdef public libhdfs3.tTime lastMod
    cdef public libhdfs3.tTime lastAccess
    cdef public libhdfs3.tOffset block_size
    cdef public str kind

    def __init__(self, name, owner, group, replication,
                 permissions, size, lastMod, lastAccess, block_size, kind):
        name = '/' + name.lstrip('/')
        self.name = name
        self.owner = owner
        self.group = group
        self.replication = replication
        self.permissions = permissions
        self.size = size
        self.lastMod = lastMod
        self.lastAccess = lastAccess
        self.block_size = block_size
        kind = 'f' if kind == 70 else 'd'
        self.kind = kind

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
        dic['block_size'] = self.block_size
        dic['kind'] = self.kind
        return dic

    def __str__(self):
        return str(self.todict())


cdef class BlockLocation(object):
    cdef public int corrupt
    cdef public int numOfNodes
    cdef public list hostnames
    cdef public list names
    cdef public list topology_paths
    cdef public libhdfs3.tOffset length
    cdef public libhdfs3.tOffset offset

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
