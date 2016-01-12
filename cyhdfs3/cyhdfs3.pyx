from __future__ import unicode_literals

from libc cimport stdlib
from cpython cimport bool
from cpython.version cimport PY_MAJOR_VERSION

cimport cyavro._cyavro as cyavro
cimport libhdfs3

# Constants
O_RDONLY = libhdfs3.O_RDONLY
O_WRONLY = libhdfs3.O_WRONLY
O_APPEND = libhdfs3.O_APPEND


cdef unicode ustring(s):
    if type(s) is unicode:
        # fast path for most common case(s)
        return <unicode>s
    elif PY_MAJOR_VERSION < 3 and isinstance(s, bytes):
        # only accept byte strings in Python 2.x, not in Py3
        return (<bytes>s).decode('UTF-8')
    elif isinstance(s, unicode):
        # an evil cast to <unicode> might work here in some(!) cases,
        # depending on what the further processing does.  to be safe,
        # we can always create a copy instead
        return unicode(s)
    else:
        raise TypeError("Not a str")


cdef str_to_charp(py_str):
    py_byte_string = ustring(py_str).encode('UTF-8')
    cdef char *c_string = py_byte_string
    return c_string


cdef class HDFSClient:
    cdef public host
    cdef public int port
    cdef libhdfs3.hdfsBuilder* builder
    cdef libhdfs3.hdfsFS fs

    def __init__(self, host='localhost', int port=8020):
        self.host = host
        self.port = port

        self.builder = libhdfs3.hdfsNewBuilder()
        _ = str_to_charp(self.host)
        cdef char *c_host = _
        libhdfs3.hdfsBuilderSetNameNode(self.builder, c_host)
        libhdfs3.hdfsBuilderSetNameNodePort(self.builder, self.port)

        self.fs = libhdfs3.hdfsBuilderConnect(self.builder)

    def __dealloc__(self):
        libhdfs3.hdfsDisconnect(self.fs)
        libhdfs3.hdfsFreeBuilder(self.builder)

    def __reduce__(self):
        return (rebuild_client, (self.host, self.port))

    def get_last_error(self):
        cdef char* c_string = libhdfs3.hdfsGetLastError()
        return c_string.decode('utf-8')

    def exists(self, path):
        _ = str_to_charp(path)
        cdef char *c_path = _
        return libhdfs3.hdfsExists(self.fs, c_path) == 0

    # def _copy(self, src, dst):
    #     srcFS, dstFS = self.fs, self.fs
    #     return libhdfs3.hdfsCopy(srcFS, src, dstFS, dst) == 0
    #
    # def _move(self, src, dst):
    #     srcFS, dstFS = self.fs, self.fs
    #     return libhdfs3.hdfsMove(srcFS, src, dstFS, dst) == 0

    def rename(self, src, dst):
        _ = str_to_charp(src)
        cdef char *c_src = _
        _ = str_to_charp(dst)
        cdef char *c_dst = _
        return libhdfs3.hdfsRename(self.fs, c_src, c_dst) == 0

    def delete(self, path, bool recursive=False):
        _ = str_to_charp(path)
        cdef char *c_path = _
        cdef int c_recursive = 0 if recursive is False else 1
        return libhdfs3.hdfsDelete(self.fs, c_path, c_recursive) == 0

    def create_dir(self, path):
        _ = str_to_charp(path)
        cdef char *c_path = _
        return libhdfs3.hdfsCreateDirectory(self.fs, c_path) == 0

    def list_dir(self, path='/', bool recurse=False, int max_depth=5):
        ret = []
        depth = 1 if recurse is False else max_depth
        self.list_dir_recursive(path, ret=ret, depth=depth)
        return ret

    def list_dir_recursive(self, path='/', list ret=None, int depth=1):
        _ = str_to_charp(path)
        cdef char *c_path = _
        cdef int num_entries = 0
        cdef libhdfs3.hdfsFileInfo* files = libhdfs3.hdfsListDirectory(self.fs, c_path, &num_entries)

        py_name = ""
        py_owner = ""
        py_group = ""
        py_kind = ""
        for i in range(num_entries):
            fInfo = files[i]
            py_name = fInfo.mName.decode('utf-8')
            py_owner = fInfo.mOwner.decode('utf-8')
            py_group = fInfo.mGroup.decode('utf-8')
            py_kind = 'f' if fInfo.mKind == 70 else 'd'
            new = FileInfo(name=py_name, owner=py_owner, group=py_group,
                            replication=fInfo.mReplication, permissions=fInfo.mPermissions,
                            size=fInfo.mSize, lastMod=fInfo.mLastMod, lastAccess=fInfo.mLastAccess,
                            block_size=fInfo.mBlockSize, kind=py_kind)
            ret.append(new)

            if new.kind == b'd' and depth > 1:
                path = new.name.decode()
                self.list_dir_recursive(path=path, ret=ret, depth=(depth - 1))

        libhdfs3.hdfsFreeFileInfo(files, num_entries)
        return ret

    def chown(self, path, owner=None, group=None):
        py_byte_string = path.encode('UTF-8')
        cdef char *c_path = py_byte_string
        cdef char *c_owner = NULL
        cdef char *c_group = NULL
        if owner != None:
            py_byte_string = owner.encode('UTF-8')
            c_owner = py_byte_string
        if group != None:
            py_byte_string = group.encode('UTF-8')
            c_group = py_byte_string
        rval = libhdfs3.hdfsChown(self.fs, c_path, c_owner, c_group)
        return rval == 0

    def chmod_s(self, path, mode):
        py_byte_string = mode.encode('UTF-8')
        cdef short c_mode = int(py_byte_string, 8)
        return self.chmod(path, c_mode)

    def chmod(self, path, short mode):
        py_byte_string = path.encode('UTF-8')
        cdef char *c_path = py_byte_string
        cdef short c_mode = mode
        rval = libhdfs3.hdfsChmod(self.fs, c_path, c_mode)
        return rval == 0

    def get_block_locations(self, path, int start=0, int length=0):
        _ = str_to_charp(path)
        cdef char *c_path = _
        cdef int numOfBlocks = 0
        cdef int c_length = self.path_info(path).size if length == 0 else length
        cdef libhdfs3.BlockLocation* blocks = libhdfs3.hdfsGetFileBlockLocations(self.fs, c_path, start, c_length, &numOfBlocks)

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

    def open(self, path, mode='r', *args, **kwargs):
        return File(self, path, mode, *args, **kwargs)

    def path_info(self, path):
        _ = str_to_charp(path)
        cdef char *c_path = _
        cdef libhdfs3.hdfsFileInfo* fInfo = libhdfs3.hdfsGetPathInfo(self.fs, c_path)

        py_name = fInfo.mName.decode('utf-8')
        py_owner = fInfo.mOwner.decode('utf-8')
        py_group = fInfo.mGroup.decode('utf-8')
        py_kind = 'f' if fInfo.mKind == 70 else 'd'
        f = FileInfo(name=py_name, owner=py_owner, group=py_group,
                        replication=fInfo.mReplication, permissions=fInfo.mPermissions,
                        size=fInfo.mSize, lastMod=fInfo.mLastMod, lastAccess=fInfo.mLastAccess,
                        block_size=fInfo.mBlockSize, kind=py_kind)
        libhdfs3.hdfsFreeFileInfo(fInfo, 1)
        return f


def rebuild_client(host, port):
    c = HDFSClient(host, port)
    return c


cdef class File:
    cdef HDFSClient client
    cdef libhdfs3.hdfsFile _file
    cdef public path
    cdef public mode
    cdef public short replication
    cdef public FileInfo _info
    cdef public list _blocks
    cdef bytes linebuff

    def __cinit__(self, client, path, mode, buffer_size=0, replication=0, block_size=0):
        self.client = client
        self.path = path
        self.mode = mode

        _ = str_to_charp(self.path)
        cdef char *c_path = _
        flags = O_RDONLY
        flags = O_WRONLY if mode == 'w' else flags
        flags = O_WRONLY | O_APPEND if mode == 'a' else flags
        self.replication = 1 if mode == 'a' else replication  # Trust in the force Luke
        self._file = libhdfs3.hdfsOpenFile(self.client.fs, c_path, flags, buffer_size, self.replication, block_size)

        is_ok = <int> self._file
        if is_ok == 0:
            raise IOError("File open failed: " + self.client.get_last_error())

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
            raise IOError('Seek Failed:', self.client.get_last_error())
        return True

    def tell(self):
        out = libhdfs3.hdfsTell(self.client.fs, self._file)
        if out == -1:
            raise IOError('Tell Failed:', self.client.get_last_error())
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
        "A `BlockLocation` reference"
        def __get__(self):
            if getattr(self, '_info', None) is None:
                self._blocks = self.client.get_block_locations(self.path)
            return self._blocks


cdef class FileInfo(object):
    cdef public name
    cdef public owner
    cdef public group
    cdef public short replication
    cdef public short permissions
    cdef public libhdfs3.tOffset size
    cdef public libhdfs3.tTime lastMod
    cdef public libhdfs3.tTime lastAccess
    cdef public libhdfs3.tOffset block_size
    cdef public kind

    def __init__(self, name, owner, group, short replication,
                 short permissions, size, lastMod, lastAccess, block_size, kind):
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
