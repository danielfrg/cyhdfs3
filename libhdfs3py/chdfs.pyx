cimport libhdfs3
from libc.stdlib cimport malloc

cdef class HDFSClient:
  cdef libhdfs3.hdfsBuilder* builder
  cdef libhdfs3.hdfsFS fs

  def __cinit__(self, host='localhost', port=8020):
    self.builder = libhdfs3.hdfsNewBuilder()

    libhdfs3.hdfsBuilderSetNameNode(self.builder, host)
    libhdfs3.hdfsBuilderSetNameNodePort(self.builder, port)

    self.fs = libhdfs3.hdfsBuilderConnect(self.builder)

  def ls(self, path='/'):
    cdef int numEntries = 0
    cdef libhdfs3.hdfsFileInfo* files = libhdfs3.hdfsListDirectory(self.fs, path, &numEntries)

    print "NFiles:", numEntries
    for i in range(numEntries):
      print "File:", files[i].mName

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

  def __dealloc__(self):
    libhdfs3.hdfsFreeBuilder(self.builder)