cdef extern from "hdfs.h":
  ctypedef struct HdfsFileSystemInternalWrapper:
    pass
  ctypedef HdfsFileSystemInternalWrapper hdfsFS


  ctypedef struct HdfsFileInternalWrapper:
    pass
  ctypedef HdfsFileInternalWrapper hdfsFile

  ctypedef struct hdfsFileInfo:
      # tObjectKind mKind
      char *mName
      # tTime mLastMod
      # tOffset mSize
      short mReplication
      # tOffset mBlockSize
      char *mOwner
      char *mGroup
      short mPermissions
      # tTime mLastAccess

  struct hdfsBuilder:
    pass
  hdfsBuilder *hdfsNewBuilder()
  void hdfsFreeBuilder(hdfsBuilder *bld)

  char* hdfsGetLastError()

  void hdfsBuilderSetNameNode(hdfsBuilder *bld, char *nn)
  void hdfsBuilderSetNameNodePort(hdfsBuilder *bld, unsigned int port)

  hdfsFS hdfsBuilderConnect(hdfsBuilder *bld)

  hdfsFileInfo *hdfsListDirectory(hdfsFS fs, char *path, int *numEntries)

  hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize, short replication, int blocksize)
  int hdfsCloseFile(hdfsFS fs, hdfsFile file)

  int hdfsFileIsOpenForRead(hdfsFile file)
  int hdfsFileIsOpenForWrite(hdfsFile file)

  int hdfsRead(hdfsFS fs, hdfsFile file, void *buffer, int length)
  int hdfsWrite(hdfsFS fs, hdfsFile file, void *buffer, int length)
  int hdfsFlush(hdfsFS fs, hdfsFile file)
  int hdfsHFlush(hdfsFS fs, hdfsFile file)
  int hdfsSync(hdfsFS fs, hdfsFile file)
