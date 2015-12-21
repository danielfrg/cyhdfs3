cdef extern from "hdfs.h":
  ctypedef int tSize
  ctypedef int tTime
  ctypedef int tOffset
  ctypedef unsigned int tPort

  ctypedef struct HdfsFileSystemInternalWrapper:
    pass
  ctypedef HdfsFileSystemInternalWrapper hdfsFS

  ctypedef struct HdfsFileInternalWrapper:
    pass
  ctypedef HdfsFileInternalWrapper hdfsFile

  ctypedef struct BlockLocation:
    int numOfNodes
    char **hosts
    char **names
    char **topologyPaths
    tOffset length
    tOffset offset
    int corrupt

  ctypedef struct hdfsFileInfo:
      tObjectKind mKind
      char *mName
      tOffset mSize
      short mReplication
      tOffset mBlockSize
      char *mOwner
      char *mGroup
      short mPermissions
      tTime mLastMod
      tTime mLastAccess

  cdef enum tObjectKind:
    kObjectKindFile = 0
    kObjectKindDirectory = 1

  struct hdfsBuilder:
    pass
  hdfsBuilder *hdfsNewBuilder()
  void hdfsFreeBuilder(hdfsBuilder *bld)

  char* hdfsGetLastError()

  void hdfsBuilderSetNameNode(hdfsBuilder *bld, char *nn)
  void hdfsBuilderSetNameNodePort(hdfsBuilder *bld, tPort port)

  hdfsFS hdfsBuilderConnect(hdfsBuilder *bld)
  int hdfsDisconnect(hdfsFS fs)

  hdfsFileInfo *hdfsListDirectory(hdfsFS fs, char *path, int *numEntries)
  void hdfsFreeFileInfo(hdfsFileInfo *infos, int numEntries)

  hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize,
                        short replication, tOffset blocksize)
  int hdfsCloseFile(hdfsFS fs, hdfsFile file)

  int hdfsFileIsOpenForRead(hdfsFile file)
  int hdfsFileIsOpenForWrite(hdfsFile file)

  int hdfsRead(hdfsFS fs, hdfsFile file, void *buffer, tSize length)
  int hdfsWrite(hdfsFS fs, hdfsFile file, void *buffer, tSize length)
  int hdfsFlush(hdfsFS fs, hdfsFile file)
  int hdfsHFlush(hdfsFS fs, hdfsFile file)
  int hdfsSync(hdfsFS fs, hdfsFile file)

  BlockLocation *hdfsGetFileBlockLocations(hdfsFS fs, char *path,
                                           int start, int length, int *numOfBlock);
  void hdfsFreeFileBlockLocations(BlockLocation *locations, int numOfBlock);
