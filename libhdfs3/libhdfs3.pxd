cdef extern from *:
  cdef int O_RDONLY
  cdef int O_WRONLY
  cdef int O_APPEND
  cdef int O_SYNC
  cdef int O_RDWR
  cdef int O_CREAT
  cdef int O_EXCL

cdef extern from "hdfs.h":
  ctypedef int tSize
  ctypedef int tTime
  ctypedef int tOffset
  ctypedef unsigned int tPort

  char* hdfsGetLastError()

  struct hdfsBuilder:
    pass

  hdfsBuilder *hdfsNewBuilder()
  void hdfsFreeBuilder(hdfsBuilder *bld)

  void hdfsBuilderSetNameNode(hdfsBuilder *bld, char *nn)
  void hdfsBuilderSetNameNodePort(hdfsBuilder *bld, tPort port)

  ctypedef struct HdfsFileSystemInternalWrapper:
    pass
  ctypedef HdfsFileSystemInternalWrapper hdfsFS

  hdfsFS hdfsBuilderConnect(hdfsBuilder *bld)
  int hdfsDisconnect(hdfsFS fs)

  int hdfsExists(hdfsFS fs, char *path)
  int hdfsCopy(hdfsFS srcFS, char *src, hdfsFS dstFS, char *dst)
  int hdfsMove(hdfsFS srcFS, char *src, hdfsFS dstFS, char *dst)
  int hdfsRename(hdfsFS fs, char *oldPath, char *newPath)
  int hdfsDelete(hdfsFS fs, char *path, int recursive)
  int hdfsCreateDirectory(hdfsFS fs, char *path)

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

  hdfsFileInfo *hdfsListDirectory(hdfsFS fs, char *path, int *numEntries)
  void hdfsFreeFileInfo(hdfsFileInfo *infos, int numEntries)

  ctypedef struct HdfsFileInternalWrapper:
    pass
  ctypedef HdfsFileInternalWrapper hdfsFile

  hdfsFile hdfsOpenFile(hdfsFS fs, char *path, int flags, int bufferSize,
                        short replication, tOffset blocksize)
  int hdfsCloseFile(hdfsFS fs, hdfsFile file)

  int hdfsFileIsOpenForRead(hdfsFile file)
  int hdfsFileIsOpenForWrite(hdfsFile file)

  int hdfsRead(hdfsFS fs, hdfsFile file, void *buffer, tSize length)
  int hdfsWrite(hdfsFS fs, hdfsFile file, void *buffer, tSize length)
  int hdfsFlush(hdfsFS fs, hdfsFile file)
  int hdfsHFlush(hdfsFS fs, hdfsFile file)
  int hdfsSync(hdfsFS fs, hdfsFile file)

  ctypedef struct BlockLocation:
    int numOfNodes
    char **hosts
    char **names
    char **topologyPaths
    tOffset length
    tOffset offset
    int corrupt

  BlockLocation *hdfsGetFileBlockLocations(hdfsFS fs, char *path,
                                           int start, int length, int *numOfBlock)
  void hdfsFreeFileBlockLocations(BlockLocation *locations, int numOfBlock)
