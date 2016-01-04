# libhdfs3.py

An attempt to wrap [libhdfs3](https://github.com/PivotalRD/libhdfs3) using cython.

## Development

```bash
# Docker container with conda and libhdfs3: Connect to remote HDFS
docker build -t libhdfs3 .
docker run -it -v $(pwd):/cylibhdfs3 libhdfs3

# Docker container with conda, libhdfs3 and HDFS (Pseudo Distributed mode)
docker build -t libhdfs3.hdfs -f Dockerfile.hdfs .
docker run -it -p 8020:8020 -p 50070:50070 -v $(pwd):/cylibhdfs3 libhdfs3.hdfs

# Bash inside the container
docker ps # Get container ID
docker exec -it {{ ID }} bash
```
