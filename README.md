# cyhdfs3

An attempt to wrap [libhdfs3](https://github.com/PivotalRD/libhdfs3) using cython.

## Development

```bash
# Docker container with conda and libhdfs3: Connect to remote HDFS
docker build -t cyhdfs3 .
docker run -it -v $(pwd):/cyhdfs3 cyhdfs3

# Docker container with conda, libhdfs3 and HDFS (Pseudo Distributed mode)
docker build -t cyhdfs3.hdfs -f Dockerfile.hdfs .
docker run -it -p 8020:8020 -p 50070:50070 -v $(pwd):/cyhdfs3 cyhdfs3.hdfs

# Bash inside the container
docker exec -it $(docker ps -q -l) bash
```
