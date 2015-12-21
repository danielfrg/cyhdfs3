# libhdfs3.py

An attempt to wrap [libhdfs3](https://github.com/PivotalRD/libhdfs3) using cython.

## Development

Docker container installs HDFS (from Cloudera), conda and libhdfs3

```bash
docker build -t libhdfs3 .
docker run -it -p 8020:8020 -p 50070:50070 -v $(pwd):/libhdfs3.py libhdfs3

# Bash inside the container
docker ps # Get container ID
docker exec -it {{ ID }} bash
```
