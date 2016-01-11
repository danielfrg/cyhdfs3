import posixpath
import subprocess

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

import cyavro

from utils import *


avroschema = """ {"type": "record",
"name": "from_bytes_test",
"fields":[
   {"name": "id", "type": "int"},
   {"name": "name", "type": "string"}
]
}
"""


@pytest.mark.parametrize(("codec",), [("null", ), ("deflate", ), ("snappy", )])
def test_avro_move_read(hdfs, request, tmpdir, codec):
    testname = request.node.name.replace('[', '_').replace(']', '_')
    hdfs_path = posixpath.join(TEST_DIR, testname + '.avro')
    local_path = tmpdir.join(testname + '.avro').realpath().strpath
    print local_path

    # Create an avrofile
    writer = cyavro.AvroWriter(local_path, codec, avroschema)

    ids = np.random.randint(100, size=10)
    ids = np.arange(10)
    names = pdt.rands_array(10, 10)
    df_write = pd.DataFrame({"id": ids, "name": names})
    df_write = cyavro.prepare_pandas_df_for_write(df_write, avroschema, copy=False)

    writer.write(df_write)
    writer.close()

    # Move file to hdfs
    out = subprocess.call("hadoop fs -put {} {}".format(local_path, hdfs_path), shell=True)
    assert out == 0

    # Read avro and compare data
    with hdfs.open(hdfs_path, 'r') as f:
        reader = f.read_avro()
        reader.init_buffers()
        df_read = pd.DataFrame(reader.read_chunk())

        pdt.assert_frame_equal(df_write, df_read)
        reader.close()


@pytest.mark.parametrize(("codec",), [("null", ), ("deflate", ), ("snappy", )])
def test_avro_write_read(hdfs, request, tmpdir, codec):
    testname = request.node.name
    hdfs_path = posixpath.join(TEST_DIR, testname + '.avro')
    local_path = tmpdir.join(testname + '.avro').realpath().strpath

    # Create an avrofile
    writer = cyavro.AvroWriter(local_path, codec, avroschema)

    ids = np.random.randint(100, size=10)
    ids = np.arange(10)
    names = pdt.rands_array(10, 10)
    df_write = pd.DataFrame({"id": ids, "name": names})
    df_write = cyavro.prepare_pandas_df_for_write(df_write, avroschema, copy=False)

    writer.write(df_write)
    writer.close()

    # Read avro file bytes from localfile and write them to hdfs
    data = b''
    with open(local_path, 'r') as f:
        data = f.read()
    with hdfs.open(hdfs_path, 'w') as f:
        f.write(data)

    # Read avro file bytes from hdfs and compare
    with hdfs.open(hdfs_path, 'r') as f:
        read_data = f.read()
        assert len(data) == len(read_data)
        assert data == read_data
