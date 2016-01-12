from __future__ import print_function, absolute_import

import posixpath
from random import randint

from click.testing import CliRunner

from utils import *

from cyhdfs3.cli import cli


def test_head(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    data = b'a' * randint(1, 9) * 2**10
    data += b'b' * randint(1, 9) * 2**10
    data += b'c' * randint(1, 9) * 2**10

    with hdfs.open(fname, 'w', replication=1) as f:
        f.write(data)

    runner = CliRunner()

    result = runner.invoke(cli, ['head', fname])
    assert result.exit_code == 0
    assert result.output[:-1].encode('utf-8') == data[:1*2**10]

    n = randint(1, 9)
    result = runner.invoke(cli, ['head', fname, "-b", n*2**10])
    assert result.exit_code == 0
    assert len(result.output[:-1]) == len(data[:n*2**10])
    assert result.output[:-1].encode('utf-8') == data[:n*2**10]

    result = runner.invoke(cli, ['head', fname, "-b", len(data)])
    assert result.exit_code == 0
    assert result.output[:-1].encode('utf-8') == data


def test_tail(hdfs, request):
    testname = request.node.name
    fname = posixpath.join(TEST_DIR, testname)

    data = b'x' * randint(1, 9) * 2**10
    data += b'y' * randint(1, 9) * 2**10
    data += b'z' * randint(1, 9) * 2**10

    with hdfs.open(fname, 'w', replication=1) as f:
        f.write(data)

    runner = CliRunner()

    result = runner.invoke(cli, ['tail', fname])
    assert result.exit_code == 0
    assert result.output[:-1].encode('utf-8') == data[len(data) - 1*2**10:]

    n = randint(1, 9)
    result = runner.invoke(cli, ['tail', fname, "-b", n*2**10])
    assert result.exit_code == 0
    assert len(result.output[:-1]) == len(data[:n*2**10])
    assert result.output[:-1].encode('utf-8') == data[len(data) - n*2**10:]

    result = runner.invoke(cli, ['tail', fname, "-b", len(data)])
    assert result.exit_code == 0
    assert result.output[:-1].encode('utf-8') == data
