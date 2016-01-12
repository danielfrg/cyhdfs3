from __future__ import unicode_literals

import click
from click.testing import CliRunner

from cyhdfs3 import cli

from utils import *


def test_create_file_list(hdfs):
    runner = CliRunner()
    result = runner.invoke(cli.ls)
    # assert result.exit_code == 0
    # assert result.output == 'Hello Peter!\n'
