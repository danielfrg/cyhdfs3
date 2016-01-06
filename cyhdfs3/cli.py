from __future__ import division
import sys
import traceback

import click


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def main():
    try:
        cli(obj={})
    except Exception as e:
        click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--namenode', '-n', default='localhost', required=False, help='Namenode host', show_default=True)
@click.option('--port', '-p', default=8020, required=False, help='Namenode port', show_default=True)
@click.pass_context
def cli(ctx, namenode, port):
    import pyximport; pyximport.install()

    import os
    os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"

    from cyhdfs3 import chdfs
    ctx.obj = {}
    ctx.obj['client'] = chdfs.HDFSClient()


@cli.command(short_help='List a path')
@click.argument('path', required=False, default='/')
@click.option('--recurse', '-R', is_flag=True, default=False, required=False, help='Recurse into subdirectories', show_default=True)
@click.pass_context
def ls(ctx, path, recurse):
    client = ctx.obj['client']
    files = client.list_dir(path, recurse=recurse)

    for f in files:
        row = []

        perm = octal_to_perm(f.permissions)
        t_perm = ('d' if f.kind == 'd' else '-') + perm
        row.append(t_perm)
        s = max([len(j.owner) for j in files]) + 3
        row.append(f.owner.ljust(s))
        s = max([len(j.group) for j in files]) + 3
        row.append(f.group.ljust(s))
        s = max([len(str(j.size)) for j in files]) + 3
        row.append(str(f.size).ljust(s))
        row.append(f.name)

        click.echo(" ".join(row))


def octal_to_perm(octal):
    import stat
    perms = list("-" * 9)
    if octal & stat.S_IRUSR:
        perms[0] = "r"
    if octal & stat.S_IWUSR:
        perms[1] = "w"
    if octal & stat.S_IXUSR:
        perms[2] = "x"
    if octal & stat.S_IRGRP:
        perms[3] = "r"
    if octal & stat.S_IWGRP:
        perms[4] = "w"
    if octal & stat.S_IXGRP:
        perms[5] = "x"
    if octal & stat.S_IROTH:
        perms[6] = "r"
    if octal & stat.S_IWOTH:
        perms[7] = "w"
    if octal & stat.S_IXOTH:
        perms[8] = "x"
    return "".join(perms)


@cli.command(short_help='Display fs stats')
@click.pass_context
def df(ctx):
    client = ctx.obj['client']

    fs = "hdfs://{}:{}".format(client.host, client.port)
    used = client.get_used()
    capacity = client.get_capacity()
    avalable = capacity - used
    block_size = client.get_default_block_size()
    use_p = "%.2f" % (used / capacity)

    headers = ["Filesystem", "Block Size", "Size", "Used", "Available", "Use%"]
    row1 = [fs, block_size, capacity, used, avalable, use_p]
    cols_lenghts = []

    for header, row in zip(headers, row1):
        cols_lenghts.append(max([len(str(_)) for _ in [header, row]]) + 1)
    for row in [headers, row1]:
        lrow = []
        for i, col in enumerate(row):
            if i == 0:
                lrow.append(str(col).ljust(cols_lenghts[i]))
            else:
                lrow.append(str(col).rjust(cols_lenghts[i]))
        click.echo(" ".join(lrow))
