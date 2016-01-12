from __future__ import division

import sys
import click

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def main():
    try:
        cli(obj={})
    except Exception as e:
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--namenode', '-n', default='localhost', required=False, help='Namenode host', show_default=True)
@click.option('--port', '-p', default=8020, required=False, help='Namenode port', show_default=True)
@click.pass_context
def cli(ctx, namenode, port):
    # import pyximport; pyximport.install()
    import os
    import cyhdfs3
    os.environ["LIBHDFS3_CONF"] = "/etc/hadoop/conf/hdfs-site.xml"
    ctx.obj = {}
    ctx.obj['client'] = cyhdfs3.HDFSClient()


@cli.command(short_help='Change mode of a file or directory.')
@click.argument('mode', required=True)
@click.argument('path', required=True)
@click.pass_context
def chmod(ctx, mode, path):
    client = ctx.obj['client']
    client.chmod_s(path.encode('UTF-8'), mode.encode('UTF-8'))


@cli.command(short_help='Change owner of a file or directory.')
@click.argument('owner', required=True)
@click.argument('path', required=True)
@click.pass_context
def chown(ctx, owner, path):
    client = ctx.obj['client']
    client.chown(path.encode('UTF-8'), owner=owner.encode('UTF-8'))


@cli.command(short_help='Change group of a file or directory.')
@click.argument('group', required=True)
@click.argument('path', required=True)
@click.pass_context
def chgrp(ctx, group, path):
    client = ctx.obj['client']
    client.chown(path.encode('UTF-8'), group=group.encode('UTF-8'))


@cli.command(short_help='Display the first (n) bytes of a file')
@click.argument('path', required=False, default='/')
@click.option('--bytes', '-b', 'nbytes', default=1*2**10, required=False, help='Number of bytes', show_default=True)
@click.pass_context
def head(ctx, path, nbytes):
    client = ctx.obj['client']
    try:
        with client.open(path, 'r') as f:
            click.echo(f.read(length=nbytes))
    except IOError:
        if client.path_info(path).kind == 'd':
            click.echo("head: error reading '{}': Is a directory".format(path), err=True)
            sys.exit(1)


@cli.command(short_help='Display the last (n) bytes of a file')
@click.argument('path', required=False, default='/')
@click.option('--bytes', '-b', 'nbytes', default=1*2**10, required=False, help='Number of bytes', show_default=True)
@click.pass_context
def tail(ctx, path, nbytes):
    client = ctx.obj['client']
    try:
        with client.open(path, 'r') as f:
            start = max(0, f.info.size - nbytes)
            f.seek(start)
            click.echo(f.read(length=nbytes))
    except IOError as e:
        if client.path_info(path).kind == 'd':
            click.echo("tail: error reading '{}': Is a directory".format(path), err=True)


@cli.command(short_help='Create directory and all non-existent parents')
@click.argument('path', required=False, default='/')
@click.pass_context
def mkdir(ctx, path):
    client = ctx.obj['client']
    client.create_dir(path)


@cli.command(short_help='List a path')
@click.argument('path', required=False, default='/')
@click.option('--recurse', '-R', is_flag=True, default=False, required=False, help='Recurse into subdirectories', show_default=True)
@click.pass_context
def ls(ctx, path, recurse):
    import datetime
    client = ctx.obj['client']

    if not client.exists(path):
        click.echo("ls: cannot access {}: No such file or directory".format(path))
        sys.exit(2)

    files = client.list_dir(path, recurse=recurse)
    click.echo("Found {} files".format(len(files)))
    for f in files:
        row = []
        perm = octal_to_perm(f.permissions)
        t_perm = ('d' if f.kind == 'd' else '-') + perm
        row.append(t_perm)

        s = max([len(str(j.replication)) for j in files]) + 1
        r = '-' if f.kind == 'd' else str(f.replication)
        row.append(r.rjust(s))
        s = max([len(j.owner) for j in files]) + 1
        row.append(f.owner.ljust(s))
        s = max([len(j.group) for j in files]) + 1
        row.append(f.group.ljust(s))
        s = max([len(str(j.size)) for j in files]) + 1
        row.append(str(f.size).ljust(s))
        d = lambda x: datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M")
        s = max([len(d(j.lastMod)) for j in files]) + 1
        row.append(d(f.lastMod).ljust(s))
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


if __name__ == '__main__':
    main()
