#!/usr/bin/env python3.7

import sys
import os
import time
import asyncio
import pprint
import attr
import copy
import shelve  # shelve is broken, with writeback=True, it's not waiting until I call .sync() to persist to disk
from sqlalchemy import create_engine
from typing import Any
from functools import partial
from pathlib import Path
from asyncio.subprocess import PIPE
from itertools import zip_longest
import click

engine = create_engine('sqlite:///:memory:', echo=True)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


async def run_command(args):
    eprint("command:", ' '.join(args))
    process = await asyncio.create_subprocess_exec(*args, stdout=PIPE)
    async for line in process.stdout:
        yield line


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def print_match(object_id, parent_id, pad):
    print(object_id, parent_id, end=pad + '\n')


def print_status(object_id, pad, count, start):
    rate = count / (time.time() - start)
    eprint("checking id:", object_id, "@", int(rate), "id/sec", end=pad + '\r', flush=True)


def generate_shelve_file(poolname):
    timestamp = int(time.time())
    data_dir = Path(os.path.expanduser("~/.zfs_index"))
    data_dir.mkdir(exist_ok=True)
    data_file = Path("_".join([poolname, str(timestamp), str(os.getpid()), '.pickle']))
    shelve_file = data_dir / data_file
    shelve_file.parent.mkdir(exist_ok=True)
    return str(shelve_file)  # as of py 3.7, shelve isnt supporting path-like objects


def pathify(path):
    return Path(os.fsdecode(path))


@attr.s(auto_attribs=True)
class Dnode():
    inode:    int = attr.ib(converter=int)                                                                    # noqa: E241
    lvl:      int = attr.ib(converter=int)                                                                    # noqa: E241
    iblk:     int = attr.ib(converter=int)                                                                    # noqa: E241
    dblk:     int = attr.ib(converter=int)                                                                    # noqa: E241
    dsize:    int = attr.ib(converter=int)                                                                    # noqa: E241
    dnsize:   int = attr.ib(converter=int)                                                                    # noqa: E241
    lsize:    int = attr.ib(converter=int)                                                                    # noqa: E241
    full:   float = attr.ib(converter=float)                                                                  # noqa: E241
    type:     str = attr.ib(converter=partial(str, encoding='utf8'))                                          # noqa: E241
    flags:    str = attr.ib(converter=attr.converters.optional(partial(str, encoding='utf8')), default=None)  # noqa: E241
    maxblkid: int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    path:   bytes = attr.ib(converter=attr.converters.optional(pathify), default=None)                        # noqa: E241
    uid:      int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    gid:      int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    atime:    str = attr.ib(converter=attr.converters.optional(partial(str, encoding='utf8')), default=None)  # noqa: E241
    mtime:    str = attr.ib(converter=attr.converters.optional(partial(str, encoding='utf8')), default=None)  # noqa: E241
    ctime:    str = attr.ib(converter=attr.converters.optional(partial(str, encoding='utf8')), default=None)  # noqa: E241
    crtime:   str = attr.ib(converter=attr.converters.optional(partial(str, encoding='utf8')), default=None)  # noqa: E241
    gen:      int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    mode:     int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    size:     int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    parent:   int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    links:    int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241
    pflags:   int = attr.ib(converter=attr.converters.optional(int), default=None)                            # noqa: E241

    def __attrs_post_init__(self):
        self._initialized = True

    def __setattr__(self, name: str, value: Any) -> None:
        """Call the converter and validator when we set the field (by default it only runs on __init__)"""
        if not hasattr(self, "_initialized"):
            super().__setattr__(name, value)
            return
        for attribute in [a for a in getattr(self.__class__, '__attrs_attrs__', []) if a.name == name]:
            attribute_type = getattr(attribute, 'type', None)
            if attribute_type is not None:

                attribute_converter = getattr(attribute, 'converter', None)
                if attribute_converter is not None:
                    value = attribute_converter(value)

            attribute_validator = getattr(attribute, 'validator', None)
            if attribute_validator is not None:
                attribute_validator(self, attribute, value)

        super().__setattr__(name, value)


# checking assumptions. rather pointless.
START_SKIP_OK = \
    [
        b'Indirect blocks:',
        b'segment [',
        b'Leafs with 2^n pointers:',
        b'Fat ZAP stats:',
        b'Entries with n chunks:',
        b'Buckets with n entries:',
        b'Blocks n/10 full:',
        b'Blocks with n*5 entries:',
        b'ZAP entries: ',
        b'Pointer table:',
        b'Leaf blocks: ',
        b'Total blocks: ',
        b'zap_magic: ',
        b'zap_block_type: ',
        b'zt_blks_copied: ',
        b'zt_shift: ',
        b'zt_numblks: ',
        b'zt_blk: ',
        b'zt_nextblk: ',
        b'bonus System attributes',
        b'microzap: ',
        b'zap_salt: ',
        b'obj-3e8 = ',
        b'3e8 = ',
        b'0 = ',
        b'obj-0 = ',
        b'utf8only = ',
        b'ROOT = ',
        b'casesensitivity = ',
        b'DELETE_QUEUE = ',
        b'normalization = ',
        b'VERSION = ',
        b'SA_ATTRS = ',
        b'1024 elements',
    ]

END_SKIP_OK = \
    [
        b'(type: Regular File)',
        b'(type: Directory)',
        b'bonus System attributes',
    ]

IN_SKIP_OK = \
    [
        b'****************************************',
        b' L0 ',
        b' L1 ',
    ]


def skip(line):
    for thing in START_SKIP_OK:
        if line.startswith(thing):
            return True
    for thing in END_SKIP_OK:
        if line.endswith(thing):
            return True
    for thing in IN_SKIP_OK:
        if thing in line:
            return True
    return False


def norm(line):
    line = line.strip()
    line = b' '.join(line.split())
    return line


def carve(line, match):
    return b' '.join(line.split(match)[1:]).strip()


MATCHES = \
    {
        'flags': b'dnode flags:',
        'maxblkid': b'dnode maxblkid:',
        'path': b'path',
        'uid': b'uid',
        'gid': b'gid',
        'atime': b'atime',
        'mtime': b'mtime',
        'ctime': b'ctime',
        'crtime': b'crtime',
        'gen': b'gen',
        'mode': b'mode',
        'size': b'size',
        'parent': b'parent',
        'links': b'links',
        'pflags': b'pflags',
    }


def mutate_if_match(line, dn, writeback, oline):
    for key in MATCHES.keys():
        match = MATCHES[key]
        if line.startswith(match):
            if key == 'path':
                ans = oline[6:]
                assert ans.endswith(b'\n')
                ans = ans[:-1]
            else:
                ans = carve(line, match)
            if writeback:
                if getattr(dn, key):
                    temp_dn = copy.deepcopy(dn)
                    setattr(temp_dn, key, ans)
                    assert getattr(dn, key) == getattr(temp_dn, key)
                    return True
            setattr(dn, key, ans)
            return True
    return False


async def reader(command, status, debug, exit_early, poolname, shelve_file):
    dnode_map = shelve.open(shelve_file, writeback=True)  # too slow unless we call sync() only once in awhile
    modify = bool(dnode_map)
    timestamp = int(time.time())
    pad = 25 * ' '
    marker = norm(b'    Object  lvl   iblk   dblk  dsize  dnsize  lsize   %full  type\n')
    found_marker = False
    found_id = False
    object_id = None
    line_num = 0
    dn = None
    async for line in run_command(command):
        oline = line
        if debug > 1: eprint(str(line_num) + ":", oline)
        line = norm(line)
        line_num += 1
        if not line: continue
        if found_id:
            if mutate_if_match(line, dn, modify, oline):
                continue

            if skip(line):
                continue

            if line == marker:
                found_marker = True
                found_id = False
                continue

            if debug: eprint("unmatched line:", line)

        if not (len(dnode_map.keys()) % 500000):
            if dnode_map.keys():
                if debug:
                    eprint("saving:", shelve_file)
                dnode_map.sync()

        if found_marker:
            assert not found_id
            sline = line.split()
            if dn:  # save prev dn
                #eprint("saving object_id:", object_id)
                assert object_id is not None
                if not modify:  # write new dn to db
                    dnode_map[str(object_id)] = dn  # limitation of shelve, keys are str()
                else:
                    assert str(object_id) in dnode_map.keys()

                dn = None
                object_id = None

            if not dn:
                assert not object_id
                object_id = int(sline.pop(0))
                if modify:  # get pre-existing dn to mutate
                    dn = dnode_map[str(object_id)]
                else:
                    dn_type = b" ".join(sline[7:])
                    dn = Dnode(object_id, *sline[:7], dn_type)

            if status:
                lpm = len(dnode_map)
                print_status(object_id, pad, lpm, timestamp)

            found_id = True
            found_marker = False
            continue

        if line == marker:
            found_marker = True

        if exit_early:
            lpm = len(dnode_map)
            if lpm >= exit_early:
                import warnings
                warnings.filterwarnings("ignore")
                eprint("\n\nExiting early after {0} id's".format(lpm))
                break

    if debug > 1: pprint.pprint(dnode_map)

    if status:
        map_count = len(dnode_map)
        eprint("Done. {0} dnode records saved in:\n{1}\n".format(map_count, shelve_file))

    dnode_map.close()
    return


async def parse_zdb_dnodes(poolname, status, debug, exit_early):
    path_command =  ["zdb", poolname, "-L", "-dddd", "-v", "-P", "--"]  # need to skip 0 or takes forever
    command =       ["zdb", poolname, "-L", "-dddd", "-P"]              # wont get paths
    shelve_file = generate_shelve_file(poolname)

    await reader(command, status, debug, exit_early, poolname, shelve_file)

    file_dnodes = []
    dnode_map = shelve.open(shelve_file, writeback=False)
    for key in dnode_map.keys():
        if dnode_map[key].type == 'ZFS plain file':
            file_dnodes.append(str(key))
    dnode_map.close()

    #debug = 2
    exit_early = False

    for group in grouper(file_dnodes, 1000):
        group = [g for g in group if g]
        next_command = path_command + group
        await reader(next_command, status, debug, exit_early, poolname, shelve_file)


def validate_pool(ctx, param, value):
    try:
        assert not value.startswith('/')
    except AssertionError:
        raise click.BadParameter('ZFS pool name must not start with "/"')
    return value


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument("poolname", type=str, nargs=1, callback=validate_pool)
@click.option("--no-status", is_flag=True)
@click.option("--debug", count=True)
@click.option("--exit-early", type=int, help="(for testing)")
def index(poolname, no_status, debug, exit_early):
    status = not no_status
    assert len(poolname.split()) == 1
    assert '/' in poolname
    if status:
        eprint("gathering all dnodes")

    asyncio.run(parse_zdb_dnodes(poolname, status, debug, exit_early))


@cli.command()
@click.argument("pickle", type=click.Path(exists=True, dir_okay=False, allow_dash=True))
def load(pickle):
    p = shelve.open(pickle)

    eprint("len(p):", len(p))
    import IPython; IPython.embed()


if __name__ == "__main__":
    # print("python version:", sys.version)  # 3.7
    cli()


#
#        if status:
#            map_count = len(parent_map)
#            all_parents = [p for p in parent_map.values() if p]
#            unique_parents = set(all_parents)
#            none_count = map_count - len(all_parents)
#            with_parent_count = map_count - none_count
#            assert with_parent_count == len(all_parents)
#            eprint("Done. {0} id->parent mappings saved in:\n{1}\n".format(map_count, shelve_file))
#            eprint("# of id's:", map_count)
#            eprint("# of id's with no parent:", none_count)
#            eprint("# of id's with parent:", with_parent_count)
#            eprint("# of unique parents:", len(unique_parents))
#
#    await reader(command, parents, status, debug, exit_early)


#@cli.command()
#@click.argument("poolname", type=str, nargs=1)
#@click.option("--no-status", is_flag=True)
#@click.option("--debug", is_flag=True)
#@click.option("--exit-early", type=int, help="(for testing)")
#def dmu_dnode_L0(poolname, no_status, debug, exit_early):
#    status = not no_status
#    assert len(poolname.split()) == 1
#    assert '/' in poolname
#    if status:
#        eprint("gathering all L0 entries from DMU dnode")
#
#    asyncio.run(parse_zdb_l0(poolname, status, debug, exit_early))
#
#
#@cli.command()
#@click.argument("poolname", type=str, nargs=1)
#@click.argument("parents", type=int, nargs=-1)
#@click.option("--no-status", is_flag=True)
#@click.option("--debug", is_flag=True)
#@click.option("--exit-early", type=int, help="(for testing)")
#def parents(poolname, parents, no_status, debug, exit_early):
#    status = not no_status
#    assert len(poolname.split()) == 1
#    assert '/' in poolname
#    if status:
#        eprint("gathering all id->parent mappings")
#        if parents:
#            eprint("looking for id's with parent(s):", *parents)
#
#    asyncio.run(parse_zdb_parents(poolname, parents, status, debug, exit_early))




#@attr.s(auto_attribs=True)
#class dmu_dnode():
#    offset: int
#    level: int
#    dva: dict
#    checksum: str
#    compress: str
#    encryption: bool
#    size: str
#    birth: str
#    fill: int
#
#
#async def parse_zdb_l0(poolname, status, debug, exit_early):
#    command = ["zdb", "-L", "-ddddbbbbvv", poolname, "0"]
#    if debug: eprint(command)
#    async def reader(command, status, debug, exit_early):
#        timestamp = int(time.time())
#        shelve_file = generate_shelve_file('L0_list', poolname, timestamp)
#        node_list = []
#        pad = 25 * ' '
#        marker = b'[L0 DMU dnode]'
#        async for line in run_command(command):
#            if debug:
#                eprint(line)
#            if b"L0 HOLE" in line:
#                continue
#            if marker in line:
#                line = line.split()
#                dva1 = line[2].split(b'<')[-1][:-1]
#                dva2 = line[2].split(b'<')[-1][:-1]
#                d = dmu_dnode(
#                    offset=line[0],
#                    level=line[1][-1],
#                    dva={0: dva1, 1: dva2},
#                    checksum=line[7],
#                    compress=line[8],
#                    encryption=(True if line[9] == b'encrypted' else False),
#                    size=line[14].split(b'=')[-1],
#                    birth=line[15].split(b'=')[-1],
#                    fill=line[16].split(b'=')[-1])
#
#                node_list.append(d)
#                if status and not debug:
#                    lpm = len(node_list)
#                    print_status(d.offset, pad, lpm, timestamp)
#
#        with open(shelve_file, 'wb') as fh:
#            pickle.dump(node_list, fh)
#
#        if status:
#            dnode_count = len(node_list)
#            eprint("Done. {0} L0 dnodes saved in:\n{1}\n".format(dnode_count, shelve_file))
#
#    await reader(command, status, debug, exit_early)

