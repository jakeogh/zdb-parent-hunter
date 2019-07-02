#!/usr/bin/env python3.7

import sys
import os
import time
import asyncio
from asyncio.subprocess import PIPE
import pprint
from typing import Any
from pathlib import Path
from itertools import zip_longest
import copy
import shelve   # shelve is broken, with writeback=True, it's not waiting until I call .sync() to persist to disk
import attr
from attr.converters import optional
import cattr    # noqa: F401
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import BigInteger, Float, String, LargeBinary
import click

#pylint: disable=missing-docstring
#pylint: disable=too-few-public-methods
#pylint: disable=multiple-statements
#pylint: disable=invalid-name

DB_FILE = "/home/user/.zfs_index/sqlite." + str(time.time()) + '.db'
DB_PATH = 'sqlite:///' + DB_FILE
ENGINE = create_engine(DB_PATH, echo=True)
Base = declarative_base()

_print = print


def print(*args, **kwargs):
    _print(*args, file=sys.stderr, **kwargs)


eprint = print


async def run_command(args):
    eprint("command:", ' '.join(args[:15]), "...")
    process = await asyncio.create_subprocess_exec(*args, stdout=PIPE)
    #print(process.returncode)  # None
    #time.sleep(3)  # long enough
    #print(process.returncode)  # still None
    #if process.returncode is not None:  # still None
    #    quit(1)

    async for line in process.stdout:
        #if process.returncode is not None: # stillllll None
        #    if line:
        yield line


def grouper(iterable, num, fillvalue=None):
    args = [iter(iterable)] * num
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


def strify(thing):
    try:
        return str(thing, encoding='utf8')
    except TypeError:
        assert isinstance(thing, str)
        return thing


def validate(instance, attribute, value):
    if value is None:  # should instead "if value is attribute.default"
        if attribute.default is None:  # .default == NOTHING if not set
            return
    else:
        assert isinstance(value, attribute.type)


#pylint: disable=bad-whitespace
@attr.s(auto_attribs=True)
class Dnode():
    inode:    int = attr.ib(validator=validate, converter=int)                              # noqa: E241
    lvl:      int = attr.ib(validator=validate, converter=int)                              # noqa: E241
    iblk:     int = attr.ib(validator=validate, converter=int)                              # noqa: E241
    dblk:     int = attr.ib(validator=validate, converter=int)                              # noqa: E241
    dsize:    int = attr.ib(validator=validate, converter=int)                              # noqa: E241
    dnsize:   int = attr.ib(validator=validate, converter=int)                              # noqa: E241
    lsize:    int = attr.ib(validator=validate, converter=int)                              # noqa: E241
    full:   float = attr.ib(validator=validate, converter=float)                            # noqa: E241
    type:     str = attr.ib(validator=validate, converter=strify)                           # noqa: E241
    flags:    str = attr.ib(validator=validate, converter=optional(strify),  default=None)  # noqa: E241
    maxblkid: int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    path:   bytes = attr.ib(validator=validate,                              default=None)  # noqa: E241
    uid:      int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    gid:      int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    atime:    str = attr.ib(validator=validate, converter=optional(strify),  default=None)  # noqa: E241
    mtime:    str = attr.ib(validator=validate, converter=optional(strify),  default=None)  # noqa: E241
    ctime:    str = attr.ib(validator=validate, converter=optional(strify),  default=None)  # noqa: E241
    crtime:   str = attr.ib(validator=validate, converter=optional(strify),  default=None)  # noqa: E241
    gen:      int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    mode:     int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    size:     int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    parent:   int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    links:    int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
    pflags:   int = attr.ib(validator=validate, converter=optional(int),     default=None)  # noqa: E241
#pylint: enable=bad-whitespace

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


def generate_sqla(cls):
    #first attribute is always the primary key
    typemap = \
        {
            int: BigInteger,
            str: String,
            float: Float,
            bytes: LargeBinary,
        }

    dst_dict = {"__tablename__": "dnodes"}
    for i, src_attr in enumerate(attr.fields(cls)):
        dst_args = {}
        dst_sq_type = typemap[src_attr.type]
        if i == 0:
            dst_args["primary_key"] = True

        dst_dict[src_attr.name] = Column(dst_sq_type, **dst_args)

    return type('SQADnode', (Base,), dst_dict)


SQADnode = generate_sqla(Dnode)
Base.metadata.create_all(ENGINE)


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
    return str(b' '.join(line.split(match)[1:]).strip(), encoding='utf8')


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
    modify_existing = bool(dnode_map)
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
            if mutate_if_match(line, dn, modify_existing, oline):
                continue

            if skip(line):
                continue

            if line == marker:
                found_marker = True
                found_id = False
                continue

            if debug: eprint("unmatched line:", line)

        if not len(dnode_map.keys()) % 500000:
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
                if not modify_existing:  # write new dn to db
                    dnode_map[object_id] = dn  # limitation of shelve, keys are str()
                else:
                    assert object_id in dnode_map.keys()

                #import IPython; IPython.embed()
                dn = None
                object_id = None

            if not dn:
                assert not object_id
                object_id = str(int(sline.pop(0)))
                if modify_existing:  # get pre-existing dn to mutate
                    dn = dnode_map[object_id]
                else:
                    dn_type = str(b" ".join(sline[7:]), encoding='utf8')
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
                if not modify_existing:
                    if object_id not in dnode_map.keys():
                        dnode_map[object_id] = dn
                break

    if not modify_existing:
        if object_id not in dnode_map.keys():
            dnode_map[object_id] = dn

    if debug > 1: pprint.pprint(dnode_map)

    if status:
        map_count = len(dnode_map)
        eprint("Done. {0} dnode records saved in:\n{1}\n".format(map_count, shelve_file))

    dnode_map.close()
    return


async def parse_zdb_dnodes(poolname, inodes, status, debug, exit_early):
    path_command =  ["zdb", poolname, "-L", "-dddd", "-v", "-P", "--"]  # need to skip 0 or takes forever   # noqa: E222
    command =       ["zdb", poolname, "-L", "-dddd", "-P"]              # wont get paths                    # noqa: E222
    shelve_file = generate_shelve_file(poolname)

    for inode in inodes:
        command.append(str(inode))

    await reader(command, status, debug, exit_early, poolname, shelve_file)

    file_dnodes = []
    dnode_map = shelve.open(shelve_file, writeback=False)
    for key in dnode_map.keys():
        if dnode_map[key].type == 'ZFS plain file':
            file_dnodes.append(str(key))
    dnode_map.close()

    #debug = 2
    exit_early = False
    inodes = None

    file_dnodes = sorted(file_dnodes, key=int)
    for group in grouper(file_dnodes, 2000):
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
@click.argument("inodes", type=int, nargs=-1)
@click.option("--no-status", is_flag=True)
@click.option("--debug", count=True)
@click.option("--exit-early", type=int, help="(for testing)")
def index(poolname, inodes, no_status, debug, exit_early):
    status = not no_status
    assert len(poolname.split()) == 1
    assert '/' in poolname
    if status:
        eprint("gathering all dnodes")

    asyncio.run(parse_zdb_dnodes(poolname, inodes, status, debug, exit_early))


@cli.command()
@click.argument("pickle", type=click.Path(exists=True, dir_okay=False, allow_dash=True))
def load(pickle):
    p = shelve.open(pickle)

    eprint("len(p):", len(p))

    from IPython import embed
    from traitlets.config import get_config
    c = get_config()
    c.InteractiveShellEmbed.colors = "Linux"
    embed(config=c)


if __name__ == "__main__":
    # print("python version:", sys.version)  # 3.7
    cli()
