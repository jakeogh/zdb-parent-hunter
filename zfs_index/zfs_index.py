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
import attr
import cattr
from attr.converters import optional
#import cattr    # noqa: F401
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import BigInteger, Float, String, LargeBinary
from sqlalchemy.orm import sessionmaker
import click

Base = declarative_base()

#pylint: disable=missing-docstring
#pylint: disable=too-few-public-methods
#pylint: disable=multiple-statements
#pylint: disable=invalid-name

_print = print


def print(*args, **kwargs):
    _print(*args, file=sys.stderr, **kwargs)


eprint = print


async def run_command(args):
    truncate = 50
    if len(args) > truncate:
        eprint("command:", ' '.join(args[:truncate]), "...")
    else:
        eprint("command:", ' '.join(args))
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


def generate_db_file(poolname):
    timestamp = int(time.time())
    data_dir = Path(os.path.expanduser("~/.zfs_index"))
    data_dir.mkdir(exist_ok=True)
    data_file = Path("_".join([poolname, str(timestamp), str(os.getpid()), '.sqlite']))
    db_file = data_dir / data_file
    db_file.parent.mkdir(exist_ok=True)
    return str(db_file)


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
    sqla:     object
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
            self.sqla.__setattr__(name, value)
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
        self.sqla.__setattr__(name, value)

    def seralize(self):
        return cattr.unstructure(self)


def generate_sqla(cls):
    #2nd attribute is always the primary key
    typemap = \
        {
            int: BigInteger,
            str: String,
            float: Float,
            bytes: LargeBinary,
        }

    dst_dict = {"__tablename__": "dnodes"}
    for i, src_attr in enumerate(attr.fields(cls)):
        if i == 0:
            assert src_attr.name == "sqla"
            continue
        dst_args = {}
        dst_sq_type = typemap[src_attr.type]
        if i == 1:
            dst_args["primary_key"] = True

        dst_dict[src_attr.name] = Column(dst_sq_type, **dst_args)

    return type('SQADnode', (Base,), dst_dict)


SQADnode = generate_sqla(Dnode)


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


def add(sdn, session):
    session.add(sdn)  # previous sdn


def commit(sdn, session):
    session.add(sdn)  # previous sdn
    session.commit()  # commit it


def retrieve(inode, session):
    query = session.query(SQADnode).filter(SQADnode.inode == inode)
    sdn = query.one()
    #print("sdn:", sdn)
    #print("type(sdn):", type(sdn))
    return sdn


def sdn_to_dn(sdn):
    #print("sdn:", sdn)
    sdn_values = {a: getattr(sdn, a) for a in dir(sdn) if (a[0] != '_') and (a != 'metadata') and (a != "sqla")}
    dn = Dnode(sdn, **sdn_values)
    #import IPython; IPython.embed()
    return dn


async def reader(command, status, debug, exit_early, poolname, db_file, session, modify_existing):
    timestamp = time.time()
    pad = 25 * ' '
    marker = norm(b'    Object  lvl   iblk   dblk  dsize  dnsize  lsize   %full  type\n')
    found_marker = False
    found_id = False
    object_id = None
    line_num = 0
    count = 0
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

        #if not len(dnode_map.keys()) % 500000:
        #    if dnode_map.keys():
        #        if debug:
        #            eprint("saving:", db_file)
        #        dnode_map.sync()

        if found_marker:
            assert not found_id
            sline = line.split()
            if dn:  # save prev dn
                #eprint("saving object_id:", object_id)
                assert object_id is not None
                #commit(sdn, session)
                add(sdn, session)
                count += 1

                dn = None
                object_id = None

            if not dn:  # first marker
                assert not object_id
                object_id = int(sline.pop(0))
                if modify_existing:  # get pre-existing dn to mutate
                    sdn = retrieve(object_id, session)
                    dn = sdn_to_dn(sdn)
                else:
                    dn_type = str(b" ".join(sline[7:]), encoding='utf8')
                    sdn = SQADnode()
                    dn = Dnode(sdn, object_id, *sline[:7], dn_type)
                    count += 1

            if status:
                print_status(object_id, pad, count, timestamp)

            found_id = True
            found_marker = False
            continue

        if line == marker:
            found_marker = True

        if exit_early:
            if count >= exit_early:
                import warnings
                warnings.filterwarnings("ignore")
                eprint("\n\nExiting early after {0} id's".format(count))
                break

    commit(sdn, session)

    if status:
        sql_count = session.query(SQADnode).count()
        eprint("Done. count:{0} total:{0} dnode records saved in:\n{1}\n".format(count, sql_count, db_file))

    return


def create_session(poolname=None, db_file=None, debug=False):
    if not db_file:
        if poolname:
            db_file = generate_db_file(poolname)

    db_path = 'sqlite:///' + db_file
    ENGINE = create_engine(db_path, echo=bool(debug))
    Base.metadata.create_all(ENGINE)
    Session = sessionmaker(bind=ENGINE)
    return Session()


async def parse_zdb_dnodes(poolname, inodes, status, debug, exit_early):
    path_command =  ["zdb", poolname, "-L", "-dddd", "-v", "-P", "--"]  # need to skip 0 or takes forever   # noqa: E222
    command =       ["zdb", poolname, "-L", "-dddd", "-P"]              # wont get paths                    # noqa: E222
    db_file = generate_db_file(poolname)

    for inode in inodes:
        command.append(str(inode))

    session = create_session(poolname=poolname, db_file=None, debug=debug)

    await reader(command, status, debug, exit_early, poolname, db_file, session, modify_existing=False)

    file_dnodes = []
    file_dnodes = session.query(SQADnode).filter(SQADnode.type == "ZFS plain file")

    #debug = 2
    exit_early = False
    inodes = None

    file_dnodes = sorted([f.inode for f in file_dnodes], key=int)
    for group in grouper(file_dnodes, 3000):
        group = [str(g) for g in group if g]
        next_command = path_command + group
        await reader(next_command, status, debug, exit_early, poolname, db_file, session, modify_existing=True)

    #import IPython; IPython.embed()
    session.close()


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
@click.argument("db_file", type=click.Path(exists=True, dir_okay=False, allow_dash=True))
def load(db_file):
    debug = False
    session = create_session(poolname=None, db_file=db_file, debug=debug)
    p = session.query(SQADnode)
    #eprint("len(p):", len(p))

    from IPython import embed
    from traitlets.config import get_config
    c = get_config()
    c.InteractiveShellEmbed.colors = "Linux"
    embed(config=c)


if __name__ == "__main__":
    # print("python version:", sys.version)  # 3.7
    cli()
