#!/usr/bin/env python3

import sys
import os
import pickle
import time
import asyncio
import attr
from pathlib import Path
from asyncio.subprocess import PIPE
import click


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


async def run_command(args):
    process = await asyncio.create_subprocess_exec(*args, stdout=PIPE)
    async for line in process.stdout:
        yield line


def print_match(object_id, parent_id, pad):
    print(object_id, parent_id, end=pad + '\n')


def print_status(object_id, pad, count, start):
    rate = count / (time.time() - start)
    eprint("checking id:", object_id, "@", int(rate), "id/sec", end=pad + '\r', flush=True)


def generate_pickle_file(note, poolname, timestamp):
    data_dir = Path(os.path.expanduser("~/.zfs_index"))
    data_dir.mkdir(exist_ok=True)
    data_file = Path("_".join([note, poolname, str(timestamp), str(os.getpid()), '.pickle']))
    pickle_file = data_dir / data_file
    pickle_file.parent.mkdir(exist_ok=True)
    return pickle_file


async def parse_zdb_parents(poolname, parents, status, debug, exit_early):
    command = ["zdb", "-L", "-dddd", poolname]

    async def reader(command, parents, status, debug, exit_early):
        timestamp = int(time.time())
        pickle_file = generate_pickle_file('parent_map', poolname, timestamp)
        parent_map = {}
        pad = 25 * ' '
        marker = b'    Object  lvl   iblk   dblk  dsize  dnsize  lsize   %full  type\n'
        parent_marker = b'\tparent\t'
        found_marker = False
        found_id = False
        object_id = None
        async for line in run_command(command):
            if found_id:
                assert not found_marker
                if line == marker:  # no parent
                    found_marker = True
                    found_id = False
                    parent_map[object_id] = None
                    continue
                if line.startswith(parent_marker):
                    parent_id = int(line.split(parent_marker)[-1].strip())
                    found_id = False
                    assert object_id not in parent_map.keys()
                    parent_map[object_id] = parent_id
                    if parent_id in parents:
                        print_match(object_id, parent_id, pad)

                    if not (len(parent_map.keys()) % 40000):
                        if parent_map.keys():
                            if debug:
                                eprint("saving:", pickle_file)
                            with open(pickle_file, 'wb') as fh:
                                pickle.dump(parent_map, fh)

            if found_marker:
                assert not found_id
                object_id = int(line.split()[0])
                if status:
                    lpm = len(parent_map)
                    print_status(object_id, pad, lpm, timestamp)
                found_id = True
                found_marker = False
                continue
            if line == marker:
                found_marker = True

            if exit_early:
                lpm = len(parent_map)
                if lpm >= exit_early:
                    import warnings
                    warnings.filterwarnings("ignore")
                    eprint("\n\nExiting early after {0} id's".format(lpm))
                    break

        with open(pickle_file, 'wb') as fh:
            pickle.dump(parent_map, fh)

        if status:
            map_count = len(parent_map)
            all_parents = [p for p in parent_map.values() if p]
            unique_parents = set(all_parents)
            none_count = map_count - len(all_parents)
            with_parent_count = map_count - none_count
            assert with_parent_count == len(all_parents)
            eprint("Done. {0} id->parent mappings saved in:\n{1}\n".format(map_count, pickle_file))
            eprint("# of id's:", map_count)
            eprint("# of id's with no parent:", none_count)
            eprint("# of id's with parent:", with_parent_count)
            eprint("# of unique parents:", len(unique_parents))

    await reader(command, parents, status, debug, exit_early)


@attr.s(auto_attribs=True)
class dmu_dnode():
    offset: int
    level: int
    dva: dict
    checksum: str
    compress: str
    encryption: bool
    size: str
    birth: str
    fill: int


#" 0      L0 DVA[0]=<0:11c7909e200:800> DVA[1]=<0:12c51e31200:800> [L0 DMU dnode] fletcher4 lz4 unencrypted"
#"LE contiguous unique double size=4000L/800P birth=492396L/492396P fill=16"


async def parse_zdb_l0(poolname, status, debug, exit_early):
    command = ["zdb", "-L", "-ddddbbbbvv", poolname, "0"]
    if debug: eprint(command)
    async def reader(command, status, debug, exit_early):
        timestamp = int(time.time())
        pickle_file = generate_pickle_file('L0_list', poolname, timestamp)
        node_list = []
        pad = 25 * ' '
        marker = b'[L0 DMU dnode]'
        async for line in run_command(command):
            if debug:
                eprint(line)
            if b"L0 HOLE" in line:
                continue
            if marker in line:
                line = line.split()
                dva1 = line[2].split(b'<')[-1][:-1]
                dva2 = line[2].split(b'<')[-1][:-1]
                d = dmu_dnode(
                    offset=line[0],
                    level=line[1][-1],
                    dva={0: dva1, 1: dva2},
                    checksum=line[7],
                    compress=line[8],
                    encryption=(True if line[9] == b'encrypted' else False),
                    size=line[14].split(b'=')[-1],
                    birth=line[15].split(b'=')[-1],
                    fill=line[16].split(b'=')[-1])

                node_list.append(d)
                if status and not debug:
                    lpm = len(node_list)
                    print_status(d.offset, pad, lpm, timestamp)

        with open(pickle_file, 'wb') as fh:
            pickle.dump(node_list, fh)

        if status:
            dnode_count = len(node_list)
            eprint("Done. {0} L0 dnodes saved in:\n{1}\n".format(dnode_count, pickle_file))

    await reader(command, status, debug, exit_early)


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument("poolname", type=str, nargs=1)
@click.option("--no-status", is_flag=True)
@click.option("--debug", is_flag=True)
@click.option("--exit-early", type=int, help="(for testing)")
def dmu_dnode_L0(poolname, no_status, debug, exit_early):
    status = not no_status
    assert len(poolname.split()) == 1
    assert '/' in poolname
    if status:
        eprint("gathering all L0 entries from DMU dnode")

    asyncio.run(parse_zdb_l0(poolname, status, debug, exit_early))


@cli.command()
@click.argument("poolname", type=str, nargs=1)
@click.argument("parents", type=int, nargs=-1)
@click.option("--no-status", is_flag=True)
@click.option("--debug", is_flag=True)
@click.option("--exit-early", type=int, help="(for testing)")
def parents(poolname, parents, no_status, debug, exit_early):
    status = not no_status
    assert len(poolname.split()) == 1
    assert '/' in poolname
    if status:
        eprint("gathering all id->parent mappings")
        if parents:
            eprint("looking for id's with parent(s):", *parents)

    asyncio.run(parse_zdb_parents(poolname, parents, status, debug, exit_early))


if __name__ == "__main__":
    # print("python version:", sys.version)  # 3.7
    cli()


#" 0      L0 DVA[0]=<0:11c7909e200:800> DVA[1]=<0:12c51e31200:800> [L0 DMU dnode] fletcher4 lz4 unencrypted"
#"LE contiguous unique double size=4000L/800P birth=492396L/492396P fill=16"
#"zdb poolz0_2TB_A/iridb_data_index -vvvv 0 -dddddbbbbb | wc -l"
#"3628520"
