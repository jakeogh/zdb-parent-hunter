#!/usr/bin/env python3

import sys
import os
import pickle
import time
import asyncio
from pathlib import Path
from asyncio.subprocess import PIPE
import click

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


async def run_command(args, verbose=False):
    process = await asyncio.create_subprocess_exec(*args, stdout=PIPE)
    async for line in process.stdout:
        yield line


def print_match(object_id, parent_id, pad):
    print(object_id, parent_id, end=pad + '\n')


def print_status(object_id, pad, count, start):
    rate = count / (time.time() - start)
    eprint("checking id:", object_id, "@", int(rate), "id/sec", end=pad + '\r', flush=True)


async def parse_zdb(poolname, parents, status, debug, exit_early):
    command = ["zdb", "-L", "-dddd", poolname]

    async def reader(command, parents, status, debug, exit_early):
        timestamp = time.time()
        data_dir = Path(os.path.expanduser("~/.zfs_parent_hunter"))
        data_dir.mkdir(exist_ok=True)
        data_file = Path("_".join(['parent_map', poolname, str(timestamp),
                                   str(os.getpid()), '.pickle']))
        pickle_file = data_dir / data_file
        pickle_file.parent.mkdir(exist_ok=True)

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


@click.command()
@click.argument("poolname", type=str, nargs=1)
@click.argument("parents", type=int, nargs=-1)
@click.option("--status", is_flag=True)
@click.option("--debug", is_flag=True)
@click.option("--exit-early", type=int, help="(for testing)")
def find_parents(poolname, parents, status, debug, exit_early):
    assert len(poolname.split()) == 1
    assert '/' in poolname
    if status:
        eprint("gathering all id->parent mappings")
        if parents:
            eprint("looking for id's with parent(s):", *parents)

    asyncio.run(parse_zdb(poolname, parents, status, debug, exit_early))


if __name__ == "__main__":
    # print("python version:", sys.version)  # 3.7
    find_parents()
