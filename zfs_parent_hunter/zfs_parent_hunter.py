#!/usr/bin/env python3

import sys
import subprocess
from math import inf
import click

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def run_command(command, verbose=False, shell=True, expected_exit_code=0, stdin=None, ignore_exit=False):
    output = ''
    if verbose:
        eprint("command:", '`' + command + '`')
        eprint("shell:", shell)
    try:
        output = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=shell, stdin=stdin)
        if verbose:
            eprint("output:", output.decode('utf8'))
    except subprocess.CalledProcessError as error:
        if verbose:
            eprint("exit code:", error.returncode, error.output)
        if error.returncode == expected_exit_code:
            return output
        elif ignore_exit:
            return output
        eprint("command:", command)
        eprint("exit code:", error.returncode, error.output)
        raise error

    return output


@click.command()
@click.argument("fs", type=str, nargs=1)
@click.argument("parents", type=int, nargs=-1)
@click.option("--start", type=int, default=0)
@click.option("--end", default=inf)
@click.option("--verbose", is_flag=True)
@click.option("--debug", is_flag=True)
def find_parents(fs, parents, start, end, verbose, debug):
    assert len(fs.split()) == 1
    if verbose:
        eprint("looking for parent(s):", parents)
    obj_id = start
    while obj_id <= end:
        if verbose:
            pad = 25 * ' ' + '\r'
            eprint("checking id:", obj_id, end=pad, flush=True)
        command = " ".join(["zdb", "-ddddd", fs, str(obj_id)])
        output = run_command(command, shell=True, verbose=debug, ignore_exit=True)
        for line in output.splitlines():
            line = line.decode('utf8')
            if '\tparent\t' in line:
                parent_id = int(line.split()[-1])
                if debug: eprint("parent_id:", parent_id)
                if parent_id in parents:
                    if verbose:
                        print("id:", obj_id, "parent:", parent_id)
                    else:
                        print(obj_id)
        if debug:
            eprint(obj_id, "len(output):", len(output))
            #eprint(output)
        obj_id += 1


if __name__ == "__main__":
    find_parents()

