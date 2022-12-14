import argparse
import sys
from os.path import exists

from .chunksum import compute
from .mp import compute_mp
from .parser import parse_chunksums

command_desc = "Print FastCDC rolling hash chunks and checksums."
command_long_desc = """
alg-name:
  Format "fc[k|m|g][0-9][sha2|blake2b|blake2s][32]".

  For example, "fck4sha2", means using FastCDC("fc") with an
  average chunk size of 2**8=256KB("k8") and using sha256("sha2")
  to calculate the checksum.

  "fcm4blake2b32" means using FastCDC with an average chunk size
  of 2**4=16MB("m4") and using "blake2b" to calculate and output
  a checksum of length "32" bytes(save storage).

  For large files, you may using large chunk size, such as "m4",
  to reduce the number of chunks.

  (default: fck4sha2)

chunksums-file and incr-file:
  You can specify the previous chunksums file if you want to
  resume a previous check, or if you want to find the incremental
  updates (new files) of the directory.

Examples:

  $ %(prog)s /etc > ~/etc.chunksums

  $ %(prog)s -n fcm4blake2b32 -m ~/Videos

  $ %(prog)s -n fcm4blake2b32 -f ~/Videos/chunksums ~/Videos

  $ %(prog)s -n fcm4blake2b32 -f ~/chunksums -i ~/chunksums.incr ~/Videos
"""


def included_in_chunksums(chunksums_file):
    sums = parse_chunksums(chunksums_file)
    paths = {x["path"] for x in sums}

    def included(path):
        return path in paths

    return included


def main():
    """
    # help
    >>> sys.argv = ['chunksum']
    >>> try:
    ...   main()  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    ... except:
    ...   pass
    usage: chunksum ...
    Print ...
    ...

    # compute chunksums
    >>> import tempfile
    >>> import os.path
    >>> dir = tempfile.TemporaryDirectory()
    >>> file1 = os.path.join(dir.name, 'testfile')
    >>> _ = open(file1, 'wb').write(b'hello')
    >>> sys.argv = ['chunksum', '-f', '-', dir.name]  # output to stdout
    >>> main()
    9595...3d50  .../testfile  fck4sha2!2cf2...9824:5
    >>> sys.argv = ['chunksum', '-n', 'fcm0blake2b32', '-f', '-', dir.name]
    >>> main()
    901c...ce59  .../testfile  fcm0blake2b32!324d...72cf:5
    >>> sys.argv = ['chunksum', '-n', 'fcm0blake2s', '-f', '-', dir.name]
    >>> main()
    8d95...5ee5  .../testfile  fcm0blake2s!1921...ca25:5
    >>> dir2 = tempfile.TemporaryDirectory()
    >>> chunksums = os.path.join(dir2.name, 'chunksums')
    >>> sys.argv = ['chunksum', '-f', chunksums, dir.name]  # output to a file
    >>> main()

    # incremental / skip file
    >>> chunksums = tempfile.NamedTemporaryFile()
    >>> sys.argv = ['chunksum', '-f', chunksums.name, dir.name]
    >>> main()
    >>> file2 = os.path.join(dir.name, 'newfile')
    >>> _ = open(file2, 'wb').write(b'world')
    >>> incr = chunksums.name + '.incr'
    >>> sys.argv = ['chunksum', '-f', chunksums.name, '-i', '-', dir.name]
    >>> main()
    63...06  .../newfile  fck4sha2!48...a7:5
    >>> sys.argv = ['chunksum', '-f', chunksums.name, '-i', incr, dir.name]
    >>> main()
    >>> open(incr).read().strip()
    '63...06  .../newfile  fck4sha2!48...a7:5'

    # resume
    >>> sys.argv = ['chunksum', '-f', chunksums.name, dir.name]
    >>> main()
    >>> for line in open(chunksums.name).readlines():
    ...   print(line.strip())
    95...50  .../testfile  fck4sha2!2c...24:5
    63...06  .../newfile  fck4sha2!48...a7:5

    # compute files
    >>> chunksums = tempfile.NamedTemporaryFile()
    >>> sys.argv = ['chunksum', '-f', chunksums.name, file1, file2]
    >>> main()
    >>> for line in open(chunksums.name).readlines():
    ...   print(line.strip())
    95...50  .../testfile  fck4sha2!2c...24:5
    63...06  .../newfile  fck4sha2!48...a7:5

    # read file or dir list from stdin, consumer mode
    >>> import io
    >>> pipe_in = io.StringIO(file1 + '\\n' + file2 + '\\n')
    >>> sys.stdin = pipe_in  # hack stdin
    >>> from . import chunksum
    >>> chunksum.sys.stdin = pipe_in
    >>> chunksums = tempfile.NamedTemporaryFile()
    >>> sys.argv = ['chunksum', '-f', chunksums.name, '-x', '-']
    >>> main()
    >>> for line in open(chunksums.name).readlines():
    ...   print(line.strip())
    95...50  .../testfile  fck4sha2!2c...24:5
    63...06  .../newfile  fck4sha2!48...a7:5

    # read content from stdin
    >>> pipe_in = io.BytesIO(b'hello')
    >>> sys.stdin.buffer = pipe_in  # hack stdin
    >>> sys.argv = ['chunksum', '-f', '-', '-']
    >>> main()
    95...50  <stdin>  fck4sha2!2c...24:5

    # multi-process
    >>> chunksums = tempfile.NamedTemporaryFile()
    >>> sys.argv = ['chunksum', '-f', chunksums.name, '-m', dir.name]
    >>> main()
    >>> for line in sorted(open(chunksums.name).readlines()):
    ...   print(line.strip())
    63...06  .../newfile  fck4sha2!48...a7:5
    95...50  .../testfile  fck4sha2!2c...24:5
    """
    parser = argparse.ArgumentParser(
        description=command_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=command_long_desc,
    )
    parser.add_argument(
        "-n",
        "--alg-name",
        default="fck4sha2",
        help="chunksum algorithm name.",
    )
    parser.add_argument(
        "-f",
        "--chunksums-file",
        default="chunksums",
        help="chunksum file path, `-' for standard output.",
    )
    parser.add_argument(
        "-i",
        "--incr-file",
        help="incremental updates file path",
    )
    parser.add_argument(
        "-m",
        "--multi-process",
        action="store_true",
        help="same number of multi-processes as cpu",
    )
    parser.add_argument(
        "-x",
        "--consumer-mode",
        action="store_true",
        help=argparse.SUPPRESS,  # get paths from stdin, but hide this command
    )
    parser.add_argument("path", nargs="*", help="path to compute chunksums")
    args = parser.parse_args()

    no_multiprocess = False
    paths = args.path
    if args.consumer_mode:
        paths = [sys.stdin]
        no_multiprocess = True
    elif len(paths) == 1 and paths[0] == "-":
        # check input chunksums
        paths = [sys.stdin.buffer]
        no_multiprocess = True

    if not paths:
        parser.print_help()
        sys.exit()

    skip_func = None
    if exists(args.chunksums_file):
        skip_func = included_in_chunksums(open(args.chunksums_file))

    if args.chunksums_file == "-" or args.incr_file == "-":
        output_file = sys.stdout
    elif args.incr_file:
        output_file = open(args.incr_file, "a")
    elif exists(args.chunksums_file):
        output_file = open(args.chunksums_file, "a")
    else:
        output_file = open(args.chunksums_file, "w")

    if args.multi_process and not no_multiprocess:
        compute_mp(
            paths,
            output_file,
            args.alg_name,
            skip_func=skip_func,
        )
    else:
        compute(
            paths,
            output_file,
            args.alg_name,
            skip_func=skip_func,
        )


if __name__ == "__main__":
    main()  # pragma: no cover
