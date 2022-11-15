import sys

from .chunksum import get_total_size
from .chunksum import walk
from .parser import parse_chunksums


def help():
    print(
        """Print FastCDC rolling hash chunks and checksums.

Usage: {cmd} <dir> [<alg_name>] [<prev_chunksums_file>]

alg_name:
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

prev_chunksums_file:
  You can specify the previous chunksums file if you want to
  resume a previous check, or if you want to find the incremental
  updates (new files) of the directory.


Examples:

  $ {cmd} /etc > ~/etc.chunksums

  $ {cmd} ~/Videos fcm4blake2b32 > ~/Videos/chunksums

  $ {cmd} ~/Videos fcm4blake2b32 ~/chunksums > ~/chunksums.incr
""".format(
            cmd=sys.argv[0],
        ),
    )


def included_in_chunksums(chunksums_file):
    sums = parse_chunksums(chunksums_file)
    paths = {x["path"] for x in sums}

    def included(path):
        return path in paths

    return included


def main():
    """
    # help
    >>> sys.argv = ['chunksup']
    >>> main()  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Print ...
    Usage: ...
    ...

    # compute chunksums
    >>> import tempfile
    >>> import os.path
    >>> dir = tempfile.TemporaryDirectory()
    >>> file1 = os.path.join(dir.name, 'testfile')
    >>> _ = open(file1, 'wb').write(b'hello')
    >>> sys.argv = ['chunksum', dir.name]
    >>> main()
    9595...3d50  .../testfile  fck4sha2!2cf2...9824:5
    >>> sys.argv = ['chunksum', dir.name, 'fcm0blake2b32']
    >>> main()
    901c...ce59  .../testfile  fcm0blake2b32!324d...72cf:5
    >>> sys.argv = ['chunksum', dir.name, 'fcm0blake2s']
    >>> main()
    8d95...5ee5  .../testfile  fcm0blake2s!1921...ca25:5

    # skip files
    >>> file2 = os.path.join(dir.name, 'newfile')
    >>> _ = open(file2, 'wb').write(b'hello')
    >>> chunksums = tempfile.NamedTemporaryFile()
    >>> _ = open(chunksums.name, 'w').write(f'sum  {file1}  fck4sha2!')
    >>> sys.argv = ['chunksum', dir.name, 'fck4sha2', chunksums.name]
    >>> main()
    9595...3d50  .../newfile  fck4sha2!2cf2...9824:5
    """
    if len(sys.argv) == 1:
        help()
        return

    skip_func = None
    if len(sys.argv) > 3:
        path, alg_name, prev_version_chunksums = sys.argv[1:4]
        skip_func = included_in_chunksums(open(prev_version_chunksums))
    if len(sys.argv) > 2:
        path, alg_name = sys.argv[1:3]
    else:
        path, alg_name = sys.argv[1], "fck4sha2"
    total = get_total_size(path)
    walk(path, sys.stdout, alg_name, skip_func=skip_func, total=total)


if __name__ == "__main__":
    main()  # pragma: no cover
