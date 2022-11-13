import sys

from .chunksum import walk


def help():
    print(
        """Print FastCDC rolling hash chunks and checksums.

Usage: {cmd} <dir> [<alg_name>] > chunksums

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

Examples:

  $ {cmd} /etc > ~/etc.chunksums

  $ {cmd} ~/Videos fcm4blake2b32 > ~/Videos/chunksums
""".format(
            cmd=sys.argv[0],
        ),
    )


def main():
    """
    >>> sys.argv = ['chunksup']
    >>> main()  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Print ...
    Usage: ...
    ...

    >>> import tempfile
    >>> import os.path
    >>> dir = tempfile.TemporaryDirectory()
    >>> path = os.path.join(dir.name, 'testfile')
    >>> _ = open(path, 'wb').write(b'hello')
    >>> sys.argv = ['chunksum', dir.name]
    >>> main()
    9595...3d50  .../testfile  fck4sha2!2cf2...9824:5
    >>> sys.argv = ['chunksum', dir.name, 'fcm0blake2b32']
    >>> main()
    901c...ce59  .../testfile  fcm0blake2b32!324d...72cf:5
    >>> sys.argv = ['chunksum', dir.name, 'fcm0blake2s']
    >>> main()
    8d95...5ee5  .../testfile  fcm0blake2s!1921...ca25:5
    """
    if len(sys.argv) == 1:
        help()
    else:
        if len(sys.argv) > 2:
            path, alg_name = sys.argv[1:3]
        else:
            path, alg_name = sys.argv[1], "fck4sha2"
        walk(path, sys.stdout, alg_name)


if __name__ == "__main__":
    main()  # pragma: no cover
