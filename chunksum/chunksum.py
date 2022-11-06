#!/usr/bin/env python
import os
import sys
from hashlib import blake2b
from hashlib import blake2s
from hashlib import sha256
from os.path import getsize
from os.path import join

from fastcdc.const import AVERAGE_MIN
from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper

from .cdc import Chunker


def iter_file_content(file, size=1024):
    """
    >>> import io
    >>> stream = io.StringIO('abcdefg')
    >>> list(iter_file_content(stream, size=3))
    ['abc', 'def', 'g']
    """

    while True:
        content = file.read(size)
        if not content:
            break
        yield content


def iter_file_content_progress(file, path, size=1024):
    with tqdm(
        total=getsize(path),
        desc=path,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        delay=1.0,
    ) as t:
        fobj = CallbackIOWrapper(t.update, file, "read")
        yield from iter_file_content(fobj, size)


class ChunkSizeError(Exception):
    ...


class ChunkSizeTooSmall(ChunkSizeError):
    ...


class ChunkSizeAlign(ChunkSizeError):
    ...


class ChunkSize:
    """
    >>> ChunkSize(1024)
    ChunkSize<1024>
    """

    def __init__(self, avg_bytes=AVERAGE_MIN):
        if (avg_bytes) < AVERAGE_MIN:
            raise ChunkSizeError(f"chunk size too small: {avg_bytes}")
        if avg_bytes % 4 != 0:
            raise ChunkSizeError(
                "chunk size should be a multiple of 4, "
                "but {} % 5 = {}".format(
                    avg_bytes,
                    avg_bytes % 4,
                ),
            )
        self.avg = avg_bytes
        self.min = avg_bytes / 4
        self.max = avg_bytes * 4

    def __repr__(self):
        """
        >>> ChunkSize(64 * 1024)
        ChunkSize<65536>
        """
        return f"ChunkSize<{self.avg}>"

    def __mul__(self, x):
        """
        >>> ChunkSize(1024) * 64
        ChunkSize<65536>
        """
        return ChunkSize(self.avg * x)

    def __rmul__(self, x):
        """
        >>> 2 * ChunkSize(1024)
        ChunkSize<2048>
        """
        return ChunkSize(self.avg * x)


KILO = ChunkSize(1024)  # 1KB
MEGA = KILO * 1024  # 1MB
GIGA = MEGA * 1024  # 1GB


def get_chunker(size_name="", avg=1024, min=256, max=4096):
    if size_name and len(size_name) == 2:
        unit, power = size_name
        coefficient = {"k": KILO, "m": MEGA, "g": GIGA}.get(unit.lower())
        if not coefficient:
            raise Exception(f"wrong unit of chunk size: {unit}")
        if not power.isdigit():
            raise Exception(f"chunk size is not a number: {power}")
        size = coefficient * 2 ** int(power)
        return Chunker(size.avg, size.min, size.max)
    else:
        return Chunker(avg, min, max)


def get_hasher(name):
    name = name.lower()
    if name == "sha2":
        return sha256()
    elif name.startswith("blake2"):
        prefix = name[: len("blake2b")]
        digest_size = name[len("blake2b") :]
        if prefix == "blake2b":
            func = blake2b
        elif prefix == "blake2s":
            func = blake2s
        else:
            raise Exception(f"unsupported blake2 hash: {prefix}")
        if digest_size:
            return func(digest_size=int(digest_size))
        else:
            return func()
    else:
        raise Exception(f"unsupported hash: {name}")


def compute_file(file, alg_name="fck4sha2", avg=0, min=0, max=0):
    """

    >>> import io
    >>> stream = io.BytesIO(b'abcdefgh' * 20000)
    >>> result = compute_file(stream, alg_name='fck4sha2')
    >>> for i in result:
    ...     print(i)
    (b'\\xfb...\\xd3', 65536)
    (b'\\xfb...\\xd3', 65536)
    (b'tG...\\xfe', 28928)
    """
    if alg_name:
        chunk_size_name = alg_name[2:4]
        chunker = get_chunker(chunk_size_name)
    else:
        chunker = Chunker(avg=avg, min=min, max=max)
    result = []
    buffer_size = 4 * 1024 * 1024
    if hasattr(file, "name"):
        iter_ = iter_file_content_progress(file, file.name, size=buffer_size)
    else:
        iter_ = iter_file_content(file, size=buffer_size)

    def gen_item(data, hasher_name):
        size = len(data)
        h = get_hasher(hasher_name)
        h.update(data)
        return (h.digest(), size)

    hasher_name = alg_name[len("fck0") :]
    for data in iter_:
        chunker.update(data)
        for chunk in chunker.chunks:
            result.append(gen_item(chunk, hasher_name))
    if chunker.tail:
        result.append(gen_item(chunker.tail, hasher_name))
    return result


def list_hash(digests, hasher_name="sha2"):
    """
    >>> list_hash([b'abc', b'def'])
    b'\xbe\xf5...\xd9<G!'
    """
    plain = b"".join(digests)
    h = get_hasher(hasher_name)
    h.update(plain)
    return h.digest()


def format_a_result(path, result, alg_name):
    """
    >>> import io
    >>> stream = io.BytesIO(b'abcdefgh' * 20000)
    >>> result = compute_file(stream, alg_name='fck4sha2')
    >>> format_a_result('example', result, 'fck4sha2')
    '82...b7  example  fck4sha2!fb...d3:65536,fb...d3:65536,74...fe:28928'
    """
    chunks = ",".join([f"{digest.hex()}:{size}" for digest, size in result])
    hasher_name = alg_name[len("fck0") :]
    digest = list_hash([d for d, _ in result], hasher_name)
    # alg_name = 'fastcdc-{}-{}-{}-sha256'.format(AVG, MIN, MAX)
    return f"{digest.hex()}  {path}  {alg_name}!{chunks}"


def walk(target, output_file, alg_name="fck4sha2"):
    for root, dirs, files in os.walk(target):
        for file in sorted(files):
            path = join(root, file)
            chunks = compute_file(open(path, "rb"), alg_name)
            print(
                format_a_result(path, chunks, alg_name),
                file=output_file,
                flush=True,
            )
        dirs.sort()


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
    if len(sys.argv) == 1:
        help()
        sys.exit()
    if len(sys.argv) > 2:
        path, alg_name = sys.argv[1:3]
    else:
        path, alg_name = sys.argv[1], "fck4sha2"
    walk(path, sys.stdout, alg_name)


if __name__ == "__main__":
    main()
