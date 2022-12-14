#!/usr/bin/env python
import re
import sys
from os.path import isdir

from tqdm.auto import tqdm

from .cdc import Chunker
from .chunksize import GIGA
from .chunksize import KILO
from .chunksize import MEGA
from .hash import hash_digest_size
from .iter import iter_file_content
from .utils import get_size
from .utils import get_total_size
from .utils import is_file_obj
from .utils import sorted_walk


UNITS = {
    "k": KILO,
    "m": MEGA,
    "g": GIGA,
}


def get_chunker(size_name="", avg=1024, min=256, max=4096):
    """
    >>> get_chunker('k0')
    <Chunker avg=1024, min=256.0, max=4096>
    >>> get_chunker('K9')
    <Chunker avg=524288, min=131072.0, max=2097152>
    >>> get_chunker('m2')
    <Chunker avg=4194304, min=1048576.0, max=16777216>
    >>> get_chunker('g1')
    <Chunker avg=2147483648, min=536870912.0, max=8589934592>
    >>> get_chunker('x1')  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    Exception: wrong unit or power of chunk size: x1
    >>> get_chunker('ka')  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    Exception: wrong unit or power of chunk size: ka
    """
    pattern = r"(?P<unit>k|m|g)(?P<power>\d)"
    mo = re.match(pattern, size_name, flags=re.IGNORECASE)
    if not mo:
        raise Exception(f"wrong unit or power of chunk size: {size_name}")

    groups = mo.groupdict()
    unit = groups["unit"]
    power = groups["power"]

    coefficient = UNITS[unit.lower()]
    size = coefficient * 2 ** int(power)
    return Chunker(size.avg, size.min, size.max)


def compute_file(file, alg_name="fck4sha2", bar_position=None):
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
    chunk_size_name = alg_name[2:4]
    chunker = get_chunker(chunk_size_name)
    result = []
    buffer_size = 4 * 1024 * 1024
    iter_ = iter_file_content(file, size=buffer_size, bar_position=bar_position)

    hasher_name = alg_name[len("fck0") :] or hash
    for data in iter_:
        chunker.update(data)
        for chunk in chunker.chunks:
            result.append(hash_digest_size(chunk, hasher_name))
    if chunker.tail:
        result.append(hash_digest_size(chunker.tail, hasher_name))
    return result


def list_hash(digests, hasher_name="sha2"):
    """
    >>> list_hash([b'abc', b'def'])
    b'\xbe\xf5...\xd9<G!'
    """
    plain = b"".join(digests)
    digest, _ = hash_digest_size(plain, hasher_name)
    return digest


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


def iter_lines(file):
    for line in file:
        yield line.strip("\n")


def iter_by_type(path):
    if path == sys.stdin:
        yield from iter_lines(sys.stdin)
    elif hasattr(sys.stdin, "buffer") and path == sys.stdin.buffer:
        yield path  # pragma: no cover
    elif isdir(path):
        yield from sorted_walk(path)
    else:
        yield path


def iter_files(paths):
    for path in paths:
        yield from iter_by_type(path)


def compute_stdin(file, output_file, alg_name="fck4sha2", skip_func=None):
    """
    >>> import io
    >>> pipe_in = io.BytesIO(b'hello')
    >>> sys.stdin = pipe_in  # hack stdin
    >>> compute_stdin(sys.stdin, sys.stdout)
    9595...3d50  <stdin>  fck4sha2!2cf2...9824:5
    """

    name = "<stdin>"
    chunks = compute_file(file, alg_name)
    print(
        format_a_result(name, chunks, alg_name),
        file=output_file,
        flush=True,
    )


def compute(paths, output_file, alg_name="fck4sha2", skip_func=None):
    """
    >>> import sys
    >>> compute([], sys.stdout)
    """
    if not paths:
        return

    if (
        is_file_obj(paths[0])
        and hasattr(sys.stdin, "buffer")
        and paths[0] == sys.stdin.buffer
    ):
        return compute_stdin(paths[0], output_file, alg_name, skip_func)

    total = sum([get_total_size(path) for path in paths])

    pbar = tqdm(
        desc="chunksum",
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        dynamic_ncols=True,
    )

    def update_progress(size):
        pbar.update(size)

    walk(iter_files(paths), output_file, update_progress, alg_name, skip_func)


def walk(iter, output_file, update_progress=None, alg_name="fck4sha2", skip_func=None):
    """
    # check a directory
    >>> import os.path
    >>> import sys
    >>> import tempfile
    >>> dir = tempfile.TemporaryDirectory()
    >>> path = os.path.join(dir.name, 'testfile')
    >>> _ = open(path, 'wb').write(b'hello')
    >>> walk(sorted_walk(dir.name), sys.stdout)
    9595...3d50  .../testfile  fck4sha2!2cf2...9824:5

    # check a file
    >>> walk([path], sys.stdout)
    9595...3d50  .../testfile  fck4sha2!2cf2...9824:5

    # skip files
    >>> skip_func=lambda x: x.endswith('testfile')
    >>> walk(sorted_walk(dir.name), sys.stdout, None, skip_func=skip_func)
    """

    for path in iter:
        compute_one_file(path, output_file, update_progress, alg_name, skip_func)


def compute_one_file(
    path,
    output_file,
    update_progress=None,
    alg_name="fck4sha2",
    skip_func=None,
    bar_position=None,
):
    """
    >>> import sys
    >>> compute_one_file('/tmp/nothing', sys.stdout)
    """
    size = get_size(path)
    if skip_func and skip_func(path):
        update_progress and update_progress(size)
        return
    try:
        file = open(path, "rb")
    except (PermissionError, FileNotFoundError):
        update_progress and update_progress(size)
        return
    chunks = compute_file(file, alg_name, bar_position=bar_position)
    sums = format_a_result(path, chunks, alg_name)
    if output_file:
        print(sums, file=output_file, flush=True)
    update_progress and update_progress(size)
    return sums
