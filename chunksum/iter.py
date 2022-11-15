from os.path import getsize

from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper

from .utils import get_tqdm_limited_desc


def iter_file_content(file, size=1024):
    if hasattr(file, "name"):
        yield from _iter_file_content_progress(file, file.name, size=size)
    else:
        yield from _iter_file_content(file, size=size)


def _iter_file_content(file, size=1024):
    """
    >>> import io
    >>> stream = io.StringIO('abcdefg')
    >>> list(_iter_file_content(stream, size=3))
    ['abc', 'def', 'g']
    """

    while True:
        content = file.read(size)
        if not content:
            break
        yield content


def _iter_file_content_progress(file, path, size=1024):
    with tqdm(
        total=getsize(path),
        desc=get_tqdm_limited_desc(path),
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        delay=1.0,
    ) as t:
        fobj = CallbackIOWrapper(t.update, file, "read")
        yield from _iter_file_content(fobj, size)
