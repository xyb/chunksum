from os.path import getsize

from tqdm.auto import tqdm
from tqdm.utils import CallbackIOWrapper

from .utils import get_tqdm_limited_desc


def iter_file_content(file, size=1024, bar_position=None):
    if hasattr(file, "name") and file.name != "<stdin>":
        yield from _iter_file_content_progress(
            file,
            file.name,
            size=size,
            bar_position=bar_position,
        )
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


def _iter_file_content_progress(file, path, size=1024, bar_position=None):
    kwargs = dict(
        total=getsize(path),
        desc=get_tqdm_limited_desc(path),
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        delay=0.5,  # display only if the file is really large
    )
    if bar_position is not None:
        kwargs["position"] = bar_position
    with tqdm(**kwargs) as t:
        fobj = CallbackIOWrapper(t.update, file, "read")
        yield from _iter_file_content(fobj, size)
