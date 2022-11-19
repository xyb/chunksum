import os
import sys
from os.path import isdir
from os.path import join

from tqdm.auto import tqdm
from tqdm.utils import _screen_shape_wrapper


def get_screen_width(fd=sys.stdout):
    """
    >>> get_screen_width(None)
    (None, None)
    """
    dynamic = _screen_shape_wrapper()
    return dynamic(fd)


def get_tqdm_limited_desc(desc, fd=sys.stdout):
    """
    >>> get_tqdm_limited_desc(str(list(range(100))), None)
    '...93, 94, 95, 96, 97, 98, 99]'
    """
    default_screen_width = 80
    reserve_size_for_tqdm = 50

    width = get_screen_width()
    if width and width[0]:
        cols = width[0]  # pragma: no cover
    else:
        cols = default_screen_width
    desc_limit = cols - reserve_size_for_tqdm
    if len(desc) > desc_limit:
        return f"...{desc[3 - desc_limit: ]}"
    else:
        return desc


def is_file_obj(file):
    return hasattr(file, "read")


def get_size(path):
    """
    # normal file
    >>> import tempfile
    >>> dir = tempfile.TemporaryDirectory()
    >>> file1 = os.path.join(dir.name, 'testfile')
    >>> _ = open(file1, 'wb').write(b'hello')
    >>> get_size(file1)
    5

    # not exists
    >>> import os
    >>> file2 = os.path.join(dir.name, 'none')
    >>> get_size(file2)
    0
    """
    if os.path.exists(path):
        return os.path.getsize(path)
    else:
        return 0


def get_total_size(path):
    """
    >>> import tempfile
    >>> import os.path
    >>> dir = tempfile.TemporaryDirectory()
    >>> file1 = os.path.join(dir.name, 'testfile')
    >>> _ = open(file1, 'wb').write(b'hello')
    >>> get_total_size(file1)
    5
    >>> get_total_size(dir.name)
    5
    >>> get_total_size(sys.stdin)
    0
    """
    if is_file_obj(path):
        return 0
    if isdir(path):
        return get_total_size_dir(path)
    return get_size(path)


def get_total_size_dir(dir):
    total = 0
    with tqdm(desc="get total file size", delay=0.5) as t:
        for root, dirs, files in os.walk(dir):
            for file in files:
                path = join(root, file)
                total += get_size(path)
                t.update()
    return total


def sorted_walk(dir):
    for root, dirs, files in os.walk(dir):
        for file in sorted(files):
            path = join(root, file)
            yield path
        dirs.sort()
