import re
from hashlib import blake2b
from hashlib import blake2s
from hashlib import sha256


HASH_FUNCTIONS = {
    "sha2": sha256,
    "blake2b": blake2b,
    "blake2s": blake2s,
}


def get_hasher(name):
    """
    >>> get_hasher('sha2')
    <sha256 ...>
    >>> get_hasher('blake2b')
    <_blake2.blake2b ...>
    >>> get_hasher('blake2b32')
    <_blake2.blake2b ...>
    >>> get_hasher('blake2s')
    <_blake2.blake2s ...>
    >>> get_hasher('badname')  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    Exception: unsupported hash name: badname
    >>> get_hasher('blake2x')  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    Exception: unsupported hash name: blake2x
    >>> get_hasher('blake2')  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    Exception: unsupported hash name: blake2
    >>> get_hasher('sha256')  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    Exception: unsupported hash name: sha256
    """
    name = name.lower()
    pattern = r"(?P<hash_name>sha2|blake2b|blake2s)(?P<digest_size>\d+)?"

    mo = re.match(pattern, name)
    if not mo:
        raise Exception(f"unsupported hash name: {name}")

    groups = mo.groupdict()
    hash_name = groups["hash_name"]
    digest_size = groups["digest_size"]

    if hash_name == "sha2" and digest_size:
        raise Exception(f"unsupported hash name: {name}")

    func = HASH_FUNCTIONS[hash_name]
    if digest_size:
        return func(digest_size=int(digest_size))
    else:
        return func()


def hash_digest_size(data, hasher_name):
    size = len(data)
    h = get_hasher(hasher_name)
    h.update(data)
    return (h.digest(), size)
