from fastcdc.const import AVERAGE_MIN


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
    >>> ChunkSize(1)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    chunksum.chunksize.ChunkSizeError: chunk size too small: 1
    >>> ChunkSize(1024 + 1)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    chunksum.chunksize.ChunkSizeError: chunk size should be a multiple of 4, but 1025 % 5 = 1
    """  # noqa: E501

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
