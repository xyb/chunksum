import io
from typing import List

from fastcdc import fastcdc

CHUNKER_AVG_CHUNK_SIZE = 8192


_default_chunker = None


def get_chunk_func(
    avg_chunk_size: int = CHUNKER_AVG_CHUNK_SIZE,
    min_chunk_size: int = None,
    max_chunk_size: int = None,
) -> callable:
    def chunk_func(stream):
        return fastcdc(
            stream,
            min_size=min_chunk_size,
            avg_size=avg_chunk_size,
            max_size=max_chunk_size,
            fat=True,
            hf=None,
        )

    return chunk_func


class Chunker:
    """Chunker of CDC(Content-defined Chunking).

    >>> c = Chunker(256)
    >>> list(c.chunks)
    []
    >>> content = b' '.join(b'%5d' % i for i in range(100))
    >>> chunks = list(c.update(content).chunks)
    >>> chunks
    [b'    0...45    4', b'6    47...74    ']
    >>> c.tail
    b'75    76...98    99'
    >>> whole = b''.join(chunks + [c.tail])
    >>> assert content == whole
    >>> _ = c.reset()
    >>> # incremental update
    >>> part1, part2 = content[:300], content[300:]
    >>> chunk1 = list(c.update(part1).chunks)
    >>> chunk2 = list(c.update(part2).chunks)
    >>> assert chunk1 + chunk2 == chunks
    """

    def __init__(
        self,
        avg_chunk_size: int = CHUNKER_AVG_CHUNK_SIZE,
        min_chunk_size: int = None,
        max_chunk_size: int = None,
    ):
        self.avg_chunk_size = avg_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size
        self._chunker = get_chunk_func(
            avg_chunk_size=self.avg_chunk_size,
            min_chunk_size=min_chunk_size,
            max_chunk_size=max_chunk_size,
        )
        self._iter = None
        self._tail = b""

    def __repr__(self):
        return "<Chunker avg={}, min={}, max={}>".format(
            self.avg_chunk_size,
            self.min_chunk_size,
            self.max_chunk_size,
        )

    def update(self, message: bytes) -> "Chunker":
        self.message = message
        self._iter = self._chunker(io.BytesIO(self._tail + message))
        return self

    @property
    def chunks(self) -> List[bytes]:
        """Get bytes of chunks."""
        if not self._iter:
            return

        self._tail = b""
        prev = None
        for chunk in self._iter:
            content = chunk.data
            if prev is not None:
                yield prev
            prev = content
        self._tail = prev

    @property
    def tail(self) -> bytes:
        """Get the bytes of last chunk if have.

        >>> c = Chunker()
        >>> list(c.update(b'12345').chunks)
        []
        >>> c.tail
        b'12345'
        """
        return self._tail

    def reset(self) -> bytes:
        """
        Reset the chunker.

        >>> # not reset
        >>> c = Chunker()
        >>> list(c.update(b'12345').chunks)
        []
        >>> list(c.update(b'67890').chunks)
        []
        >>> c.tail
        b'1234567890'
        >>> c = Chunker()
        >>> list(c.update(b'12345').chunks)
        []
        >>> c.reset()
        b'12345'
        >>> list(c.update(b'67890').chunks)
        []
        >>> c.tail
        b'67890'
        """
        remaining = self._tail
        self._tail = b""
        return remaining
