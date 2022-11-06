def parse_chunks(data):
    alg_name, chunks = data.split('!')
    if chunks:
        chunks = [c.split(':') for c in chunks.split(',') if c]
        chunks = [(id, int(size)) for id, size in chunks]
        return alg_name, chunks
    else:
        return alg_name, []


def parse_line(line):
    """
    >>> from pprint import pprint
    >>> pprint(parse_line('sum1  ./a  fck0sha2!'))
    {'alg_name': 'fck0sha2', 'checksum': 'sum1', 'chunks': [], 'path': './a'}
    >>> pprint(parse_line('sum2  ./file1  fck0sha2!abcd:10'))
    {'alg_name': 'fck0sha2',
     'checksum': 'sum2',
     'chunks': [('abcd', 10)],
     'path': './file1'}
    >>> pprint(parse_line('sum3  ./file2  fck0sha2!bad:20,beef:30'))
    {'alg_name': 'fck0sha2',
     'checksum': 'sum3',
     'chunks': [('bad', 20), ('beef', 30)],
     'path': './file2'}
    """
    items = line.split('  ')
    checksum = items[0]
    chunks = items[-1]
    if len(items) > 3:
        path = '  '.join(items[1:-1])
    else:
        path = items[1]
    alg_name, chunks = parse_chunks(chunks)
    return dict(checksum=checksum,
                path=path,
                alg_name=alg_name,
                chunks=chunks)


def parse_chunksums(file):
    """
    >>> from pprint import pprint
    >>> import io
    >>> pprint(list(parse_chunksums(io.StringIO('''
    ... sum1  ./a  fck0sha2!
    ... sum2  ./file1  fck0sha2!abcd:10
    ... sum3  ./file2  fck0sha2!bad:20,beef:30'''))))
    [{'alg_name': 'fck0sha2', 'checksum': 'sum1', 'chunks': [], 'path': './a'},
     {'alg_name': 'fck0sha2',
      'checksum': 'sum2',
      'chunks': [('abcd', 10)],
      'path': './file1'},
     {'alg_name': 'fck0sha2',
      'checksum': 'sum3',
      'chunks': [('bad', 20), ('beef', 30)],
      'path': './file2'}]
    """
    for line in file:
        line = line.strip()
        if not line:
            continue
        yield parse_line(line)
