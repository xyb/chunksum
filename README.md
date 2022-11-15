# chunksum

[![test](https://github.com/xyb/chunksum/actions/workflows/test.yml/badge.svg)](https://github.com/xyb/chunksum/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/xyb/chunksum/branch/main/graph/badge.svg?token=LR3ET0TBK2)](https://codecov.io/gh/xyb/chunksum)
[![Maintainability](https://api.codeclimate.com/v1/badges/9bd0a3b4fcefb196b2f8/maintainability)](https://codeclimate.com/github/xyb/chunksum/maintainability)
[![Latest version](https://img.shields.io/pypi/v/chunksum.svg)](https://pypi.org/project/chunksum/)
[![Support python versions](https://img.shields.io/pypi/pyversions/chunksum)](https://pypi.org/project/chunksum/)

Print FastCDC rolling hash chunks and checksums.

```
Usage: chunksum <dir> [<alg_name>] [<prev_chunksums_file>]

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

prev_chunksums_file:
  You can specify the previous chunksums file if you want to
  resume a previous check, or if you want to find the incremental
  updates (new files) of the directory.


Examples:

  $ chunksum /etc > ~/etc.chunksums

  $ chunksum ~/Videos fcm4blake2b32 > ~/Videos/chunksums

  $ chunksum ~/Videos fcm4blake2b32 ~/chunksums > ~/chunksums.incr
```
