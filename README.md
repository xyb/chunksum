# chunksum

Print FastCDC rolling hash chunks and checksums.

[![test](https://github.com/xyb/chunksum/actions/workflows/test.yml/badge.svg)](https://github.com/xyb/chunksum/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/xyb/chunksum/branch/main/graph/badge.svg?token=LR3ET0TBK2)](https://codecov.io/gh/xyb/chunksum)
[![Maintainability](https://api.codeclimate.com/v1/badges/9bd0a3b4fcefb196b2f8/maintainability)](https://codeclimate.com/github/xyb/chunksum/maintainability)
[![Latest version](https://img.shields.io/pypi/v/chunksum.svg)](https://pypi.org/project/chunksum/)
[![Support python versions](https://img.shields.io/pypi/pyversions/chunksum)](https://pypi.org/project/chunksum/)

```
usage: chunksum [-h] [-n ALG_NAME] [-f CHUNKSUMS_FILE] [-i INCR_FILE] [-m]
                [path ...]

Print FastCDC rolling hash chunks and checksums.

positional arguments:
  path                  path to compute chunksums

optional arguments:
  -h, --help            show this help message and exit
  -n ALG_NAME, --alg-name ALG_NAME
                        chunksum algorithm name.
  -f CHUNKSUMS_FILE, --chunksums-file CHUNKSUMS_FILE
                        chunksum file path, `-' for standard output.
  -i INCR_FILE, --incr-file INCR_FILE
                        incremental updates file path
  -m, --multi-process   same number of multi-processes as cpu

alg-name:
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

chunksums-file and incr-file:
  You can specify the previous chunksums file if you want to
  resume a previous check, or if you want to find the incremental
  updates (new files) of the directory.

Examples:

  $ chunksum /etc > ~/etc.chunksums

  $ chunksum -n fcm4blake2b32 -m ~/Videos

  $ chunksum -n fcm4blake2b32 -f ~/Videos/chunksums ~/Videos

  $ chunksum -n fcm4blake2b32 -f ~/chunksums -i ~/chunksums.incr ~/Videos
```
