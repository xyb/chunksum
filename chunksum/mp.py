import multiprocessing
import time
from multiprocessing import cpu_count
from multiprocessing import Event
from multiprocessing import Queue
from queue import Empty

from tqdm.auto import tqdm

from .chunksum import compute_one_file
from .chunksum import iter_files
from .utils import get_total_size


def producer(queue_path, iter):
    for path in iter:
        queue_path.put(path)


def consumer(
    output_file,
    progress_bar,
    alg_name,
    skip_func,
    queue_path,
    queue_sums,
    busy,
    stop,
):
    while not stop.is_set() or not queue_path.empty():
        try:
            path = queue_path.get(timeout=0.001)
        except Empty:
            continue

        busy.set()

        if skip_func and skip_func(path):
            busy.clear()
            continue
        sums = compute_one_file(
            path,
            output_file=output_file,
            progress_bar=progress_bar,
            alg_name=alg_name,
            skip_func=skip_func,
        )
        queue_sums.put(sums)

        busy.clear()


def collector(queue_sums, output_file, busy, stop):
    while not stop.is_set() or not queue_sums.empty():
        try:
            sums = queue_sums.get(timeout=0.001)
        except Empty:
            continue

        busy.set()
        print(sums, file=output_file, flush=True)
        busy.clear()


def wait_consumers(consumers, queue_path, busy_events, stop):
    while True:
        if queue_path.empty() and all([not e.is_set() for e in busy_events]):
            stop.set()
            [p.join() for p in consumers]
            break
        time.sleep(0.001)  # pragma: no cover


def compute_mp(paths, output_file, alg_name="fck4sha2", skip_func=None):
    """
    >>> import os
    >>> import tempfile
    >>> dir = tempfile.TemporaryDirectory()
    >>> path = os.path.join(dir.name, 'testfile')
    >>> _ = open(path, 'wb').write(b'hello')
    >>> chunksums = tempfile.NamedTemporaryFile()
    >>> compute_mp([dir.name], open(chunksums.name, 'w'))
    >>> for line in open(chunksums.name).readlines():
    ...   print(line.strip())
    95...50  .../testfile  fck4sha2!2c...24:5

    >>> chunksums = open(tempfile.NamedTemporaryFile().name, 'w')
    >>> compute_mp([dir.name], chunksums, skip_func=lambda x: True)
    >>> open(chunksums.name).readlines()
    []
    """

    total = sum([get_total_size(path) for path in paths])

    progress_bar = tqdm(
        desc="chunksum",
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    )

    consumers = []
    queue_path = Queue(10)
    queue_sums = Queue(10)
    busy_events = []

    # fix macos issue. see https://github.com/pytorch/pytorch/pull/36542
    Process = multiprocessing.get_context("fork").Process

    proc_producer = Process(target=producer, args=(queue_path, iter_files(paths)))
    stop_collector = Event()
    busy = Event()
    busy_events.append(busy)
    proc_collector = Process(
        target=collector,
        args=(queue_sums, output_file, busy, stop_collector),
    )

    stop_consumers = Event()
    for i in range(cpu_count()):
        busy = Event()
        busy_events.append(busy)
        p = Process(
            target=consumer,
            args=(
                output_file,
                progress_bar,
                alg_name,
                skip_func,
                queue_path,
                queue_sums,
                busy,
                stop_consumers,
            ),
        )
        consumers.append(p)
        p.start()

    proc_collector.start()
    proc_producer.start()

    # wait producer
    proc_producer.join()

    wait_consumers(consumers, queue_path, busy_events, stop_consumers)

    # wait collector
    stop_collector.set()
    proc_collector.join()
