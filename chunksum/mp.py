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
    index,
    output_file,
    update_progress,
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

        sums = compute_one_file(
            path,
            output_file=output_file,
            update_progress=update_progress,
            alg_name=alg_name,
            skip_func=skip_func,
            bar_position=index,
        )
        if sums:
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


def progress_monitor(queue_progress, total):
    progress_bar = tqdm(
        desc="chunksum",
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        position=0,
    )

    while True:
        try:
            size = queue_progress.get(timeout=0.001)
        except Empty:
            continue
        progress_bar.update(size)
        if total == progress_bar.n:
            break


def wait_consumers(consumers, queue_path, busy_events, stop):
    while True:
        all_idle = all([not e.is_set() for e in busy_events])
        if all_idle and queue_path.empty():
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
    # fix macos issue. see https://github.com/pytorch/pytorch/pull/36542
    Process = multiprocessing.get_context("fork").Process

    total = sum([get_total_size(path) for path in paths])

    queue_progress = Queue(10)
    progress = Process(target=progress_monitor, args=(queue_progress, total))
    progress.start()

    def update_progress(size):
        queue_progress.put(size)

    busy_events = []
    consumers = []
    queue_sums = Queue(10)
    queue_path = Queue(10)

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
                i + 1,
                output_file,
                update_progress,
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
