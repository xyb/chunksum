import time
from multiprocessing import cpu_count
from multiprocessing import Event
from multiprocessing import Process
from multiprocessing import Queue
from queue import Empty

from tqdm.auto import tqdm

from .chunksum import compute_one_file
from .chunksum import iter_files
from .utils import get_total_size


def compute_mp(paths, output_file, alg_name="fck4sha2", skip_func=None):
    def producer(queue_path, iter_):
        for path in iter_:
            queue_path.put(path)

    def consumer(queue_path, queue_sums, busy, stop):
        while not stop.is_set():
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
                output_file=None,
                progress_bar=progress_bar,
                alg_name=alg_name,
                skip_func=skip_func,
            )
            queue_sums.put(sums)
            busy.clear()

    def collector(queue_sms, output_file, busy, stop):
        while not stop.is_set():
            try:
                sums = queue_sums.get(timeout=0.001)
            except Empty:
                continue
            busy.set()
            print(sums, file=output_file, flush=True)
            busy.clear()

    total = sum([get_total_size(path) for path in paths])

    progress_bar = tqdm(
        desc="chunksum",
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    )
    progress_bar = None

    consumers = []
    queue_path = Queue(10)
    queue_sums = Queue(10)
    events = []

    prod = Process(target=producer, args=(queue_path, iter_files(paths)))
    stop = Event()
    busy = Event()
    events.append(busy)
    coll = Process(target=collector, args=(queue_sums, output_file, busy, stop))

    for i in range(cpu_count()):
        busy = Event()
        events.append(busy)
        p = Process(target=consumer, args=(queue_path, queue_sums, busy, stop))
        consumers.append(p)

    for p in consumers:
        p.start()

    coll.start()
    prod.start()
    prod.join()
    while True:
        if (
            queue_path.empty()
            and queue_sums.empty()
            and all([not busy.is_set() for busy in events])
        ):
            stop.set()
            break
        time.sleep(0.001)
