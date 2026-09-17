#!/usr/bin/env python3
"""A steady webhook stream, and nothing clever.

It exists to keep requests in flight while somebody sends the process a signal, so what matters is
that it does not stop on its own and does not lie about what it got. Every response is counted by
status; a connection that is refused or reset is counted separately, because after the shutdown
begins that is the expected answer rather than a failure of this script.

It is deliberately NOT the oracle. What was accepted is the row in the service's own database; this
only makes rows happen.

    generator.py <url> <seconds> <concurrency>
"""
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter

url, seconds, concurrency = sys.argv[1], float(sys.argv[2]), int(sys.argv[3])
stop = threading.Event()
counts = Counter()
lock = threading.Lock()


def worker(index: int) -> None:
    n = 0
    while not stop.is_set():
        n += 1
        body = b'{"worker":%d,"n":%d}' % (index, n)
        request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                outcome = str(response.status)
        except urllib.error.HTTPError as refused:
            outcome = str(refused.code)
        except Exception as failure:              # noqa: BLE001 - every transport failure is one bucket
            outcome = type(failure).__name__
        with lock:
            counts[outcome] += 1


threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
for thread in threads:
    thread.start()

# A SIGTERM ENDS IT THE SAME WAY THE CLOCK DOES, and that is the point: the harness stops this
# script once the service is gone, and a generator killed outright prints nothing — which is how the
# first run of the harness produced an empty counts file and no way to tell a stream that ran from
# one that never started.
signal.signal(signal.SIGTERM, lambda *_: stop.set())
signal.signal(signal.SIGINT, lambda *_: stop.set())

stop.wait(seconds)
stop.set()
for thread in threads:
    thread.join(timeout=5)

print(" ".join(f"{name}={count}" for name, count in sorted(counts.items())))
