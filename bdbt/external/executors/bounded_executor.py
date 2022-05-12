from concurrent.futures import ThreadPoolExecutor, Future
from threading import BoundedSemaphore
from typing import Callable, Any


class BoundedExecutor:
    """
    Prevent the work_queue in ThreadPoolExecutor from growing indefinitely and waiting
    until a thread is released when there are no locks left in the semaphore
    """

    def __init__(
            self, bound: int, max_workers: int
    ) -> None:
        self._delegate = ThreadPoolExecutor(max_workers=max_workers)
        self._semaphore = BoundedSemaphore(bound + max_workers)

    def submit(
            self, fn: Callable, *args: Any, **kwargs: Any
    ) -> Future:
        self._semaphore.acquire()
        try:
            future = self._delegate.submit(fn, *args, **kwargs)
        except:
            self._semaphore.release()
            raise
        else:
            future.add_done_callback(lambda _: self._semaphore.release())
            return future

    def shutdown(self, wait=True):
        self._delegate.shutdown(wait)
