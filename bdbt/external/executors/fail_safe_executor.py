from concurrent.futures import Future
from typing import Callable, Any, List

from bdbt.external.executors.bounded_executor import BoundedExecutor


class FailSafeExecutor:
    def __init__(
            self, delegate: BoundedExecutor
    ) -> None:
        self._delegate = delegate
        self._futures: List[Future] = []

    def submit(
            self, fn: Callable, *args: Any, **kwargs: Any
    ) -> Future:
        self._check_completed_futures()
        future = self._delegate.submit(fn, *args, **kwargs)
        self._futures.append(future)

        return future

    def shutdown(self):
        self._delegate.shutdown(wait=True)
        self._check_completed_futures()
        assert len(self._futures) == 0

    def _check_completed_futures(self):
        for future in self._futures.copy():
            if future.done():
                # Will throw an exception here if the future failed
                future.result()
                self._futures.remove(future)
