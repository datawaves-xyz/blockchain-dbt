import logging
from typing import Callable, Iterable, Optional, Tuple, Generator, Any

from requests.exceptions import (
    Timeout as RequestsTimeout,
    HTTPError,
    TooManyRedirects,
    RequestException
)

from bdbt.external.executors.bounded_executor import BoundedExecutor
from bdbt.external.executors.fail_safe_executor import FailSafeExecutor
from bdbt.external.executors.progress_logger import ProgressLogger

RETRY_EXCEPTIONS = (ConnectionError, HTTPError, RequestsTimeout, TooManyRedirects, OSError, RequestException)


class BatchWorkExecutor:
    def __init__(
            self,
            batch_size: int,
            max_workers: int,
            max_retries: int = 3,
            sleep_seconds: int = 60,
            log_item_step: int = 5000,
            retry_exceptions: Tuple = RETRY_EXCEPTIONS,
    ) -> None:
        self.executor = FailSafeExecutor(BoundedExecutor(1, max_workers))
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.sleep_seconds = sleep_seconds
        self.retry_exceptions = retry_exceptions
        self.logger = logging.getLogger(self.__class__.__name__)
        self.progress_logger = ProgressLogger(logger=self.logger, log_item_step=log_item_step)

    def execute(
            self,
            work_iterable: Iterable,
            work_handler: Callable,
            total_items: Optional[int] = None
    ) -> None:
        self.progress_logger.start(total_items=total_items)
        for batch in self._batch_iterator(work_iterable):
            future = self.executor.submit(self._execute_handler_with_progress, work_handler, batch)

    def _execute_handler_with_progress(
            self, work_handler: Callable, batch: list
    ) -> None:
        work_handler(batch)
        self.progress_logger.track(len(batch))

    def _batch_iterator(
            self, iterable: Iterable,
    ) -> Generator[list, Any, None]:
        batch = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if len(batch) > 0:
            yield batch

    def shutdown(self) -> None:
        self.executor.shutdown()
        self.progress_logger.finish()
