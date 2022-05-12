import logging
import os
import pathlib
import sys
import time
from typing import Optional, Dict, IO, Callable, Any, Generator

import backoff
import requests
from requests import Response
from requests.exceptions import (
    Timeout as RequestsTimeout,
    HTTPError,
    TooManyRedirects,
    RequestException
)

RETRY_EXCEPTIONS = (ConnectionError, HTTPError, RequestsTimeout, TooManyRedirects, OSError, RequestException)


@backoff.on_exception(
    backoff.expo,
    requests.exceptions.RequestException,
    max_tries=3
)
def get_url_retry_on_failed(
        url: str, params: Optional[Dict[str, any]] = None, **kwargs
) -> Response:
    # the interval of backoff retry is 3s,
    # the main effect is handling the network error,
    # so the interval is relatively short
    return requests.get(url, params=params, **kwargs)


def execute_with_retries(
        func: Callable,
        *args,
        max_retries: int = 3,
        sleep_seconds: int = 60,
        retry_exceptions=RETRY_EXCEPTIONS
) -> Any:
    for i in range(max_retries):
        try:
            return func(*args)
        except retry_exceptions as e:
            logging.exception('An exception occurred while executing execute_with_retries. Retry #{}'.format(i))
            if i < max_retries - 1:
                logging.info('The request will be retried after {} seconds. Retry #{}'.format(sleep_seconds, i))
                time.sleep(sleep_seconds)
                continue
            else:
                raise


def get_file_handle(
        filename: str,
        mode: str = 'w',
        binary: bool = False,
        create_parent_dirs: bool = True
) -> IO:
    if create_parent_dirs and filename is not None:
        dirname = os.path.dirname(filename)
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
    full_mode = mode + ('b' if binary else '')
    is_file = filename and filename != '-'
    if is_file:
        fh = open(filename, full_mode)
    elif filename == '-':
        fd = sys.stdout.fileno() if mode == 'w' else sys.stdin.fileno()
        fh = os.fdopen(fd, full_mode)
    else:
        raise ValueError('the file type is not supported')
    return fh


def get_partitions(
        _list: list, partition_batch_size: int
) -> Generator[list, Any, None]:
    for i in range(0, len(_list), partition_batch_size):
        yield _list[i: i + partition_batch_size]
