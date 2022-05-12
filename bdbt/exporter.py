import csv
import io
import threading
from typing import Any, Dict, List

from bdbt.utils import get_file_handle


class CsvItemExporter:
    def __init__(
            self,
            filename: str,
            headers: List[str],
            encoding: str = 'utf-8',
            include_headers_line=True,
            **kwargs
    ) -> None:
        file = get_file_handle(filename, binary=True)

        self.encoding = encoding
        self.stream = io.TextIOWrapper(
            file,
            line_buffering=False,
            write_through=True,
            encoding=self.encoding
        )
        self.csv_writer = csv.DictWriter(f=self.stream, fieldnames=headers, **kwargs)
        self.include_headers_line = include_headers_line
        self._headers_not_written = True
        self._write_headers_lock = threading.Lock()

    def export_item(self, item: Dict[str, Any]):
        # Double-checked locking (safe in Python because of GIL) https://en.wikipedia.org/wiki/Double-checked_locking
        if self._headers_not_written:
            with self._write_headers_lock:
                if self._headers_not_written:
                    self.csv_writer.writeheader()
                    self._headers_not_written = False

        self.csv_writer.writerow(item)
