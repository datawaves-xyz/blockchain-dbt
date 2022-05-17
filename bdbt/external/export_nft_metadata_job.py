import logging
import os
from dataclasses import dataclass
from typing import Optional, List, Tuple

import pyarrow.parquet as pq
from mashumaro import DataClassDictMixin
from pyarrow import json as pjson

from bdbt.exporter import JsonItemExporter
from bdbt.external.clients.moralis import Moralis
from bdbt.external.executors.batch_work_executor import BatchWorkExecutor


@dataclass(frozen=True)
class Collection(DataClassDictMixin):
    contract_address: str
    name: str
    symbol: Optional[str]
    standard: Optional[str]


class ExportNFTMetadataJob:
    def __init__(
            self,
            addresses: List[str],
            api_keys: List[str],
            filename: str,
            max_workers: Optional[int] = None,
            to_parquet: bool = True
    ) -> None:
        if max_workers is None:
            max_workers = len(api_keys)

        self.addresses = addresses
        self.api_keys = api_keys
        self.executor = BatchWorkExecutor(
            batch_size=1,
            max_workers=max_workers,
            log_item_step=20,
        )
        self.filename = filename
        self.to_parquet = to_parquet
        self.exporter = JsonItemExporter(filename=filename)
        self.client = Moralis(host='https://deep-index.moralis.io/api/v2')
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self) -> None:
        items = [(address, self.api_keys[idx % len(self.api_keys)])
                 for idx, address in enumerate(self.addresses)]

        self.executor.execute(
            work_iterable=items,
            work_handler=self._export_single_metadata,
            total_items=len(self.addresses)
        )

    def end(self) -> None:
        self.executor.shutdown()

        if self.to_parquet:
            try:
                table = pjson.read_json(self.filename)
                pq.write_table(table, self.filename.replace('json', 'parquet'))
                os.remove(self.filename)
            except Exception as e:
                self.logger.error(f"can't transform json to parquet, file: {self.filename}, error: {e}")

    def _export_single_metadata(
            self, items: List[Tuple[str, str]]
    ) -> None:
        for address, api_key in items:
            res = self.client.drink_up_single_token_metadata(address, api_key)
            for item in res:
                self.exporter.export_item(item)
