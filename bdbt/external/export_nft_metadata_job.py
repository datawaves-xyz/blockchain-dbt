from dataclasses import dataclass
from typing import Optional, List, Tuple

from mashumaro import DataClassDictMixin

from bdbt.exporter import CsvItemExporter
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
            collections: List[Collection],
            api_keys: List[str],
            filename: str,
            max_workers: Optional[int] = None,
    ) -> None:
        if max_workers is None:
            max_workers = len(api_keys)

        self.collections = collections
        self.api_keys = api_keys
        self.executor = BatchWorkExecutor(
            batch_size=1,
            max_workers=max_workers,
            log_item_step=20,
        )
        self.exporter = CsvItemExporter(filename=filename, headers=[
            'token_address',
            'token_id',
            'amount',
            'token_hash',
            'block_number_minted',
            'contract_type',
            'name',
            'symbol',
            'token_uri',
            'metadata',
            'synced_at'
        ])
        self.client = Moralis(host='https://deep-index.moralis.io/api/v2')

    def run(self) -> None:
        items = [(collection, self.api_keys[idx % len(self.api_keys)])
                 for idx, collection in enumerate(self.collections)]

        self.executor.execute(
            work_iterable=items,
            work_handler=self._export_single_metadata,
            total_items=len(self.collections)
        )

    def _export_single_metadata(
            self, items: List[Tuple[Collection, str]]
    ) -> None:
        for collection, api_key in items:
            res = self.client.drink_up_single_token_metadata(collection.contract_address, api_key)
            for item in res:
                self.exporter.export_item(item)
