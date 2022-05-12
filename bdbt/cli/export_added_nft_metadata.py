import logging
import time

import click

from bdbt.external.export_nft_metadata_job import ExportNFTMetadataJob
from bdbt.utils import get_partitions


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-a', '--api-keys', required=True, type=str,
              help='The api keys for moralis, split with commas.')
@click.option('-ca', '--contract-address', required=True, type=str,
              help='The added NFT contract address, split with commas.')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int,
              help='The number of metadata in a file.')
@click.option('-mw', '--max-workers', default=4, show_default=True, type=int,
              help='The maximum number of exporting nft metadata workers.')
def export_added_nft_metadata(
        api_keys: str,
        contract_address: str,
        batch_size: int = 100,
        max_workers: int = 4
) -> None:
    _api_keys = api_keys.split(',')
    _address = contract_address.split(',')

    if max_workers > len(_api_keys):
        raise ValueError('the max workers should less than the size of api keys.')

    logging.info(f'there are {len(_address)} need to be sync.')

    for idx, partition in enumerate(get_partitions(_address, batch_size)):
        logging.info(f'start export new partition, {idx * batch_size} to {idx * batch_size + len(partition)}')
        filename = 'tmp/nft_metadata_{}.csv'.format(idx)

        job = ExportNFTMetadataJob(
            collections=partition,
            api_keys=_api_keys,
            filename=filename,
            max_workers=max_workers
        )

        job.run()
        time.sleep(60)
