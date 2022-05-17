import csv
import logging
import os

import click as click

from bdbt.external.export_nft_metadata_job import Collection, ExportNFTMetadataJob
from bdbt.logging_utils import logging_basic_config
from bdbt.utils import get_partitions

logging_basic_config()


def tmp_folder_path() -> str:
    dir_path = os.path.dirname(__file__)
    return os.path.join(dir_path, '..', '..', 'tmp')


def load_breaking_point() -> int:
    tmp_path = tmp_folder_path()

    max_partition_number = -1
    if os.path.isdir(tmp_path):
        filenames = os.listdir(tmp_path)
        for filename in filenames:
            if filename.startswith('nft_metadata'):
                partition_number = int(filename[:-4].split('_')[-1])
                if partition_number > max_partition_number:
                    max_partition_number = partition_number
        # can't promise the last file is finished.
        return max(max_partition_number - 1, 0)
    else:
        return max_partition_number


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-a', '--api-keys', required=True, type=str,
              help='The api keys for moralis.')
@click.option('-w', '--whitelist', required=True, type=str,
              help='The file path of the NFT whitelist.')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int,
              help='The number of metadata in a file.')
@click.option('-mw', '--max-workers', default=4, show_default=True, type=int,
              help='The maximum number of exporting nft metadata workers.')
@click.option('-p', '--to-parquet', default=True, show_default=True, type=bool,
              help='Transform the result file to parquet.')
def export_all_nft_metadata(
        api_keys: str,
        whitelist: str,
        batch_size: int = 100,
        max_workers: int = 4,
        to_parquet: bool = True
) -> None:
    keys = api_keys.split(',')
    if max_workers > len(keys):
        raise ValueError('the max workers should less than the size of api keys.')

    addresses = []
    with open(whitelist, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            addresses.append(Collection.from_dict(row).contract_address)

    cached_partition_number = load_breaking_point()
    logging.info(f'restart to export all nft metadata, the current breaking point is {cached_partition_number}')

    for idx, partition in enumerate(get_partitions(addresses, batch_size)):
        if idx <= cached_partition_number:
            continue

        logging.info(f'start export new partition, {idx * batch_size} to {idx * batch_size + len(partition)}')
        filename = 'tmp/nft_metadata_{}.json'.format(idx)

        job = ExportNFTMetadataJob(
            addresses=partition,
            api_keys=keys,
            filename=filename,
            max_workers=max_workers,
            to_parquet=to_parquet
        )

        try:
            job.run()
        finally:
            job.end()
