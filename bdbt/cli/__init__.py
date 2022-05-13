import click

from bdbt.cli.ethereum_codegen import ethereum_codegen
from bdbt.cli.export_added_nft_metadata import export_added_nft_metadata
from bdbt.cli.export_all_nft_metadata import export_all_nft_metadata
from bdbt.logging_utils import logging_basic_config

logging_basic_config()


@click.group()
@click.version_option()
@click.pass_context
def cli(ctx):
    pass


# ethereum module
cli.add_command(ethereum_codegen, "ethereum_codegen")

# external module
cli.add_command(export_all_nft_metadata, "export_all_nft_metadata")
cli.add_command(export_added_nft_metadata, "export_added_nft_metadata")
