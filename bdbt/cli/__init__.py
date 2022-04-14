from bdbt.cli.ethereum_codegen import ethereum_codegen
from bdbt.logging_utils import logging_basic_config

logging_basic_config()

import click


@click.group()
@click.version_option(version='0.2.0')
@click.pass_context
def cli(ctx):
    pass


cli.add_command(ethereum_codegen, "ethereum_codegen")
