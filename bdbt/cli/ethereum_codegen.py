from pathlib import Path

import click

from bdbt.ethereum.dbt.dbt_generator import DbtGenerator
from bdbt.global_type import Database


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-d', '--dbt-dir', default=Path.cwd(), show_default=True, type=str,
              help='The absolute path for the dbt project.')
@click.option('-r', '--remote-dir-url', default='s3a://blockchain-dbt/dist/jars', show_default=True, type=str,
              help='The absolute path for remote workspace that will store external dependencies.')
@click.option('-d', '--database', default=Database.SPARK.value, show_default=True, type=str,
              help='The database to work for, like: spark, big_query, snowflake...')
def ethereum_codegen(
        dbt_dir: str = Path.cwd(),
        remote_dir_url: str = 's3a://ifcrypto/blockchain-dbt/jars',
        database: str = Database.SPARK.value,
):
    database_obj = Database(database)
    generator = DbtGenerator(database=database_obj, remote_dir_url=remote_dir_url, dbt_dir=dbt_dir)
    generator.gen_all()
