import glob
import json
import pathlib
from typing import Optional, cast

import click

from bdbt.cli.content import Contract
from bdbt.ethereum.dbt.dbt_factory import DbtFactory
from bdbt.global_type import Database


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-w', '--workspace', required=True, type=str,
              help='The absolute path for workspace that generate all files.')
@click.option('-rw', '--remote-workspace', required=True, type=str,
              help='The absolute path for remote workspace that will store external dependencies.')
@click.option('-p', '--project-folder', required=True, type=str,
              help='The absolute path for project folder, '
                   'program will read all contracts from the project folder and do the codegen for every contract.')
@click.option('-d', '--database', default=Database.SPARK, show_default=True, type=str,
              help='The database to work for, like: spark, big_query, snowflake...')
@click.option('-pn', '--project-name', type=str,
              help='It will be the name of project folder if it is not be set.')
def ethereum_codegen(
        workspace: str,
        remote_workspace: str,
        project_folder: str,
        database: str = Database.SPARK,
        project_name: Optional[str] = None
):
    contract_paths = glob.glob('*.json', root_dir=workspace)
    dbt_codegen = DbtFactory.new_code_generator(database, remote_workspace)
    dbt_schemagen = DbtFactory.new_schema_generator(database)

    if project_name is None:
        project_name = pathlib.Path(project_folder).name

    for contract_path in contract_paths:
        with open(contract_path, 'r') as f:
            contract = cast(json.loads(f.read()), Contract)

            dbt_codegen.generate_dbt_models(
                workspace=workspace,
                project_name=project_name,
                contract_name=contract.name,
                contract_address=contract.address,
                abi=contract.abi
            )

            dbt_codegen.generate_dbt_udf(
                workspace=workspace,
                project_name=project_name,
                contract_name=contract.name,
                abi=contract.abi
            )

            dbt_schemagen.generate_dbt_schema_file(
                workspace=workspace,
                project_name=project_name,
                contract_name=contract,
                abi=contract.abi
            )

    dbt_schemagen.generate_dbt_project_additional(
        workspace=workspace,
        projects=[project_name]
    )
