import os.path
import pathlib
from typing import List, Dict

import pyaml

from bdbt.ethereum.abi.abi_data_type import ABISchema
from bdbt.global_type import DbtTable, DbtColumn, DbtModelSchema

evt_base_column = [
    'evt_block_number',
    'evt_block_time',
    'evt_index',
    'evt_tx_hash',
    'contract_address',
    'dt'
]

call_base_column = [
    'call_success',
    'call_block_number',
    'call_block_time',
    'call_trace_address',
    'call_tx_hash',
    'contract_address',
    'dt'
]


class DbtSchemaGenerator:
    @staticmethod
    def gen_dbt_schema(
            workspace: str,
            project_name: str,
            contract_name_to_abi: Dict[str, ABISchema]
    ):
        project_path = os.path.join(workspace, project_name)
        pathlib.Path(project_path).mkdir(parents=True, exist_ok=True)

        models: List[DbtTable] = []
        for contract_name, abi in contract_name_to_abi.items():
            for event in abi.events:
                columns = [DbtColumn(name=i.name) for i in event.inputs]
                columns.extend(DbtColumn(name=i) for i in evt_base_column)
                models.append(DbtTable(name=f'{contract_name}_evt_{event.name}', columns=columns))

            for call in abi.calls:
                columns = [DbtColumn(name=i.name) for i in call.inputs]
                columns.extend([DbtColumn(name=i.name) for i in call.outputs])
                columns.extend(DbtColumn(name=i) for i in call_base_column)
                models.append(DbtTable(name=f'{contract_name}_call_{call.name}', columns=columns))

        schema = DbtModelSchema(models=models)
        schema_path = os.path.join(project_path, 'schema.yml')
        with open(schema_path, 'w') as f:
            # https://docs.getdbt.com/faqs/why-version-2
            f.write('version: 2\n')
            f.write(pyaml.dump(schema, sort_dicts=False))
