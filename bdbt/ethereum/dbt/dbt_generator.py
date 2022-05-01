import functools
import glob
import json
import logging
import os
import shutil
from os import listdir
from typing import Dict, cast, List

import pyaml
import ruamel.yaml
import yaml

from bdbt.content import Contract
from bdbt.ethereum.abi.abi_data_type import ABISchema
from bdbt.ethereum.abi.abi_transformer import ABITransformer
from bdbt.ethereum.dbt.dbt_code_generator import DbtCodeGenerator as CG
from bdbt.ethereum.dbt.dbt_factory import DbtFactory
from bdbt.ethereum.dbt.dbt_schema_generator import DbtSchemaGenerator
from bdbt.global_type import Database, DbtTable, DbtColumn, DbtModelSchema

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


class DbtGenerator:
    def __init__(self, database: Database, dbt_dir: str, remote_dir_url: str):
        self._dbt_dir = dbt_dir
        self._remote_dir_url = remote_dir_url
        self._codegen = DbtFactory.new_code_generator(database, remote_dir_url)
        self._schemagen = DbtSchemaGenerator()
        self._transformer = ABITransformer()
        self._logger = logging.getLogger(self.__class__.__name__)

    def gen_all(self):
        # Remove the old codegen folder and recreate it
        if os.path.exists(self.codegen_dir):
            shutil.rmtree(self.codegen_dir)
        os.mkdir(self.codegen_dir)
        self._logger.info('recreate codegen folder.')

        self._gen_models_and_schema()
        self._gen_udf()
        self._replenish_project_yml()

    def _gen_models_and_schema(self):
        models_count_map: Dict[str, int] = {}

        for project, contracts in self.contracts_map.items():
            # Generate models
            for contract in contracts:
                self._codegen.gen_models_for_project(
                    workspace=self.codegen_dir,
                    project_name=project,
                    contract=contract,
                    version=self.version,
                    abi=self._transformer.transform_abi(contract['abi'])
                )

            # Generate schema
            models: List[DbtTable] = []
            for contract in contracts:
                abi = self._transformer.transform_abi(contract['abi'])
                for event in abi.events:
                    columns = [DbtColumn(name=i.name) for i in event.inputs]
                    columns.extend(DbtColumn(name=i) for i in evt_base_column)
                    models.append(DbtTable(name=CG.evt_model_name(contract['name'], event, project),
                                           columns=columns))

                for call in abi.calls:
                    columns = [DbtColumn(name=i.name) for i in call.inputs]
                    columns.extend([DbtColumn(name=i.name) for i in call.outputs])
                    columns.extend(DbtColumn(name=i) for i in call_base_column)
                    models.append(DbtTable(name=CG.call_model_name(contract['name'], call, project),
                                           columns=columns))

            schema = DbtModelSchema(models=models)
            schema_path = os.path.join(self.codegen_dir, project, 'schema.yml')
            with open(schema_path, 'w') as f:
                # https://docs.getdbt.com/faqs/why-version-2
                f.write('version: 2\n')
                f.write(pyaml.dump(schema, sort_dicts=False))

            models_count_map[project] = len(models)

        self._logger.info('generate all models and schemas: ')
        for project, count in models_count_map.items():
            self._logger.info(f'  {project} has {count} models')

    def _gen_udf(self):
        if not self._codegen.need_udf:
            return

        abi_map: Dict[str, Dict[str, ABISchema]] = {}
        for project, contracts in self.contracts_map.items():
            abi_map[project] = {}
            for contract in contracts:
                abi_map[project][contract['name']] = self._transformer.transform_abi(contract['abi'])

        self._codegen.gen_udf_for_dbt(self._dbt_dir, abi_map, self.version)
        self._logger.info('generate a UDF dependency.')

    def _replenish_project_yml(self):
        projects_dict = {
            project: {
                '+schema': project,
                '+tags': ['chain_ethereum', 'level_parse', f'proj_{project}']
            }
            for project in self.contracts_map.keys()
        }

        project_conf, ind, bsi = ruamel.yaml.util.load_yaml_guess_indent(open(self.dbt_project_yml))
        project_conf = json.loads(json.dumps(project_conf))

        if 'models' not in project_conf.keys():
            raise ValueError('models should be in the dbt_project.yml')
        else:
            model_conf: Dict[str, any] = project_conf['models']

        if len(model_conf.keys()) != 1:
            raise ValueError('only one database should be defined in the models')
        else:
            database_conf: Dict[str, any] = model_conf[list(model_conf.keys())[0]]

        database_conf['codegen'] = projects_dict

        y = ruamel.yaml.YAML()
        y.indent(mapping=ind, sequence=ind, offset=bsi)
        with open(self.dbt_project_yml, 'w') as nf:
            y.dump(project_conf, nf)

        self._logger.info('replenish dbt_project.yml with all project configs.')

    @property
    def contracts_dir(self) -> str:
        return os.path.join(self._dbt_dir, 'contracts')

    @property
    def model_dir(self) -> str:
        return os.path.join(self._dbt_dir, 'models')

    @property
    def codegen_dir(self) -> str:
        return os.path.join(self.model_dir, 'codegen')

    @property
    def dbt_project_yml(self) -> str:
        return os.path.join(self._dbt_dir, 'dbt_project.yml')

    @property
    def new_dbt_project_yml(self) -> str:
        return os.path.join(self._dbt_dir, 'new_dbt_project.yml')

    @functools.cached_property
    def version(self) -> str:
        dbt_project_yml = os.path.join(self._dbt_dir, 'dbt_project.yml')
        with open(dbt_project_yml, 'r') as f:
            project_conf: Dict[str, any] = yaml.safe_load(f)
            dbt_version: str = project_conf['version']

        if dbt_version is None:
            raise ValueError('dbt_project.yml should contains the version field.')

        return dbt_version

    @functools.cached_property
    def project_dirs(self):
        return [f for f in listdir(self.contracts_dir) if os.path.isdir(os.path.join(self.contracts_dir, f))]

    @functools.cached_property
    def contracts_map(self) -> Dict[str, List[Contract]]:
        contracts_map: Dict[str, List[Contract]] = {}
        for project in self.project_dirs:
            contract_paths = glob.glob(os.path.join(self.contracts_dir, project, '*.json'))
            for contract_path in contract_paths:
                with open(contract_path, 'r') as f:
                    if project not in contracts_map:
                        contracts_map[project] = []
                    contracts_map[project].append(cast(Contract, json.loads(f.read())))
        return contracts_map
