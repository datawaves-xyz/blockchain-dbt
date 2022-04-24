import logging
import os.path
import pathlib
from typing import Dict

from bdbt.ethereum.abi.abi_data_type import ABISchema, ABIEventSchema, ABICallSchema


class DbtCodeGenerator:

    def __init__(self, need_udf: bool):
        self.need_udf = need_udf
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def create_file_and_write(filepath: str, content: str):
        if os.path.exists(filepath):
            raise FileExistsError(filepath + ' is exists.')

        with open(filepath, 'w') as f:
            f.write(content)

    def gen_models_for_project(
            self,
            workspace: str,
            project_name: str,
            contract_name: str,
            contract_address: str,
            abi: ABISchema
    ):
        """
        Generate some dbt model sql files and schema yaml file,
        one model sql file for one event or call.
        """
        project_path = os.path.join(workspace, project_name)
        pathlib.Path(project_path).mkdir(parents=True, exist_ok=True)

        for event in abi.events:
            self.gen_event_dbt_model(project_path, contract_name, contract_address, event)
        for call in abi.calls:
            self.gen_call_dbt_model(project_path, contract_name, contract_address, call)

    def gen_udf_for_dbt(self, dbt_dir: str, abi_map: Dict[str, Dict[str, ABISchema]]):
        """
        Generating the external dependency UDF required for the dbt decode contract process, there are some databases
        that are available with a script language runtime, then they do not need to generate additional UDFs via this
        method.
        However, if the database does not support another language in the SQL runtime, then an external UDF may be
        required, such as Spark, which can generate jar packages via this function to enable the use of more complex
        functions in SQL.
        Notes: the best use of this function is to collect all the contract information and call it once to generate all
        the UDFs.

        :param dbt_dir: the absolute path of the dbt project folder
        :param abi_map: project_name -> (contract_name -> abi_schema)
        """
        udf_workspace = self.prepare_udf_workspace(dbt_dir)

        # the empty events and empty calls don't need UDF to decode data
        for proj_name, contract_name_to_abi in abi_map.items():
            for contract_name, abi in contract_name_to_abi.items():
                for event in abi.nonempty_events:
                    self.gen_event_udf(udf_workspace, proj_name, contract_name, event)
                for call in abi.nonempty_calls:
                    self.gen_call_udf(udf_workspace, proj_name, contract_name, call)

        self.build_udf(dbt_dir)

    def gen_event_dbt_model(
            self,
            project_path: str,
            contract_name: str,
            contract_address: str,
            event: ABIEventSchema
    ):
        raise NotImplementedError()

    def gen_call_dbt_model(
            self,
            project_path: str,
            contract_name: str,
            contract_address: str,
            call: ABICallSchema
    ):
        raise NotImplementedError()

    def gen_event_udf(self, udf_workspace: str, project_name, contract_name: str, event: ABIEventSchema):
        raise NotImplementedError()

    def gen_call_udf(self, udf_workspace: str, project_name, contract_name: str, call: ABICallSchema):
        raise NotImplementedError()

    def prepare_udf_workspace(self, dbt_dir: str) -> str:
        raise NotImplementedError()

    def build_udf(self, dbt_dir: str):
        raise NotImplementedError()

    @staticmethod
    def evt_model_file_name(
            project_name: str, contract_name: str, event: ABIEventSchema
    ) -> str:
        return f'{project_name}_{contract_name}_evt_{event.name}'

    @staticmethod
    def call_model_file_name(
            project_name: str, contract_name: str, call: ABICallSchema
    ) -> str:
        return f'{project_name}_{contract_name}_call_{call.name}'
