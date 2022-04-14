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

    def generate_dbt_models(
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
            self.generate_event_dbt_model(project_path, contract_name, contract_address, event)
        for call in abi.calls:
            self.generate_call_dbt_model(project_path, contract_name, contract_address, call)

    def generate_dbt_udf(
            self,
            workspace: str,
            project_name: str,
            contract_name_to_abi: Dict[str, ABISchema]
    ):
        """
        Generate some UDF file bind with the destination database, the UDF is used to decode contract,
        we can integrate Javascript or Python code with SQL in some database, maybe they needn't define UDF.
        """
        if not self.need_udf:
            raise Exception(f'{self.__class__.__name__} do not implement some functions about UDF')

        project_path = os.path.join(workspace, project_name)
        pathlib.Path(project_path).mkdir(parents=True, exist_ok=True)

        udf_workspace = self.prepare_udf_workspace(project_path)

        # the empty events and empty calls don't need UDF to decode data
        for contract_name, abi in contract_name_to_abi.items():
            for event in abi.nonempty_events:
                self.generate_event_udf(udf_workspace, contract_name, event)
            for call in abi.nonempty_calls:
                self.generate_call_udf(udf_workspace, contract_name, call)

        self.build_udf(project_path)

    def generate_event_dbt_model(
            self,
            project_path: str,
            contract_name: str,
            contract_address: str,
            event: ABIEventSchema
    ):
        raise NotImplementedError()

    def generate_call_dbt_model(
            self,
            project_path: str,
            contract_name: str,
            contract_address: str,
            call: ABICallSchema
    ):
        raise NotImplementedError()

    def generate_event_udf(self, udf_workspace: str, contract_name: str, event: ABIEventSchema):
        raise NotImplementedError()

    def generate_call_udf(self, udf_workspace: str, contract_name: str, call: ABICallSchema):
        raise NotImplementedError()

    def prepare_udf_workspace(self, project_path: str) -> str:
        raise NotImplementedError()

    def build_udf(self, project_path: str):
        raise NotImplementedError()
