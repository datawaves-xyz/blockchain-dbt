from typing import Dict, Any

from bdbt.ethereum.dbt.dbt_schema_generator import DbtSchemaGenerator


# TODO: this class should be deleted?
class SparkDbtSchemaGenerator(DbtSchemaGenerator):
    def get_dbt_config(self) -> Dict[str, Any]:
        return {}
