from typing import Dict, Any

from bdbt.dbt.dbt_schema_generator import DbtSchemaGenerator


class SparkDbtSchemaGenerator(DbtSchemaGenerator):
    def get_dbt_config(self) -> Dict[str, Any]:
        return {
            '+file_format': 'parquet',
            '+partition_by': 'dt'
        }
