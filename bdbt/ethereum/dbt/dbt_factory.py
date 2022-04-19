from bdbt.ethereum.dbt.dbt_code_generator import DbtCodeGenerator
from bdbt.ethereum.dbt.spark.spark_dbt_code_generator import SparkDbtCodeGenerator
from bdbt.global_type import Database


class DbtFactory:

    @staticmethod
    def new_code_generator(database: Database, remote_workspace: str) -> DbtCodeGenerator:
        if database == Database.SPARK:
            return SparkDbtCodeGenerator(remote_workspace)
        else:
            raise ValueError(f'{database} is not be supported now.')
