import os
import tempfile
import unittest
from typing import AnyStr

import test
from bdbt.ethereum.abi.abi_transformer import ABITransformer
from bdbt.ethereum.abi.utils import normalize_abi
from bdbt.ethereum.dbt.spark.spark_dbt_schema_generator import SparkDbtSchemaGenerator

RESOURCE_GROUP = 'dbt_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class DbtSchemaGeneratorTestCase(unittest.TestCase):
    def test_generate_dbt_schema_file(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            SparkDbtSchemaGenerator.generate_dbt_schema_file(
                workspace=tempdir,
                project_name='opensea',
                contract_name='WyvernExchangeV2',
                abi=abi
            )

            project_path = os.path.join(tempdir, 'opensea')
            schema_filepath = os.path.join(project_path, os.listdir(project_path)[0])
            with open(schema_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('schema.yml')
            self.assertEqual(required_content, content)
