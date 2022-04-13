import os
import tempfile
import unittest
from typing import AnyStr

import test
from bdbt.abi.abi_transformer import ABITransformer
from bdbt.abi.utils import normalize_abi
from bdbt.dbt.spark_dbt_udf_generator import SparkDbtUDFGenerator

RESOURCE_GROUP = 'dbt_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class SparkDbtCodeGeneratorTestCase(unittest.TestCase):

    def test_generate_dbt_udf(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtUDFGenerator()
            generator.generate_dbt_udf(
                workspace=tempdir,
                project_name='opensea',
                contract_name='WyvernExchangeV2',
                abi=abi
            )

            self.assertEqual(1, len(os.listdir(tempdir)))
            self.assertEqual('opensea', os.listdir(tempdir)[0])

            project_path = os.path.join(tempdir, 'opensea')
            self.assertEqual(1, len(os.listdir(project_path)))
            self.assertEqual('udf.jar', os.listdir(project_path)[0])

    def test_generate_call_udf(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('full_types_function_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtUDFGenerator()
            generator.generate_call_udf(
                udf_workspace=tempdir,
                contract_name='Test',
                call=[i for i in abi['calls'] if i['name'] == 'AllTypeFunction'][0]
            )

            java_filepath = os.path.join(tempdir, os.listdir(tempdir)[0])
            with open(java_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('Test_AllTypeFunctionCallDecodeUDF.java')

            self.assertEqual(required_content, content)

    def test_generate_event_udf(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtUDFGenerator()
            generator.generate_event_udf(
                udf_workspace=tempdir,
                contract_name='WyvernExchangeV2',
                event=[i for i in abi['events'] if i['name'] == 'OrderApprovedPartOne'][0]
            )

            java_filepath = os.path.join(tempdir, os.listdir(tempdir)[0])
            with open(java_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('WyvernExchangeV2_OrderApprovedPartOneEventDecodeUDF.java')

            self.assertEqual(required_content, content)
