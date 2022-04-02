import unittest
from typing import AnyStr

import pyaml

import test
from bdbt.abi.abi_transformer import ABITransformer
from bdbt.abi.utils import normalize_abi
from bdbt.dbt.dbt_transformer import DbtTransformer
from bdbt.provider.spark_type_provider import SparkDataTypeProvider

RESOURCE_GROUP = 'abi_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class SmokeTestCase(unittest.TestCase):
    def test_call_to_spark_dbt_model(self):
        contract_name = "Test"
        call_name = "AllTypeFunction"

        abi = normalize_abi(_read_resource('abi1.json'))
        provider = SparkDataTypeProvider()
        abi_transformer = ABITransformer(provider=provider)
        dbt_transformer = DbtTransformer(provider=provider)

        abi_call_schema = abi_transformer.transform_abi_call(abi=abi, call_name="AllTypeFunction")
        call_schema = abi_transformer.transform_to_call_schema(abi_call_schema)
        model = dbt_transformer.call_schema_to_model(
            call=call_schema,
            contract_name=contract_name,
            call_name=call_name
        )

        print(pyaml.print(model))

        self.assertIsNotNone(model)
