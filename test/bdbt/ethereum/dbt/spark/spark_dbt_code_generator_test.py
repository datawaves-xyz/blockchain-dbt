import os
import pathlib
import tempfile
import unittest
from typing import AnyStr

import test
from bdbt.ethereum.abi.abi_transformer import ABITransformer
from bdbt.ethereum.abi.utils import normalize_abi
from bdbt.ethereum.dbt.spark.spark_dbt_code_generator import SparkDbtCodeGenerator

RESOURCE_GROUP = 'dbt_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class SparkDbtCodeGeneratorTestCase(unittest.TestCase):
    remote_workspace = 's3a://test'

    def test_generate_dbt_udf(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.generate_dbt_udf(
                workspace=tempdir,
                project_name='opensea',
                contract_name_to_abi={'WyvernExchangeV2': abi}
            )

            self.assertEqual(1, len(os.listdir(tempdir)))
            self.assertEqual('opensea', os.listdir(tempdir)[0])

            project_path = os.path.join(tempdir, 'opensea')
            self.assertEqual(1, len(os.listdir(project_path)))
            self.assertEqual('opensea_udf.jar', os.listdir(project_path)[0])

    def test_generate_call_udf(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('full_types_function_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.generate_call_udf(
                udf_workspace=tempdir,
                contract_name='Test',
                call=[i for i in abi.calls if i.name == 'AllTypeFunction'][0]
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

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.generate_event_udf(
                udf_workspace=tempdir,
                contract_name='WyvernExchangeV2',
                event=[i for i in abi.events if i.name == 'OrderApprovedPartOne'][0]
            )

            java_filepath = os.path.join(tempdir, os.listdir(tempdir)[0])
            with open(java_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('WyvernExchangeV2_OrderApprovedPartOneEventDecodeUDF.java')

            self.assertEqual(required_content, content)

    def test_generate_dbt_models(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.generate_dbt_models(
                workspace=tempdir,
                project_name='opensea',
                contract_name='WyvernExchangeV2',
                contract_address='0x7f268357a8c2552623316e2562d90e642bb538e5',
                abi=abi
            )

            self.assertEqual(1, len(os.listdir(tempdir)))
            self.assertEqual('opensea', os.listdir(tempdir)[0])

            project_path = os.path.join(tempdir, 'opensea')

            self.maxDiff = None
            self.assertListEqual(
                ['WyvernExchangeV2_call_staticCall.sql',
                 'WyvernExchangeV2_call_minimumMakerProtocolFee.sql',
                 'WyvernExchangeV2_evt_OrderCancelled.sql',
                 'WyvernExchangeV2_call_cancelledOrFinalized.sql',
                 'WyvernExchangeV2_call_cancelOrderWithNonce_.sql',
                 'WyvernExchangeV2_call_DOMAIN_SEPARATOR.sql',
                 'WyvernExchangeV2_evt_OrderApprovedPartOne.sql',
                 'WyvernExchangeV2_call_guardedArrayReplace.sql',
                 'WyvernExchangeV2_call_validateOrderParameters_.sql',
                 'WyvernExchangeV2_call_name.sql',
                 'WyvernExchangeV2_call_minimumTakerProtocolFee.sql',
                 'WyvernExchangeV2_call_owner.sql',
                 'WyvernExchangeV2_call_protocolFeeRecipient.sql',
                 'WyvernExchangeV2_call_exchangeToken.sql',
                 'WyvernExchangeV2_call_calculateMatchPrice_.sql',
                 'WyvernExchangeV2_call_version.sql',
                 'WyvernExchangeV2_call_approvedOrders.sql',
                 'WyvernExchangeV2_call_approveOrder_.sql',
                 'WyvernExchangeV2_call_incrementNonce.sql',
                 'WyvernExchangeV2_call_transferOwnership.sql',
                 'WyvernExchangeV2_evt_OwnershipRenounced.sql',
                 'WyvernExchangeV2_call_registry.sql',
                 'WyvernExchangeV2_call_codename.sql',
                 'WyvernExchangeV2_call_renounceOwnership.sql',
                 'WyvernExchangeV2_call_ordersCanMatch_.sql',
                 'WyvernExchangeV2_call_calculateCurrentPrice_.sql',
                 'WyvernExchangeV2_call_tokenTransferProxy.sql',
                 'WyvernExchangeV2_call_hashToSign_.sql',
                 'WyvernExchangeV2_call_changeProtocolFeeRecipient.sql',
                 'WyvernExchangeV2_call_orderCalldataCanMatch.sql',
                 'WyvernExchangeV2_evt_OwnershipTransferred.sql',
                 'WyvernExchangeV2_call_nonces.sql',
                 'WyvernExchangeV2_evt_OrdersMatched.sql',
                 'WyvernExchangeV2_evt_NonceIncremented.sql',
                 'WyvernExchangeV2_call_calculateFinalPrice.sql',
                 'WyvernExchangeV2_call_cancelOrder_.sql',
                 'WyvernExchangeV2_call_changeMinimumMakerProtocolFee.sql',
                 'WyvernExchangeV2_call_changeMinimumTakerProtocolFee.sql',
                 'WyvernExchangeV2_call_atomicMatch_.sql',
                 'WyvernExchangeV2_evt_OrderApprovedPartTwo.sql',
                 'WyvernExchangeV2_call_INVERSE_BASIS_POINT.sql',
                 'WyvernExchangeV2_call_validateOrder_.sql',
                 'WyvernExchangeV2_call_hashOrder_.sql'],
                os.listdir(project_path)
            )

    def test_generate_event_model(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)
            project_path = os.path.join(tempdir, 'opensea')
            pathlib.Path(project_path).mkdir()

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.generate_event_dbt_model(
                project_path=project_path,
                contract_name='WyvernExchangeV2',
                contract_address='0x7f268357a8c2552623316e2562d90e642bb538e5',
                event=[i for i in abi.events if i.name == 'OrderApprovedPartOne'][0]
            )

            model_filepath = os.path.join(project_path, os.listdir(project_path)[0])
            with open(model_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('WyvernExchange_evt_OrderApprovedPartOne_dbt_sql')

            self.assertEqual(required_content, content)

    def test_generate_call_model(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)
            project_path = os.path.join(tempdir, 'opensea')
            pathlib.Path(project_path).mkdir()

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.generate_call_dbt_model(
                project_path=project_path,
                contract_name='WyvernExchangeV2',
                contract_address='0x7f268357a8c2552623316e2562d90e642bb538e5',
                call=[i for i in abi.calls if i.name == 'atomicMatch_'][0]
            )

            model_filepath = os.path.join(project_path, os.listdir(project_path)[0])
            with open(model_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('WyvernExchange_call_atomicMatch_dbt_sql')

            self.assertEqual(required_content, content)
