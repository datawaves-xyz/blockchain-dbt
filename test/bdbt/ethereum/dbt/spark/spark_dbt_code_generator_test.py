import os
import pathlib
import shutil
import tempfile
import unittest
from typing import AnyStr

import test
from bdbt.content import Contract
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
            shutil.copyfile(_get_resource_path('dbt_project.yml'), os.path.join(tempdir, 'dbt_project.yml'))
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.gen_udf_for_dbt(
                dbt_dir=tempdir,
                abi_map={'opensea': {'WyvernExchangeV2': abi}},
                version='0.1.0'
            )

            self.assertEqual(2, len(os.listdir(tempdir)))
            self.assertTrue('blockchain-dbt-udf-0.1.0.jar' in os.listdir(tempdir))

    def test_generate_call_udf(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('full_types_function_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.gen_call_udf(
                udf_workspace=tempdir,
                project_name='test',
                contract_name='Test',
                call=[i for i in abi.calls if i.name == 'AllTypeFunction'][0]
            )

            java_filepath = os.path.join(tempdir, os.listdir(tempdir)[0])
            with open(java_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('Test_Test_AllTypeFunctionCallDecodeUDF.java')

            self.assertEqual(required_content, content)

    def test_generate_event_udf(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.gen_event_udf(
                udf_workspace=tempdir,
                project_name='opensea',
                contract_name='WyvernExchangeV2',
                event=[i for i in abi.events if i.name == 'OrderApprovedPartOne'][0]
            )

            java_filepath = os.path.join(tempdir, os.listdir(tempdir)[0])
            with open(java_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('Opensea_WyvernExchangeV2_OrderApprovedPartOneEventDecodeUDF.java')

            self.assertEqual(required_content, content)

    def test_generate_dbt_models(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)
            contract = Contract(
                name='WyvernExchangeV2',
                address='0x7f268357a8c2552623316e2562d90e642bb538e5',
                materialize='increment',
                abi=raw_abi
            )

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.gen_models_for_project(
                workspace=tempdir,
                project_name='opensea',
                contract=contract,
                abi=abi,
                version='0.1.0'
            )

            self.assertEqual(1, len(os.listdir(tempdir)))
            self.assertEqual('opensea', os.listdir(tempdir)[0])

            project_path = os.path.join(tempdir, 'opensea')

            self.maxDiff = None
            # Test that sequence first contains the same elements as second, regardless of their order.
            # When they don’t, an error message listing the differences between the sequences will be generated.
            self.assertCountEqual(
                ['opensea_WyvernExchangeV2_call_staticCall.sql',
                 'opensea_WyvernExchangeV2_call_minimumMakerProtocolFee.sql',
                 'opensea_WyvernExchangeV2_evt_OrderCancelled.sql',
                 'opensea_WyvernExchangeV2_call_cancelledOrFinalized.sql',
                 'opensea_WyvernExchangeV2_call_cancelOrderWithNonce_.sql',
                 'opensea_WyvernExchangeV2_call_DOMAIN_SEPARATOR.sql',
                 'opensea_WyvernExchangeV2_evt_OrderApprovedPartOne.sql',
                 'opensea_WyvernExchangeV2_call_guardedArrayReplace.sql',
                 'opensea_WyvernExchangeV2_call_validateOrderParameters_.sql',
                 'opensea_WyvernExchangeV2_call_name.sql',
                 'opensea_WyvernExchangeV2_call_minimumTakerProtocolFee.sql',
                 'opensea_WyvernExchangeV2_call_owner.sql',
                 'opensea_WyvernExchangeV2_call_protocolFeeRecipient.sql',
                 'opensea_WyvernExchangeV2_call_exchangeToken.sql',
                 'opensea_WyvernExchangeV2_call_calculateMatchPrice_.sql',
                 'opensea_WyvernExchangeV2_call_version.sql',
                 'opensea_WyvernExchangeV2_call_approvedOrders.sql',
                 'opensea_WyvernExchangeV2_call_approveOrder_.sql',
                 'opensea_WyvernExchangeV2_call_incrementNonce.sql',
                 'opensea_WyvernExchangeV2_call_transferOwnership.sql',
                 'opensea_WyvernExchangeV2_evt_OwnershipRenounced.sql',
                 'opensea_WyvernExchangeV2_call_registry.sql',
                 'opensea_WyvernExchangeV2_call_codename.sql',
                 'opensea_WyvernExchangeV2_call_renounceOwnership.sql',
                 'opensea_WyvernExchangeV2_call_ordersCanMatch_.sql',
                 'opensea_WyvernExchangeV2_call_calculateCurrentPrice_.sql',
                 'opensea_WyvernExchangeV2_call_tokenTransferProxy.sql',
                 'opensea_WyvernExchangeV2_call_hashToSign_.sql',
                 'opensea_WyvernExchangeV2_call_changeProtocolFeeRecipient.sql',
                 'opensea_WyvernExchangeV2_call_orderCalldataCanMatch.sql',
                 'opensea_WyvernExchangeV2_evt_OwnershipTransferred.sql',
                 'opensea_WyvernExchangeV2_call_nonces.sql',
                 'opensea_WyvernExchangeV2_evt_OrdersMatched.sql',
                 'opensea_WyvernExchangeV2_evt_NonceIncremented.sql',
                 'opensea_WyvernExchangeV2_call_calculateFinalPrice.sql',
                 'opensea_WyvernExchangeV2_call_cancelOrder_.sql',
                 'opensea_WyvernExchangeV2_call_changeMinimumMakerProtocolFee.sql',
                 'opensea_WyvernExchangeV2_call_changeMinimumTakerProtocolFee.sql',
                 'opensea_WyvernExchangeV2_call_atomicMatch_.sql',
                 'opensea_WyvernExchangeV2_evt_OrderApprovedPartTwo.sql',
                 'opensea_WyvernExchangeV2_call_INVERSE_BASIS_POINT.sql',
                 'opensea_WyvernExchangeV2_call_validateOrder_.sql',
                 'opensea_WyvernExchangeV2_call_hashOrder_.sql'],
                os.listdir(project_path)
            )

    def test_generate_event_model(self):
        with tempfile.TemporaryDirectory() as tempdir:
            transformer = ABITransformer()
            raw_abi = normalize_abi(_read_resource('wyvern_exchange_v2_abi.json'))
            abi = transformer.transform_abi(abi=raw_abi)
            project_path = os.path.join(tempdir, 'opensea')
            pathlib.Path(project_path).mkdir()
            contract = Contract(
                name='WyvernExchangeV2',
                address='0x7f268357a8c2552623316e2562d90e642bb538e5',
                materialize='table',
                abi=raw_abi
            )

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.gen_event_dbt_model(
                project_path=project_path,
                contract=contract,
                version='0.1.0',
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
            contract = Contract(
                name='WyvernExchangeV2',
                address='0x7f268357a8c2552623316e2562d90e642bb538e5',
                materialize='increment',
                abi=raw_abi
            )

            generator = SparkDbtCodeGenerator(self.remote_workspace)
            generator.gen_call_dbt_model(
                project_path=project_path,
                contract=contract,
                version='0.1.0',
                call=[i for i in abi.calls if i.name == 'atomicMatch_'][0]
            )

            model_filepath = os.path.join(project_path, os.listdir(project_path)[0])
            with open(model_filepath, 'r') as f:
                content = f.read()

            required_content = _read_resource('WyvernExchange_call_atomicMatch_dbt_sql')

            self.assertEqual(required_content, content)
