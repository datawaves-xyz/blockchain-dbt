import unittest
from typing import AnyStr

import test
from bdbt.abi.abi_type import ABI, ABICall, ABICallElement
from bdbt.abi.utils import (
    normalize_abi,
    filter_by_name,
    filter_by_type
)

RESOURCE_GROUP = 'abi_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class UtilsTestCase(unittest.TestCase):

    def test_normalize_abi_for_function(self):
        abi: ABI = normalize_abi(_read_resource('abi1.json'))

        func: ABICall = abi[0]
        self.assertEqual('function', func['type'])
        self.assertEqual('AllTypeFunction', func['name'])
        self.assertEqual(True, func['constant'])
        self.assertEqual([], func['outputs'])

        self.assertEqual(19, len(func['inputs']))

        input1: ABICallElement = func['inputs'][0]
        self.assertEqual('addr', input1['name'])
        self.assertEqual('address', input1['type'])

        input18: ABICallElement = func['inputs'][18]
        self.assertEqual(2, len(input18['components']))
        c1: ABICallElement = input18['components'][0]
        self.assertEqual('value', c1['name'])

    def test_filter_by_name(self):
        abi: ABI = normalize_abi(_read_resource('abi1.json'))

        filtered = filter_by_name('AllTypeFunction', abi)
        self.assertEqual(1, len(filtered))
        self.assertEqual('AllTypeFunction', filtered[0].get('name'))

        filtered = filter_by_name('not_exists', abi)
        self.assertEqual(0, len(filtered))

    def test_filter_by_type(self):
        abi: ABI = normalize_abi(_read_resource('abi1.json'))

        filtered = filter_by_type('function', abi)
        self.assertEqual(1, len(filtered))
        self.assertEqual('AllTypeFunction', filtered[0].get('name'))

        filtered = filter_by_name('event', abi)
        self.assertEqual(0, len(filtered))
