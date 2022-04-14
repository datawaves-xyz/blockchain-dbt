import unittest
from typing import AnyStr, Dict

import test
from bdbt.ethereum.abi.abi_data_type import (
    ABIField,
    ABIBytesType,
    ABIArrayType,
    ABIIntType,
    ABIBoolType,
    ABITupleType,
    ABIFixedType
)
from bdbt.ethereum.abi.abi_transformer import ABITransformer
from bdbt.ethereum.abi.utils import normalize_abi

RESOURCE_GROUP = 'abi_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class ABITransformerTestCase(unittest.TestCase):

    def test_transform_abi_call(self):
        transformer = ABITransformer()
        abi = normalize_abi(_read_resource('abi1.json'))
        call_schema = transformer.transform_abi_call(abi=abi, call_name='AllTypeFunction')

        inputs_mapping_by_name: Dict[str, ABIField] = \
            {i.name: i for i in call_schema.inputs}

        self.assertEqual("address", inputs_mapping_by_name['addr'].ftype.canonical_type)
        self.assertEqual("int8", inputs_mapping_by_name['i8'].ftype.canonical_type)
        self.assertEqual("int16", inputs_mapping_by_name['i16'].ftype.canonical_type)
        self.assertEqual("int32", inputs_mapping_by_name['i32'].ftype.canonical_type)
        self.assertEqual("uint8", inputs_mapping_by_name['ui8'].ftype.canonical_type)
        self.assertEqual("uint16", inputs_mapping_by_name['ui16'].ftype.canonical_type)
        self.assertEqual("uint32", inputs_mapping_by_name['ui32'].ftype.canonical_type)
        self.assertEqual("uint64", inputs_mapping_by_name['ui64'].ftype.canonical_type)
        self.assertEqual("bool", inputs_mapping_by_name['bool'].ftype.canonical_type)
        self.assertEqual("string", inputs_mapping_by_name['str'].ftype.canonical_type)

        atype: ABIFixedType = inputs_mapping_by_name['f128x20'].ftype
        self.assertEqual("fixed128x20", atype.canonical_type)
        self.assertEqual(False, atype.unsigned)
        self.assertEqual(128, atype.bit_length)
        self.assertEqual(20, atype.scale)

        atype: ABIFixedType = inputs_mapping_by_name['uf128x20'].ftype
        self.assertEqual("ufixed128x20", atype.canonical_type)
        self.assertEqual(True, atype.unsigned)
        self.assertEqual(128, atype.bit_length)
        self.assertEqual(20, atype.scale)

        atype: ABIBytesType = inputs_mapping_by_name['bytes'].ftype
        self.assertEqual("bytes32", atype.canonical_type)
        self.assertEqual(32, atype.length)

        atype: ABIBytesType = inputs_mapping_by_name['bytes10'].ftype
        self.assertEqual("bytes10", atype.canonical_type)
        self.assertEqual(10, atype.length)

        atype: ABIArrayType = inputs_mapping_by_name['int8_array'].ftype
        self.assertEqual("int8[6]", atype.canonical_type)
        self.assertEqual(6, atype.length)
        etype: ABIIntType = atype.element_type
        self.assertEqual("int8", etype.canonical_type)
        self.assertEqual(False, etype.unsigned)
        self.assertEqual(8, etype.bit_length)

        atype: ABIArrayType = inputs_mapping_by_name['bytes12_array'].ftype
        self.assertEqual("bytes12[2]", atype.canonical_type)
        self.assertEqual(2, atype.length)
        etype: ABIBytesType = atype.element_type
        self.assertEqual("bytes12", etype.canonical_type)
        self.assertEqual(12, etype.length)

        atype: ABIArrayType = inputs_mapping_by_name['bool_array'].ftype
        self.assertEqual("bool[10]", atype.canonical_type)
        self.assertEqual(10, atype.length)
        etype: ABIBoolType = atype.element_type
        self.assertEqual("bool", etype.canonical_type)

        atype: ABITupleType = inputs_mapping_by_name['tuple'].ftype
        self.assertEqual(2, len(atype.element_fields))
        afield1 = atype.element_fields[0]
        self.assertEqual("value", afield1.name)
        self.assertEqual("uint256", afield1.ftype.canonical_type)
        afield2 = atype.element_fields[1]
        self.assertEqual("key", afield2.name)
        self.assertEqual("string", afield2.ftype.canonical_type)
