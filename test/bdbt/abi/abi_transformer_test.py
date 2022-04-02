import unittest
from typing import AnyStr, Dict

import test
from bdbt.abi.abi_data_type import ABIField, ABIDataType, ABIBytesType, ABIArrayType, ABIIntType, ABIBoolType, \
    ABITupleType, ABIFixedType
from bdbt.abi.abi_transformer import ABITransformer
from bdbt.abi.utils import normalize_abi
from bdbt.provider.data_type_provider import DataTypeProvider

RESOURCE_GROUP = 'abi_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class ABITransformerTestCase(unittest.TestCase):

    def test_transform_abi_call(self):
        transformer = ABITransformer(DataTypeProvider())
        abi = normalize_abi(_read_resource('abi1.json'))
        call_schema = transformer.transform_abi_call(abi=abi, call_name='AllTypeFunction')

        inputs_mapping_by_name: Dict[str, ABIField[ABIDataType]] = \
            {i.name: i for i in call_schema['inputs']}

        self.assertEqual("address", inputs_mapping_by_name['addr'].ftype.name)
        self.assertEqual("int8", inputs_mapping_by_name['i8'].ftype.name)
        self.assertEqual("int16", inputs_mapping_by_name['i16'].ftype.name)
        self.assertEqual("int32", inputs_mapping_by_name['i32'].ftype.name)
        self.assertEqual("uint8", inputs_mapping_by_name['ui8'].ftype.name)
        self.assertEqual("uint16", inputs_mapping_by_name['ui16'].ftype.name)
        self.assertEqual("uint32", inputs_mapping_by_name['ui32'].ftype.name)
        self.assertEqual("uint64", inputs_mapping_by_name['ui64'].ftype.name)
        self.assertEqual("bool", inputs_mapping_by_name['bool'].ftype.name)
        self.assertEqual("string", inputs_mapping_by_name['str'].ftype.name)

        atype: ABIFixedType = inputs_mapping_by_name['f128x20'].ftype
        self.assertEqual("fixed128x20", atype.name)
        self.assertEqual(False, atype.unsigned)
        self.assertEqual(128, atype.bit_length)
        self.assertEqual(20, atype.scale)

        atype: ABIFixedType = inputs_mapping_by_name['uf128x20'].ftype
        self.assertEqual("ufixed128x20", atype.name)
        self.assertEqual(True, atype.unsigned)
        self.assertEqual(128, atype.bit_length)
        self.assertEqual(20, atype.scale)

        atype: ABIBytesType = inputs_mapping_by_name['bytes'].ftype
        self.assertEqual("bytes", atype.name)
        self.assertIsNone(atype.length)

        atype: ABIBytesType = inputs_mapping_by_name['bytes10'].ftype
        self.assertEqual("bytes10", atype.name)
        self.assertEqual(10, atype.length)

        atype: ABIArrayType = inputs_mapping_by_name['int8_array'].ftype
        self.assertEqual("array", atype.name)
        self.assertEqual(6, atype.length)
        etype: ABIIntType = atype.element_type
        self.assertEqual("int8", etype.name)
        self.assertEqual(False, etype.unsigned)
        self.assertEqual(8, etype.bit_length)

        atype: ABIArrayType = inputs_mapping_by_name['bytes12_array'].ftype
        self.assertEqual("array", atype.name)
        self.assertEqual(2, atype.length)
        etype: ABIBytesType = atype.element_type
        self.assertEqual("bytes12", etype.name)
        self.assertEqual(12, etype.length)

        atype: ABIArrayType = inputs_mapping_by_name['bool_array'].ftype
        self.assertEqual("array", atype.name)
        self.assertEqual(10, atype.length)
        etype: ABIBoolType = atype.element_type
        self.assertEqual("bool", etype.name)

        atype: ABITupleType = inputs_mapping_by_name['tuple'].ftype
        self.assertEqual(2, len(atype.element_fields))
        afield1 = atype.element_fields[0]
        self.assertEqual("value", afield1.name)
        self.assertEqual("uint256", afield1.ftype.name)
        afield2 = atype.element_fields[1]
        self.assertEqual("key", afield2.name)
        self.assertEqual("string", afield2.ftype.name)
