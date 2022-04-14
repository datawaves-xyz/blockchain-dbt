from bdbt.ethereum.abi.abi_data_type import (
    ABIArrayType,
    ABIFunctionType,
    ABIBytesType,
    ABIFixedType,
    ABIBoolType,
    ABIAddressType,
    ABIStringType,
    ABIIntType,
    ABITupleType
)
from bdbt.ethereum.abi.provider.data_type_provider import DataTypeProvider


class HiveObjectInspectorTypeProvider(DataTypeProvider[str]):
    def transform_from_int_type(self, atype: ABIIntType) -> str:
        signed_bit_length = atype.bit_length + atype.unsigned

        if 0 < signed_bit_length <= 32:
            return 'PrimitiveObjectInspectorFactory.writableIntObjectInspector'
        elif 32 < signed_bit_length <= 64:
            return 'PrimitiveObjectInspectorFactory.writableLongObjectInspector'
        elif 64 < signed_bit_length:
            return 'PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector'

    def transform_from_string_type(self, atype: ABIStringType) -> str:
        return 'PrimitiveObjectInspectorFactory.writableStringObjectInspector'

    def transform_from_address_type(self, atype: ABIAddressType) -> str:
        return 'PrimitiveObjectInspectorFactory.writableStringObjectInspector'

    def transform_from_bool_type(self, atype: ABIBoolType) -> str:
        return 'PrimitiveObjectInspectorFactory.writableBooleanObjectInspector'

    def transform_from_fixed_type(self, atype: ABIFixedType) -> str:
        return 'PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector'

    def transform_from_bytes_type(self, atype: ABIBytesType) -> str:
        return 'PrimitiveObjectInspectorFactory.writableBinaryObjectInspector'

    def transform_from_function_type(self, atype: ABIFunctionType) -> str:
        return 'PrimitiveObjectInspectorFactory.writableBinaryObjectInspector'

    def transform_from_array_type(self, atype: ABIArrayType) -> str:
        return f'ObjectInspectorFactory.getStandardListObjectInspector({self.transform(atype.element_type)})'

    def transform_from_tuple_type(self, atype: ABITupleType) -> str:
        field_names = ','.join([f'"{i.name}"' for i in atype.element_fields])
        field_ois = ',\n'.join([self.transform(i.ftype) for i in atype.element_fields])

        return f"""ObjectInspectorFactory.getStandardStructObjectInspector(
            ImmutableList.of({field_names}),
            ImmutableList.of({field_ois})
        )
        """
