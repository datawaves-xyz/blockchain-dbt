from typing import Generic, TypeVar

from bdbt.abi.abi_data_type import (
    ABIIntType,
    ABIStringType,
    ABIAddressType,
    ABIBoolType,
    ABIFixedType,
    ABIBytesType,
    ABIFunctionType,
    ABIArrayType,
    ABITupleType,
    ABIDataType,
)

T = TypeVar('T')


class DataTypeProvider(Generic[T]):

    def transform(self, atype: ABIDataType) -> T:
        if isinstance(atype, ABIIntType):
            return self.transform_from_int_type(atype)
        elif isinstance(atype, ABIStringType):
            return self.transform_from_string_type(atype)
        elif isinstance(atype, ABIAddressType):
            return self.transform_from_address_type(atype)
        elif isinstance(atype, ABIBoolType):
            return self.transform_from_bool_type(atype)
        elif isinstance(atype, ABIFixedType):
            return self.transform_from_fixed_type(atype)
        elif isinstance(atype, ABIBytesType):
            return self.transform_from_bytes_type(atype)
        elif isinstance(atype, ABIFunctionType):
            return self.transform_from_function_type(atype)
        elif isinstance(atype, ABIArrayType):
            return self.transform_from_array_type(atype)
        elif isinstance(atype, ABITupleType):
            return self.transform_from_tuple_type(atype)

    def transform_from_int_type(self, atype: ABIIntType) -> T:
        raise NotImplementedError()

    def transform_from_string_type(self, atype: ABIStringType) -> T:
        raise NotImplementedError()

    def transform_from_address_type(self, atype: ABIAddressType) -> T:
        raise NotImplementedError()

    def transform_from_bool_type(self, atype: ABIBoolType) -> T:
        raise NotImplementedError()

    def transform_from_fixed_type(self, atype: ABIFixedType) -> T:
        raise NotImplementedError()

    def transform_from_bytes_type(self, atype: ABIBytesType) -> T:
        raise NotImplementedError()

    def transform_from_function_type(self, atype: ABIFunctionType) -> T:
        raise NotImplementedError()

    def transform_from_array_type(self, atype: ABIArrayType) -> T:
        raise NotImplementedError()

    def transform_from_tuple_type(self, atype: ABITupleType) -> T:
        raise NotImplementedError()

    def get_type_name(self, data_type: T) -> str:
        raise NotImplementedError()
