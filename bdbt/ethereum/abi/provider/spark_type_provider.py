import math
from typing import Union

from pyspark.sql.types import (
    DataType,
    IntegerType,
    LongType,
    DecimalType,
    StringType,
    BooleanType,
    BinaryType,
    ArrayType,
    StructType,
    StructField
)

from bdbt.ethereum.abi.abi_data_type import (
    ABIFunctionType,
    ABIBytesType,
    ABIFixedType,
    ABIBoolType,
    ABIAddressType,
    ABIStringType,
    ABIIntType,
    ABITupleType,
    ABIArrayType
)
from bdbt.ethereum.abi.provider.data_type_provider import DataTypeProvider


class SparkDataTypeProvider(DataTypeProvider[DataType]):

    def transform_from_array_type(self, atype: ABIArrayType, deep: int = 0) -> ArrayType:
        return ArrayType(elementType=self.transform(atype.element_type))

    def transform_from_tuple_type(self, atype: ABITupleType) -> StructType:
        return StructType(fields=[StructField(
            name=i.name,
            dataType=self.transform(i.ftype),
            metadata=i.metadata
        ) for i in atype.element_fields])

    def transform_from_int_type(self, atype: ABIIntType) -> Union[IntegerType, LongType, DecimalType]:
        signed_bit_length = atype.bit_length + atype.unsigned

        # Ignore ByteType(8bit) and ShortType(16bit) for match the type in the headlong.
        if 0 < signed_bit_length <= 32:
            return IntegerType()
        elif 32 < signed_bit_length <= 64:
            return LongType()
        elif 64 < signed_bit_length:
            return DecimalType(38, 0)

    def transform_from_string_type(self, atype: ABIStringType) -> StringType:
        return StringType()

    def transform_from_address_type(self, atype: ABIAddressType) -> StringType:
        return StringType()

    def transform_from_bool_type(self, atype: ABIBoolType) -> BooleanType:
        return BooleanType()

    def transform_from_fixed_type(self, atype: ABIFixedType) -> DecimalType:
        signed_bit_length = atype.bit_length + atype.unsigned
        # precision = log10(2^bit_length) + 1
        # The precision can be up to 38, scale can also be up to 38 (less or equal to precision) in Spark
        # doc: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DecimalType.html
        precision = min(38, math.floor(signed_bit_length / math.log2(10)) + 1)
        return DecimalType(precision=min(38, math.floor(signed_bit_length / math.log2(10)) + 1),
                           scale=min(precision, atype.scale))

    def transform_from_bytes_type(self, atype: ABIBytesType) -> BinaryType:
        return BinaryType()

    def transform_from_function_type(self, atype: ABIFunctionType) -> BinaryType:
        return BinaryType()
