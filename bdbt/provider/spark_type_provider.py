import math
from typing import Union, List, Dict

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

from bdbt.abi.abi_data_type import (
    ABIFunctionType,
    ABIBytesType,
    ABIFixedType,
    ABIBoolType,
    ABIAddressType,
    ABIStringType,
    ABIIntType,
    ABITupleType,
    ABIArrayType,
    EventSchema,
    ABIField
)
from bdbt.dbt.resource_type import DbtModel, DbtColumn, DbtMeta, DbtTable, DbtSource
from bdbt.provider.data_type_provider import DataTypeProvider


class SparkDataTypeProvider(DataTypeProvider[DataType]):

    def get_type_name(self, data_type: DataType) -> str:
        return data_type.jsonValue()

    def transform_from_array_type(self, atype: ABIArrayType) -> ArrayType:
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


SparkField = ABIField[DataType]


class SparkEventSchema(EventSchema[DataType]):
    def __init__(self, inputs: List[SparkField]):
        super(SparkEventSchema, self).__init__(inputs)

    def to_dbt_model(self, contract_name: str, event_name: str) -> DbtModel:
        return DbtModel(
            name=f"{contract_name}_evt_{event_name}",
            columns=[self.field_to_dbt_column(i) for i in self.inputs]
        )

    def to_dbt_table(self, name: str) -> DbtTable:
        return DbtTable(
            name=name,
            columns=[self.field_to_dbt_column(i) for i in self.inputs]
        )

    @staticmethod
    def field_to_dbt_column(field: SparkField) -> DbtColumn:
        return DbtColumn(name=field.name, meta=DbtMeta(type=field.ftype.typeName()))

    @staticmethod
    def to_dbt_source(
            event_map: Dict[str, 'SparkEventSchema'],
            contract_name: str,
            database_name: str
    ) -> DbtSource:
        return DbtSource(
            name=database_name,
            tables=[schema.to_dbt_table(f"{contract_name}_evt_{event_name}")
                    for event_name, schema in event_map.items()]
        )
