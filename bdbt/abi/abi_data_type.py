from typing import (
    Optional,
    List,
    Dict,
)

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from bdbt.exceptions import ABITypeNotValid


class ABIDataType:
    """
    Follow by: https://docs.soliditylang.org/en/v0.8.11/abi-spec.html#types
    """

    def __init__(self, canonical_type: Optional[str]):
        self.canonical_type = canonical_type


class ABIIntType(ABIDataType):
    def __init__(self, bit_length: int, unsigned: bool):
        if bit_length > 256 or bit_length <= 0 or bit_length % 8 != 0:
            raise ABITypeNotValid(
                'The bit length of the integer type must less than or equal to 256, more than 0 and divisible by 8.')

        super(ABIIntType, self).__init__(canonical_type=f'{"u" if unsigned else ""}int{bit_length}')
        self.bit_length = bit_length
        self.unsigned = unsigned


class ABIStringType(ABIDataType):
    def __init__(self):
        super(ABIStringType, self).__init__(canonical_type='string')


class ABIAddressType(ABIDataType):
    """
    The address type equivalent to uint160 in the document, but we will transform to HexString to show.
    """

    def __init__(self):
        super(ABIAddressType, self).__init__(canonical_type='address')


class ABIBoolType(ABIDataType):
    def __init__(self):
        super(ABIBoolType, self).__init__(canonical_type='bool')


class ABIFixedType(ABIDataType):
    def __init__(self, bit_length: int, scale: int, unsigned: bool):
        if bit_length > 256 or bit_length < 8 or bit_length % 8 != 0:
            raise ABITypeNotValid(
                'The bit length of the fixed type must less than or equal to 256, more than 0 and divisible by 8.')

        if scale > 80 or scale <= 0:
            raise ABITypeNotValid(
                'The scale of the fixed type must less than or equal to 80 and more than 0.')

        super(ABIFixedType, self).__init__(canonical_type=f'{"u" if unsigned else ""}fixed{bit_length}x{scale}')
        self.bit_length = bit_length
        self.scale = scale
        self.unsigned = unsigned


class ABIByteType(ABIDataType):
    def __init__(self):
        super(ABIByteType, self).__init__(canonical_type='byte')


class ABIArrayType(ABIDataType):
    def __init__(self, canonical_type: str, element_type: ABIDataType, length: int):
        super(ABIArrayType, self).__init__(canonical_type=canonical_type)

        self.length = length
        self.element_type = element_type


class ABIBytesType(ABIArrayType):
    def __init__(self, length: int, dynamic: bool):
        if length > 32 or length <= 0:
            raise ABITypeNotValid(
                'The length of bytes must les than or equal to 32 and more than 0.')

        super(ABIBytesType, self).__init__(
            canonical_type=f'bytes{length}',
            element_type=ABIByteType(),
            length=length
        )

        self.dynamic = dynamic


class ABIFunctionType(ABIArrayType):
    def __init__(self):
        super(ABIFunctionType, self).__init__(
            canonical_type='function',
            element_type=ABIByteType(),
            length=24
        )


class ABITupleType(ABIDataType):
    def __init__(self, element_fields: List['ABIField']):
        super().__init__(canonical_type='tuple')

        self.element_fields = element_fields

    def add(self, field: 'ABIField'):
        self.element_fields.append(field)


class ABIField:
    def __init__(self, name: str, ftype: ABIDataType, metadata: Optional[Dict[str, any]] = None):
        self.name = name
        self.ftype = ftype
        self.metadata = metadata


class ABIEventSchema(TypedDict, total=False):
    name: str
    inputs: List[ABIField]


class ABICallSchema(TypedDict, total=False):
    name: str
    inputs: List[ABIField]
    outputs: List[ABIField]


class ABISchema(TypedDict, total=False):
    events: List[ABIEventSchema]
    calls: List[ABICallSchema]
