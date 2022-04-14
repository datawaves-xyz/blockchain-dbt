from dataclasses import dataclass
from typing import (
    Optional,
    List,
    Dict,
)

from bdbt.ethereum.abi.abi_type import ABIEvent, ABICall

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from bdbt.ethereum.exceptions import ABITypeNotValid


@dataclass
class ABIDataType:
    """
    Follow by: https://docs.soliditylang.org/en/v0.8.11/abi-spec.html#types
    """
    canonical_type: Optional[str] = None


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


@dataclass
class ABIField:
    name: str
    ftype: ABIDataType
    metadata: Optional[Dict[str, any]] = None


@dataclass
class ABIEventSchema:
    name: str
    inputs: List[ABIField]
    raw_schema: ABIEvent

    @property
    def is_empty(self):
        return len(self.inputs) == 0


@dataclass
class ABICallSchema:
    name: str
    inputs: List[ABIField]
    outputs: List[ABIField]
    raw_schema: ABICall

    @property
    def is_empty(self):
        return len(self.inputs) + len(self.outputs) == 0


@dataclass
class ABISchema:
    events: List[ABIEventSchema]
    calls: List[ABICallSchema]

    @property
    def nonempty_events(self):
        return [i for i in self.events if not i.is_empty]

    @property
    def nonempty_calls(self):
        return [i for i in self.calls if not i.is_empty]
