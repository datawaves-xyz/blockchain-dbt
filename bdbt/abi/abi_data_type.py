from typing import (
    TypeVar,
    Generic,
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
    def __init__(self, canonical_type: str, name: str):
        self.canonical_type = canonical_type
        self.name = name


class ABIIntType(ABIDataType):
    def __init__(self, bit_length: int, unsigned: bool, *args, **kwargs):
        if bit_length > 256 or bit_length <= 0 or bit_length % 8 != 0:
            raise ABITypeNotValid(
                'The bit length of the integer type must less than or equal to 256, more than 0 and divisible by 8.')

        super(ABIIntType, self).__init__(
            canonical_type=f'{"u" if unsigned else ""}int{bit_length}',
            *args, **kwargs
        )
        self.bit_length = bit_length
        self.unsigned = unsigned


class ABIStringType(ABIDataType):
    def __init__(self, name: str = 'string'):
        super(ABIStringType, self).__init__(canonical_type='string', name=name)


class ABIAddressType(ABIDataType):
    """
    The address type equivalent to uint160 in the document, but we will transform to HexString to show.
    """

    def __init__(self):
        super(ABIAddressType, self).__init__(canonical_type='address', name='address')


class ABIBoolType(ABIDataType):
    def __init__(self):
        super(ABIBoolType, self).__init__(canonical_type='bool', name='bool')


class ABIFixedType(ABIDataType):
    def __init__(self, bit_length: int, scale: int, unsigned: bool, *args, **kwargs):
        if bit_length > 256 or bit_length < 8 or bit_length % 8 != 0:
            raise ABITypeNotValid(
                'The bit length of the fixed type must less than or equal to 256, more than 0 and divisible by 8.')

        if scale > 80 or scale <= 0:
            raise ABITypeNotValid(
                'The scale of the fixed type must less than or equal to 80 and more than 0.')

        super(ABIFixedType, self).__init__(
            canonical_type=f'{"u" if unsigned else ""}fixed{bit_length}x{scale}',
            *args, **kwargs
        )
        self.bit_length = bit_length
        self.scale = scale
        self.unsigned = unsigned


class ABIBytesType(ABIDataType):
    def __init__(self, length: Optional[int] = None, *args, **kwargs):
        if length is not None:
            if length > 32 or length <= 0:
                raise ABITypeNotValid(
                    'The length of bytes must les than or equal to 32 and more than 0.')

        super(ABIBytesType, self).__init__(
            canonical_type=f'bytes{"" if length is None else length}',
            *args, **kwargs
        )

        self.length = length


class ABIFunctionType(ABIBytesType):
    def __init__(self):
        # an address(20 bytes) followed by a function selector(4 bytes)
        super(ABIFunctionType, self).__init__(length=24, name='function')


class ABIArrayType(ABIDataType):
    def __init__(self, element_type: ABIDataType, length: int):
        super().__init__(canonical_type='array', name='array')

        self.length = length
        self.element_type = element_type


class ABITupleType(ABIDataType):
    def __init__(self, element_fields: List['ABIField[ABIDataType]']):
        super().__init__(canonical_type='tuple', name='tuple')

        self.element_fields = element_fields

    def add(self, field: 'ABIField'):
        self.element_fields.append(field)


T = TypeVar('T')
G = TypeVar('G')


class ABIField(Generic[G]):
    def __init__(self, name: str, ftype: G, metadata: Optional[Dict[str, any]] = None):
        self.name = name
        self.ftype = ftype
        self.metadata = metadata


class ABIEventSchema(TypedDict, total=False):
    inputs: List[ABIField[ABIDataType]]


class ABICallSchema(TypedDict, total=False):
    inputs: List[ABIField[ABIDataType]]
    outputs: List[ABIField[ABIDataType]]


class EventSchema(Generic[G]):
    def __init__(self, inputs: List[ABIField[G]]):
        self.inputs = inputs


class CallSchema(Generic[G]):
    def __init__(self, inputs: List[ABIField[G]], outputs: List[ABIField[G]]):
        self.inputs = inputs
        self.outputs = outputs


class ABISchema(Generic[G]):
    def __init__(self, events: List[EventSchema[G]], calls: List[CallSchema[G]]):
        self.event = events
        self.calls = calls
