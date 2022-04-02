import re
from typing import Generic, List, Dict, TypeVar

from bdbt.abi.abi_data_type import (
    ABIDataType,
    ABIIntType,
    ABIAddressType,
    ABIFixedType,
    ABIBoolType,
    ABIBytesType,
    ABIFunctionType,
    ABIStringType,
    ABIField,
    ABIArrayType,
    ABITupleType,
    ABIEventSchema,
    ABICallSchema,
    EventSchema,
    CallSchema
)
from bdbt.abi.abi_type import (
    ABIEventElement,
    ABI,
    ABIEvent,
    ABICall,
    ABICallElement
)
from bdbt.abi.utils import filter_by_type_and_name
from bdbt.exceptions import TargetItemNotFound
from bdbt.provider.data_type_provider import DataTypeProvider

T = TypeVar('T')


class ABITransformer(Generic[T]):
    abi_types: List[ABIDataType] = [
        ABIIntType(bit_length=256, unsigned=False, name='int'),
        ABIIntType(bit_length=256, unsigned=True, name='uint'),
        ABIAddressType(),
        ABIBoolType(),
        ABIFixedType(bit_length=128, scale=18, unsigned=False, name='fixed'),
        ABIFixedType(bit_length=128, scale=18, unsigned=True, name='ufixed'),
        ABIBytesType(name='bytes'),
        ABIFunctionType(),
        ABIStringType()
    ]

    for i in range(8, 257, 8):
        abi_types.append(ABIIntType(bit_length=i, unsigned=False, name=f'int{i}'))
        abi_types.append(ABIIntType(bit_length=i, unsigned=True, name=f'uint{i}'))

        for j in range(1, 81):
            abi_types.append(ABIFixedType(bit_length=i, scale=j, unsigned=False, name=f'fixed{i}x{j}'))
            abi_types.append(ABIFixedType(bit_length=i, scale=j, unsigned=True, name=f'ufixed{i}x{j}'))

    for i in range(1, 33):
        abi_types.append(ABIBytesType(length=i, name=f'bytes{i}'))

    abi_type_mapping: Dict[str, ABIDataType] = {i.name: i for i in abi_types}

    def __init__(self, provider: DataTypeProvider[T]):
        self.provider = provider

    def _transform_event_element(self, event_element: ABIEventElement) -> ABIField:
        name = event_element['name']
        atype_str = event_element['type']
        indexed = event_element['indexed']

        arr_reg = re.search(r'\[[\d]*\]$', atype_str)

        if arr_reg:
            element_type_str = atype_str[:arr_reg.start()]
            arr_length = int(atype_str[arr_reg.start() + 1: arr_reg.end()])
            return ABIField(
                name=name,
                ftype=ABIArrayType(element_type=self.abi_type_mapping[element_type_str], length=arr_length),
                metadata={'indexed': indexed}
            )
        else:
            return ABIField(
                name=name,
                ftype=self.abi_type_mapping[atype_str],
                metadata={'indexed': indexed}
            )

    def _transform_call_element(self, function_element: ABICallElement) -> ABIField:
        name = function_element.get('name')
        atype_str = function_element.get('type')
        components = function_element.get('components')

        arr_reg = re.search(r'\[[\d]*\]$', atype_str)

        if components is not None:
            return ABIField(
                name=name,
                ftype=ABITupleType([self._transform_call_element(i) for i in components])
            )
        elif arr_reg:
            element_type_str = atype_str[:arr_reg.start()]
            arr_length = int(atype_str[arr_reg.start() + 1: arr_reg.end() - 1])
            return ABIField(
                name=name,
                ftype=ABIArrayType(element_type=self.abi_type_mapping[element_type_str], length=arr_length),
            )
        else:
            return ABIField(
                name=name,
                ftype=self.abi_type_mapping[atype_str],
            )

    def transform_abi_event(self, abi: ABI, event_name: str) -> ABIEventSchema:
        candidate_events: List[ABIEvent] = filter_by_type_and_name(name=event_name, type_str='event', contract_abi=abi)
        if len(candidate_events) != 1:
            raise TargetItemNotFound(f"{event_name} event can not be found in ABI")

        event = candidate_events[0]
        return ABIEventSchema(inputs=[self._transform_event_element(i) for i in event.get('inputs', [])])

    def transform_abi_call(self, abi: ABI, call_name: str) -> ABICallSchema:
        candidate_calls: List[ABICall] = filter_by_type_and_name(name=call_name, type_str='function', contract_abi=abi)
        if len(candidate_calls) != 1:
            raise TargetItemNotFound(f"{call_name} call can not be found in ABI")

        call = candidate_calls[0]
        return ABICallSchema(
            inputs=[self._transform_call_element(i) for i in call.get('inputs', [])],
            outputs=[self._transform_call_element(i) for i in call.get('outputs', [])]
        )

    def transform_to_event_schema(self, abi_event: ABIEventSchema) -> EventSchema[T]:
        return EventSchema(inputs=[ABIField(
            name=i.name,
            ftype=self.provider.transform(i.ftype),
            metadata=i.metadata
        ) for i in abi_event['inputs']])

    def transform_to_call_schema(self, abi_call: ABICallSchema) -> CallSchema[T]:
        return CallSchema(
            inputs=[ABIField(
                name=i.name,
                ftype=self.provider.transform(i.ftype),
                metadata=i.metadata
            ) for i in abi_call['inputs']],

            outputs=[ABIField(
                name=i.name,
                ftype=self.provider.transform(i.ftype),
                metadata=i.metadata
            ) for i in abi_call['outputs']]
        )
