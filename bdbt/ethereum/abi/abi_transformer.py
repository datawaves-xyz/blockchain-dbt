import re
from copy import deepcopy
from typing import List, Dict

from bdbt.ethereum.abi.abi_data_type import (
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
    ABISchema
)
from bdbt.ethereum.abi.abi_type import (
    ABIEventElement,
    ABI,
    ABIEvent,
    ABICall,
    ABICallElement
)
from bdbt.ethereum.abi.utils import filter_by_type_and_name, filter_by_type
from bdbt.ethereum.exceptions import TargetItemNotFound


class ABITransformer:
    abi_type_mapping: Dict[str, ABIDataType] = {}

    for i in range(8, 257, 8):
        abi_type_mapping[f'int{i}'] = ABIIntType(bit_length=i, unsigned=False)
        abi_type_mapping[f'uint{i}'] = ABIIntType(bit_length=i, unsigned=True)

        for j in range(1, 81):
            abi_type_mapping[f'fixed{i}x{j}'] = ABIFixedType(bit_length=i, scale=j, unsigned=False)
            abi_type_mapping[f'ufixed{i}x{j}'] = ABIFixedType(bit_length=i, scale=j, unsigned=True)

    for i in range(1, 33):
        abi_type_mapping[f'bytes{i}'] = ABIBytesType(length=i, dynamic=False)

    abi_type_mapping['int'] = abi_type_mapping['int256']
    abi_type_mapping['uint'] = abi_type_mapping['uint256']
    abi_type_mapping['fixed'] = abi_type_mapping['fixed128x18']
    abi_type_mapping['ufixed'] = abi_type_mapping['ufixed128x18']
    abi_type_mapping['address'] = ABIAddressType()
    abi_type_mapping['bool'] = ABIBoolType()
    abi_type_mapping['bytes'] = ABIBytesType(length=32, dynamic=True)
    abi_type_mapping['function'] = ABIFunctionType()
    abi_type_mapping['string'] = ABIStringType()

    def _transform_event_element(self, event_element: ABIEventElement) -> ABIField:
        name = event_element.get('name')
        atype_str = event_element.get('type')
        indexed = event_element.get('indexed')

        arr_reg = re.search(r'\[[\d]*\]$', atype_str)

        if arr_reg:
            element_type_str = atype_str[:arr_reg.start()]
            element_type = self.abi_type_mapping[element_type_str]
            arr_str = atype_str[arr_reg.start(): arr_reg.end()]
            arr_length = -1 if arr_str == '[]' else int(arr_str[1:-1])
            return ABIField(
                name=name,
                ftype=ABIArrayType(
                    element_type=element_type,
                    length=arr_length,
                    canonical_type=f'{element_type.canonical_type}{arr_str}'
                ),
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
            element_type = self.abi_type_mapping[element_type_str]
            arr_str = atype_str[arr_reg.start(): arr_reg.end()]
            arr_length = -1 if arr_str == '[]' else int(arr_str[1:-1])
            return ABIField(
                name=name,
                ftype=ABIArrayType(
                    element_type=element_type,
                    length=arr_length,
                    canonical_type=f'{element_type.canonical_type}{arr_str}'
                ),
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
        return ABIEventSchema(
            name=event.get('name'),
            inputs=self._revise_fields([self._transform_event_element(i) for i in event.get('inputs', [])]),
            raw_schema=event
        )

    def transform_abi_call(self, abi: ABI, call_name: str) -> ABICallSchema:
        candidate_calls: List[ABICall] = filter_by_type_and_name(name=call_name, type_str='function', contract_abi=abi)
        if len(candidate_calls) != 1:
            raise TargetItemNotFound(f"{call_name} call can not be found in ABI")

        call = candidate_calls[0]
        return ABICallSchema(
            name=call.get('name'),
            inputs=self._revise_fields([self._transform_call_element(i) for i in call.get('inputs', [])]),
            outputs=self._revise_fields([self._transform_call_element(i) for i in call.get('outputs', [])], 'output'),
            raw_schema=call
        )

    def transform_abi(self, abi: ABI) -> ABISchema:
        event_abi_list = filter_by_type(type_str='event', contract_abi=abi)
        call_abi_list = filter_by_type(type_str='function', contract_abi=abi)

        events = [self.transform_abi_event(abi, i['name']) for i in event_abi_list]
        calls = [self.transform_abi_call(abi, i['name']) for i in call_abi_list]
        return ABISchema(events=events, calls=calls)

    @staticmethod
    def _revise_fields(fields: List[ABIField], prefix: str = '') -> List[ABIField]:
        size = len(fields)

        if size == 0:
            return fields

        revised_fields = []
        for i in range(0, size):
            new_field = deepcopy(fields[i])
            if prefix == '':
                new_field.name = f'_{i}' if new_field.name == '' else new_field.name
            else:
                new_field.name = f'{prefix}_{i}' if new_field.name == '' else f'{prefix}_{new_field.name}'
            revised_fields.append(new_field)

        return revised_fields
