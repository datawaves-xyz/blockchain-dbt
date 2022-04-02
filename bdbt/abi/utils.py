import json
from typing import cast, List

from bdbt.abi.abi_type import ABI, ABIElement


def normalize_abi(abi_json_str: str) -> ABI:
    """
    Convert a json ABI string to an :class:`ABI` object which uses TypedDict
    """
    abi_json_str = json.loads(abi_json_str)
    return cast(ABI, abi_json_str)


def filter_by_type(type_str: str, contract_abi: ABI) -> List[ABIElement]:
    return [abi for abi in contract_abi if abi['type'] == type_str]


def filter_by_name(name: str, contract_abi: ABI) -> List[ABIElement]:
    return [abi for abi in contract_abi
            if (abi['type'] not in ('fallback', 'constructor', 'receive') and abi['name'] == name)]


def filter_by_type_and_name(name: str, type_str: str, contract_abi: ABI) -> List[ABIElement]:
    return [abi for abi in contract_abi if abi['type'] == type_str and abi['name'] == name]
