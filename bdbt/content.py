from typing import TypedDict

from bdbt.ethereum.abi.abi_type import ABI


class Contract(TypedDict, total=False):
    abi: ABI
    address: str
    name: str
    # table / increment
    materialize: str
