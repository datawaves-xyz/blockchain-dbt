from typing import TypedDict, Optional

from bdbt.ethereum.abi.abi_type import ABI


class Contract(TypedDict, total=False):
    abi: ABI

    # If the address is null, SQL will match all contracts.
    address: Optional[str]

    name: str
    
    # table / increment
    materialize: str
