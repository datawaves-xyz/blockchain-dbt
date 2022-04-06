from typing import (
    Sequence,
    Union,
    Optional
)

try:
    from typing import TypedDict, Literal
except ImportError:
    from typing_extensions import TypedDict, Literal


# event
class ABIEventElement(TypedDict, total=False):
    indexed: bool
    name: str
    type: str


class ABIEvent(TypedDict, total=False):
    anonymous: bool
    inputs: Sequence[ABIEventElement]
    name: str
    type: Literal["event"]


# function
class ABICallElement(TypedDict, total=False):
    components: Optional[Sequence['ABICallElement']]
    name: str
    type: str


class ABICall(TypedDict, total=False):
    constant: bool
    name: str
    inputs: Sequence[ABICallElement]
    outputs: Sequence[ABICallElement]
    payable: bool
    stateMutability: Literal["pure", "view", "nonpayable", "payable"]
    type: Literal["function", "constructor", "fallback", "receive"]


ABIElement = Union[ABICall, ABIEvent]

# Internal ABI type for safer function parameters
ABI = Sequence[ABIElement]
