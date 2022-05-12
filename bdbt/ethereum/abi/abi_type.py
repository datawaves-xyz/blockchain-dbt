from dataclasses import dataclass, field
from typing import (
    Union,
    Optional, List, Dict
)

from mashumaro import DataClassDictMixin
from mashumaro.config import BaseConfig, TO_DICT_ADD_OMIT_NONE_FLAG

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal


@dataclass(frozen=True)
class ABIEventElement(DataClassDictMixin):
    indexed: bool
    name: str
    type: str
    internalType: Optional[str] = None

    class Config(BaseConfig):
        code_generation_options = [TO_DICT_ADD_OMIT_NONE_FLAG]


@dataclass(frozen=True)
class ABIEvent(DataClassDictMixin):
    anonymous: bool
    inputs: List[ABIEventElement]
    name: str
    type: Literal["event"]

    class Config(BaseConfig):
        code_generation_options = [TO_DICT_ADD_OMIT_NONE_FLAG]


@dataclass(frozen=True)
class ABICallElement(DataClassDictMixin):
    name: str
    type: str
    components: Optional[List['ABICallElement']] = None

    class Config(BaseConfig):
        code_generation_options = [TO_DICT_ADD_OMIT_NONE_FLAG]


@dataclass(frozen=True)
class ABICall(DataClassDictMixin):
    type: Literal["function", "constructor", "fallback", "receive"]

    # there are None when the type is constructor
    name: Optional[str] = None
    constant: Optional[bool] = None
    payable: Optional[bool] = None
    stateMutability: Optional[Literal["pure", "view", "nonpayable", "payable"]] = None

    inputs: List[ABICallElement] = field(default_factory=list)
    outputs: List[ABICallElement] = field(default_factory=list)

    class Config(BaseConfig):
        code_generation_options = [TO_DICT_ADD_OMIT_NONE_FLAG]


ABIElement = Union[ABICall, ABIEvent]


@dataclass(frozen=True)
class ABI(DataClassDictMixin):
    elements: List[ABIElement]

    @classmethod
    def from_dicts(
            cls, abi: List[Dict]
    ) -> "ABI":
        return cls(
            elements=[ABIEvent.from_dict(i) if 'anonymous' in i else ABICall.from_dict(i)
                      for i in abi]
        )
