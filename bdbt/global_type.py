from dataclasses import dataclass
from enum import Enum
from typing import Sequence, List, Optional, Mapping

from mashumaro import DataClassDictMixin

from bdbt.ethereum.abi.abi_type import ABI


@dataclass(frozen=True)
class DbtColumn(DataClassDictMixin):
    name: str


@dataclass(frozen=True)
class DbtTable(DataClassDictMixin):
    name: str
    columns: Sequence[DbtColumn]


@dataclass(frozen=True)
class DbtModelSchema(DataClassDictMixin):
    models: List[DbtTable]


class Database(Enum):
    SPARK = 'spark'
    BIG_QUERY = 'big_query'
    REDSHIFT = 'redshift'
    SNOWFLAKE = 'snowflake'
    POSTGRES = 'postgres'


@dataclass(frozen=True)
class Contract(DataClassDictMixin):
    abi: ABI
    name: str
    # table / increment
    materialize: str
    # If the address is null, SQL will match all contracts.
    address: Optional[str] = None

    @classmethod
    def from_dicts(
            cls, d: Mapping,
    ) -> "Contract":
        return cls(
            abi=ABI.from_dicts(d['abi']),
            name=d['name'],
            materialize=d['materialize'],
            address=d.get('address')
        )
