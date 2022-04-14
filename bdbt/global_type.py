from enum import Enum
from typing import Sequence, List

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class DbtColumn(TypedDict, total=False):
    name: str


class DbtTable(TypedDict, total=False):
    name: str
    columns: Sequence[DbtColumn]


class DbtModelSchema(TypedDict, total=False):
    models: List[DbtTable]


class Database(Enum):
    SPARK = 'spark'
    BIG_QUERY = 'big_query'
    REDSHIFT = 'redshift'
    SNOWFLAKE = 'snowflake'
    POSTGRES = 'postgres'
