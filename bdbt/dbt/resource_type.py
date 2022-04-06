from typing import Sequence

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class DbtMeta(TypedDict, total=False):
    type: str
    # event model or source only
    indexed: bool


class DbtColumn(TypedDict, total=False):
    name: str
    meta: DbtMeta


class DbtTable(TypedDict, total=False):
    name: str
    columns: Sequence[DbtColumn]


class DbtSource(TypedDict, total=False):
    name: str
    tables: Sequence[DbtTable]


class DbtModel(TypedDict, total=False):
    name: str
    columns: Sequence[DbtColumn]
