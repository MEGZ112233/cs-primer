from dataclasses import dataclass
from enum import Enum
from typing import Dict


@dataclass
class Schema:
    table_name: str
    columns: Dict[str, str]


class SortMergeActions(Enum):
    GET_LEFT_NO_MATCH = -1
    GET_RIGHT_NO_MATCH = 1
    GET_LEFT_MATCH = -2
    GET_RIGHT_MATCH = 2


class DataTypes(Enum):
    INT = "int32"
    STRING = "text"
    FLOAT = "float"
