from dataclasses import dataclass
from typing import Dict 

@dataclass
class Schema:
    table_name : str
    columns : Dict[str , str]