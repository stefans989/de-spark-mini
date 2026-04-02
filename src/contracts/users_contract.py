from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class DataContract:
    required_columns: List[str]
    column_types: Dict[str, str]
    strict: bool = True


users_contract = DataContract(
    required_columns=["user_id", "email", "country", "signup_date"],
    column_types={
        "user_id": "string",
        "email": "string",
        "country": "string",
        "signup_date": "string",
    },
    strict=True,
)
