"""
Turns a Salesforce Bulk API v2 CSV result into records shaped like our schema.

Salesforce returns CamelCase column names ("AnnualRevenue") and every value as
text; our tables use snake_case and typed columns, plus an organisation_id that
Salesforce does not know about.
"""
import csv
import io
import re
from typing import Dict, List

from app.salesforce.schemas import SALESFORCE_SCHEMAS

_CAMEL = re.compile(r"(?<!^)(?=[A-Z])")

# Salesforce field names that differ from our column names after snake_casing.
SOQL_TO_SCHEMA = {"contract_term": "contract_term_months"}
SCHEMA_TO_SOQL = {v: k for k, v in SOQL_TO_SCHEMA.items()}


def to_snake(name: str) -> str:
    snake = _CAMEL.sub("_", name).lower()
    return SOQL_TO_SCHEMA.get(snake, snake)


def to_camel(name: str) -> str:
    name = SCHEMA_TO_SOQL.get(name, name)
    return "".join(part.capitalize() for part in name.split("_"))


def _coerce(value: str, type_name: str):
    if value is None or value == "":
        return None
    try:
        if type_name == "float":
            return float(value)
        if type_name == "int":
            return int(float(value))
        if type_name == "bool":
            return value.strip().lower() in ("true", "1", "yes")
    except ValueError:
        return None  # a malformed number is stored as NULL, not a crash
    return value


def parse_bulk_csv(csv_text: str, object_name: str, org_id: str) -> List[dict]:
    types: Dict[str, str] = dict(SALESFORCE_SCHEMAS[object_name])
    records = []
    for row in csv.DictReader(io.StringIO(csv_text)):
        record = {to_snake(col): _coerce(val, types.get(to_snake(col), "string")) for col, val in row.items()}
        record["organisation_id"] = org_id
        records.append(record)
    return records
