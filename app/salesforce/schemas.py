"""
Distinct per-object schemas for all 10+ required Salesforce objects. Last
time every object got the same flat 6-field schema (id, organisation_id,
name, status, created_date, value) — this is the fix: each object has its
own realistic fields, typed for ClickHouse column creation.
"""
import random
import uuid
from typing import Dict, List, Tuple

from faker import Faker

fake = Faker()

SchemaField = Tuple[str, str]  # (field_name, type_name: string|float|int|bool|datetime)

SALESFORCE_SCHEMAS: Dict[str, List[SchemaField]] = {
    "Accounts": [
        ("id", "string"), ("organisation_id", "string"), ("name", "string"),
        ("industry", "string"), ("annual_revenue", "float"),
        ("billing_city", "string"), ("billing_country", "string"),
        ("created_date", "datetime"),
    ],
    "Contacts": [
        ("id", "string"), ("organisation_id", "string"), ("first_name", "string"),
        ("last_name", "string"), ("email", "string"), ("phone", "string"),
        ("account_id", "string"), ("created_date", "datetime"),
    ],
    "Opportunities": [
        ("id", "string"), ("organisation_id", "string"), ("name", "string"),
        ("stage_name", "string"), ("amount", "float"), ("probability", "float"),
        ("account_id", "string"), ("close_date", "datetime"),
    ],
    "Leads": [
        ("id", "string"), ("organisation_id", "string"), ("first_name", "string"),
        ("last_name", "string"), ("company", "string"), ("status", "string"),
        ("email", "string"), ("lead_source", "string"),
    ],
    "Tasks": [
        ("id", "string"), ("organisation_id", "string"), ("subject", "string"),
        ("status", "string"), ("priority", "string"), ("who_id", "string"),
        ("activity_date", "datetime"),
    ],
    "Cases": [
        ("id", "string"), ("organisation_id", "string"), ("case_number", "string"),
        ("subject", "string"), ("status", "string"), ("priority", "string"),
        ("origin", "string"), ("account_id", "string"),
    ],
    "Products": [
        ("id", "string"), ("organisation_id", "string"), ("name", "string"),
        ("product_code", "string"), ("family", "string"), ("is_active", "bool"),
    ],
    "PricebookEntries": [
        ("id", "string"), ("organisation_id", "string"), ("product_id", "string"),
        ("pricebook_id", "string"), ("unit_price", "float"), ("is_active", "bool"),
    ],
    "Contracts": [
        ("id", "string"), ("organisation_id", "string"), ("account_id", "string"),
        ("status", "string"), ("contract_term_months", "int"), ("start_date", "datetime"),
    ],
    "Assets": [
        ("id", "string"), ("organisation_id", "string"), ("name", "string"),
        ("account_id", "string"), ("serial_number", "string"),
        ("status", "string"), ("install_date", "datetime"),
    ],
}


def _fake_value(field_name: str, type_: str):
    if type_ == "string":
        if "email" in field_name:
            return fake.email()
        if field_name in ("first_name",):
            return fake.first_name()
        if field_name in ("last_name",):
            return fake.last_name()
        if field_name in ("name", "company") or "account" in field_name and field_name.endswith("name"):
            return fake.company()
        if "city" in field_name:
            return fake.city()
        if "country" in field_name:
            return fake.country()
        if "phone" in field_name:
            return fake.phone_number()
        if "serial" in field_name:
            return fake.bothify(text="SN-########")
        return fake.word()
    if type_ == "float":
        if "probability" in field_name:
            return round(random.uniform(0, 100), 2)
        if "price" in field_name:
            return round(random.uniform(5, 5_000), 2)
        return round(random.uniform(100, 250_000), 2)
    if type_ == "int":
        return random.randint(1, 60)
    if type_ == "bool":
        return random.choice([True, False])
    if type_ == "datetime":
        return fake.date_time_this_year().isoformat()
    return None


def generate_fake_records(
    object_name: str,
    count: int,
    org_id: str = "org1",
    corrupt_indices: set | None = None,
) -> List[dict]:
    """
    Stands in for Salesforce Bulk API v2's Retrieve Results step.
    `corrupt_indices` optionally strips the id from specific records, to
    exercise the dead-letter path deterministically in tests/demos.
    """
    schema = SALESFORCE_SCHEMAS[object_name]
    corrupt_indices = corrupt_indices or set()
    records = []
    for i in range(count):
        record = {}
        for field_name, type_ in schema:
            if field_name == "id":
                record[field_name] = str(uuid.uuid4())
            elif field_name == "organisation_id":
                record[field_name] = org_id
            else:
                record[field_name] = _fake_value(field_name, type_)
        if i in corrupt_indices:
            record.pop("id", None)
        records.append(record)
    return records


def validate_records(records: List[dict]) -> Tuple[List[dict], List[Tuple[dict, str]]]:
    """Required-field check. Bad records go to the dead-letter table, not the void."""
    valid, invalid = [], []
    for record in records:
        if not record.get("id") or not record.get("organisation_id"):
            invalid.append((record, "missing required field: id or organisation_id"))
        else:
            valid.append(record)
    return valid, invalid
