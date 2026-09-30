"""
Per-object HubSpot schemas — all 8 resources the task spec requires:
Contacts, Companies, Deals, Tickets, LineItems, Engagements, Pipelines, Owners.

Records are generated deterministically from their offset (not random uuids)
so that pausing mid-pagination and resuming from a saved cursor reproduces the
exact same record ids — which is what lets us prove "resume without duplicates"
instead of just asserting it.
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from faker import Faker

fake = Faker()
Faker.seed(42)  # deterministic field content across runs

SchemaField = Tuple[str, str]

HUBSPOT_SCHEMAS: Dict[str, List[SchemaField]] = {
    "Contacts": [
        ("id", "string"), ("organisation_id", "string"), ("email", "string"),
        ("first_name", "string"), ("last_name", "string"),
        ("lifecycle_stage", "string"), ("updated_at", "datetime"),
    ],
    "Deals": [
        ("id", "string"), ("organisation_id", "string"), ("deal_name", "string"),
        ("stage", "string"), ("amount", "float"), ("pipeline", "string"),
        ("updated_at", "datetime"),
    ],
    "Companies": [
        ("id", "string"), ("organisation_id", "string"), ("name", "string"),
        ("domain", "string"), ("industry", "string"), ("employee_count", "int"),
        ("updated_at", "datetime"),
    ],
    "Tickets": [
        ("id", "string"), ("organisation_id", "string"), ("subject", "string"),
        ("status", "string"), ("priority", "string"), ("pipeline", "string"),
        ("updated_at", "datetime"),
    ],
    # ── NEW: 4 missing resources required by BE-2 ──────────────────────────
    "LineItems": [
        ("id", "string"), ("organisation_id", "string"), ("deal_id", "string"),
        ("product_id", "string"), ("name", "string"), ("quantity", "int"),
        ("unit_price", "float"), ("amount", "float"), ("updated_at", "datetime"),
    ],
    "Engagements": [
        ("id", "string"), ("organisation_id", "string"), ("type", "string"),
        ("contact_id", "string"), ("deal_id", "string"),
        ("body", "string"), ("updated_at", "datetime"),
    ],
    "Pipelines": [
        ("id", "string"), ("organisation_id", "string"), ("label", "string"),
        ("display_order", "int"), ("active", "bool"), ("updated_at", "datetime"),
    ],
    "Owners": [
        ("id", "string"), ("organisation_id", "string"), ("email", "string"),
        ("first_name", "string"), ("last_name", "string"),
        ("team_id", "string"), ("updated_at", "datetime"),
    ],
}

_STAGES = ["appointmentscheduled", "qualifiedtobuy", "presentationscheduled", "closedwon", "closedlost"]
_LIFECYCLE = ["subscriber", "lead", "marketingqualifiedlead", "opportunity", "customer"]
_TICKET_STATUS = ["new", "open", "waiting", "closed"]
_PRIORITY = ["low", "medium", "high"]
_ENGAGEMENT_TYPES = ["NOTE", "EMAIL", "CALL", "MEETING", "TASK"]
_BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _fake_field(object_name: str, field_name: str, type_: str, offset: int):
    if field_name == "updated_at":
        return (_BASE_TIME + timedelta(minutes=offset)).isoformat()
    if type_ == "string":
        if field_name == "email":
            return f"contact{offset}@example.com"
        if field_name == "first_name":
            return fake.first_name()
        if field_name == "last_name":
            return fake.last_name()
        if field_name in ("name", "deal_name"):
            return fake.company()
        if field_name == "domain":
            return fake.domain_name()
        if field_name == "stage":
            return _STAGES[offset % len(_STAGES)]
        if field_name == "lifecycle_stage":
            return _LIFECYCLE[offset % len(_LIFECYCLE)]
        if field_name == "status":
            return _TICKET_STATUS[offset % len(_TICKET_STATUS)]
        if field_name == "priority":
            return _PRIORITY[offset % len(_PRIORITY)]
        if field_name == "pipeline":
            return "default"
        if field_name == "subject":
            return fake.sentence(nb_words=6)
        if field_name == "industry":
            return fake.bs()
        if field_name == "label":
            return f"Pipeline {offset % 3 + 1}"
        if field_name == "type":
            return _ENGAGEMENT_TYPES[offset % len(_ENGAGEMENT_TYPES)]
        if field_name == "body":
            return fake.paragraph(nb_sentences=2)
        if field_name in ("deal_id", "contact_id", "product_id", "team_id"):
            return f"{field_name.replace('_id','')}-{offset % 20}"
        return fake.word()
    if type_ == "float":
        return round(1000 + (offset * 137.5) % 50_000, 2)
    if type_ == "int":
        if field_name == "display_order":
            return offset % 10
        if field_name == "quantity":
            return 1 + offset % 10
        return 5 + (offset * 7) % 500
    if type_ == "bool":
        return offset % 2 == 0
    return None


def generate_fake_page(
    object_name: str, offset: int, count: int, org_id: str = "org1"
) -> List[dict]:
    """Deterministic: record at a given (object_name, offset, org_id) is always identical."""
    schema = HUBSPOT_SCHEMAS[object_name]
    records = []
    for i in range(count):
        idx = offset + i
        record = {}
        for field_name, type_ in schema:
            if field_name == "id":
                record[field_name] = f"{object_name.lower()}-{org_id}-{idx}"
            elif field_name == "organisation_id":
                record[field_name] = org_id
            else:
                record[field_name] = _fake_field(object_name, field_name, type_, idx)
        records.append(record)
    return records


def validate_records(records: List[dict]) -> Tuple[List[dict], List[Tuple[dict, str]]]:
    valid, invalid = [], []
    for record in records:
        if not record.get("id") or not record.get("organisation_id"):
            invalid.append((record, "missing required field: id or organisation_id"))
        else:
            valid.append(record)
    return valid, invalid
