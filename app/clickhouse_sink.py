"""
ClickHouse loader, abstracted the same way storage.py is: a no-op sink so the
pipeline is fully runnable/testable without Docker, and a real sink for when
`docker compose up -d` is running and CLICKHOUSE_ENABLED=true.

Supports:
- Multi-service bronze table naming:
    bronze_salesforce_{object_name}
    bronze_hubspot_{object_name}
    bronze_slack_{object_name}
- Deduplication read views: v_{table}_latest FINAL
- Curated Analytical Views:
    1. v_hubspot_deal_pipeline (Deals + Companies + Pipelines)
    2. v_client_360_deals (Salesforce Opportunities + Accounts + HubSpot Deals)
- Live and in-memory schema & row inspection
"""
from __future__ import annotations

from typing import Dict, List, Optional, Protocol, Tuple
from app.config import settings

# Maps our internal schema type names to ClickHouse column types.
CH_TYPE_MAP = {
    "string": "String",
    "float": "Float64",
    "int": "Int32",
    "bool": "UInt8",
    "datetime": "DateTime64(3)",
}

SchemaField = Tuple[str, str]  # (field_name, type_name)


class ClickHouseSink(Protocol):
    def ensure_table(self, object_name: str, schema: List[SchemaField], service: str = "salesforce") -> None: ...
    def insert_rows(self, object_name: str, rows: List[Dict], service: str = "salesforce") -> None: ...
    def ensure_view(self, object_name: str, service: str = "salesforce") -> None: ...
    def ensure_analytical_views(self) -> None: ...
    def inspect_table(self, table_or_object_name: str, service: Optional[str] = None) -> dict: ...
    def _table_name(self, object_name: str, service: str = "salesforce") -> str: ...


class NullClickHouseSink:
    """
    Used whenever ClickHouse isn't running (default for local dev). Keeps an
    in-memory record of what *would* have been created/inserted so tests and
    the UI can still report something sensible, without needing a real server.
    """

    def __init__(self):
        self.tables_created: set[str] = set()
        self.views_created: set[str] = set()
        self.inserted: Dict[str, List[Dict]] = {}
        self.table_schemas: Dict[str, List[SchemaField]] = {}

    @staticmethod
    def _table_name(object_name: str, service: str = "salesforce") -> str:
        clean = object_name.lower().replace("-", "_")
        if clean.startswith("bronze_") or clean.startswith("v_") or clean.startswith("slack_"):
            return clean
        return f"bronze_{service.lower()}_{clean}"

    def ensure_table(self, object_name: str, schema: List[SchemaField], service: str = "salesforce") -> None:
        table = self._table_name(object_name, service)
        # Record both raw name and full table name for backward-compatibility with tests
        self.tables_created.add(object_name)
        self.tables_created.add(table)
        self.table_schemas[object_name] = schema
        self.table_schemas[table] = schema

    def ensure_view(self, object_name: str, service: str = "salesforce") -> None:
        table = self._table_name(object_name, service)
        self.views_created.add(object_name)
        self.views_created.add(f"v_{table}_latest")

    def insert_rows(self, object_name: str, rows: List[Dict], service: str = "salesforce") -> None:
        table = self._table_name(object_name, service)
        for key in (object_name, table):
            existing = self.inserted.setdefault(key, [])
            by_id = {row["id"]: row for row in existing if "id" in row}
            for row in rows:
                if "id" in row:
                    by_id[row["id"]] = row  # last write wins, same as ReplacingMergeTree on merge
            self.inserted[key] = list(by_id.values())

    def ensure_analytical_views(self) -> None:
        self.views_created.add("v_hubspot_deal_pipeline")
        self.views_created.add("v_client_360_deals")
        self.views_created.add("v_slack_compliance_timeline")

    def inspect_table(self, table_or_object_name: str, service: Optional[str] = None) -> dict:
        target = table_or_object_name
        if service and not target.startswith("bronze_") and not target.startswith("v_"):
            target = self._table_name(table_or_object_name, service)

        # Check for curated analytical views
        if "deal_pipeline" in target:
            deals = self.inserted.get("Deals", self.inserted.get("bronze_hubspot_deals", []))
            companies = self.inserted.get("Companies", self.inserted.get("bronze_hubspot_companies", []))
            comp_map = {c.get("id"): c for c in companies}
            joined_rows = []
            for d in deals[:10]:
                comp = comp_map.get(d.get("organisation_id"), companies[0] if companies else {})
                joined_rows.append({
                    "deal_id": d.get("id"),
                    "deal_name": d.get("deal_name"),
                    "deal_stage": d.get("stage"),
                    "deal_amount": d.get("amount"),
                    "company_name": comp.get("name", "Acme Corporation"),
                    "pipeline_label": "Default Pipeline",
                    "updated_at": d.get("updated_at"),
                })
            return {
                "exists": True,
                "mode": "in-memory view (ClickHouse disabled)",
                "table_name": "v_hubspot_deal_pipeline",
                "row_count": len(deals),
                "columns": [
                    {"name": "deal_id", "type": "String"},
                    {"name": "deal_name", "type": "String"},
                    {"name": "deal_stage", "type": "String"},
                    {"name": "deal_amount", "type": "Float64"},
                    {"name": "company_name", "type": "String"},
                    {"name": "pipeline_label", "type": "String"},
                    {"name": "updated_at", "type": "DateTime64(3)"},
                ],
                "sample": joined_rows,
            }

        if "client_360" in target:
            opps = self.inserted.get("Opportunities", self.inserted.get("bronze_salesforce_opportunities", []))
            deals = self.inserted.get("Deals", self.inserted.get("bronze_hubspot_deals", []))
            accounts = self.inserted.get("Accounts", self.inserted.get("bronze_salesforce_accounts", []))
            acc_map = {a.get("id"): a for a in accounts}
            joined_rows = []
            for i, opp in enumerate(opps[:10]):
                deal = deals[i % len(deals)] if deals else {}
                acc = acc_map.get(opp.get("account_id"), accounts[0] if accounts else {})
                joined_rows.append({
                    "salesforce_opportunity_id": opp.get("id"),
                    "opportunity_name": opp.get("name"),
                    "salesforce_amount": opp.get("amount"),
                    "salesforce_stage": opp.get("stage_name"),
                    "account_name": acc.get("name", "Apex Financial"),
                    "hubspot_deal_id": deal.get("id", "hs-deal-001"),
                    "hubspot_deal_name": deal.get("deal_name", opp.get("name")),
                    "hubspot_amount": deal.get("amount", opp.get("amount")),
                    "organisation_id": opp.get("organisation_id", "org1"),
                })
            return {
                "exists": True,
                "mode": "in-memory view (ClickHouse disabled)",
                "table_name": "v_client_360_deals",
                "row_count": max(len(opps), len(deals)),
                "columns": [
                    {"name": "salesforce_opportunity_id", "type": "String"},
                    {"name": "opportunity_name", "type": "String"},
                    {"name": "salesforce_amount", "type": "Float64"},
                    {"name": "salesforce_stage", "type": "String"},
                    {"name": "account_name", "type": "String"},
                    {"name": "hubspot_deal_id", "type": "String"},
                    {"name": "hubspot_deal_name", "type": "String"},
                    {"name": "hubspot_amount", "type": "Float64"},
                    {"name": "organisation_id", "type": "String"},
                ],
                "sample": joined_rows,
            }

        # Check standard tables
        schema = self.table_schemas.get(target)
        rows = self.inserted.get(target, [])
        if not schema and not rows:
            for k in self.table_schemas:
                if target in k or k in target:
                    schema = self.table_schemas[k]
                    rows = self.inserted.get(k, [])
                    target = k
                    break

        columns = [{"name": s[0], "type": s[1]} for s in schema] if schema else []
        return {
            "exists": bool(schema or rows),
            "mode": "in-memory (ClickHouse disabled)",
            "table_name": target,
            "row_count": len(rows),
            "columns": columns,
            "sample": rows[:10],
        }


class RealClickHouseSink:
    def __init__(self):
        import clickhouse_connect  # imported lazily so it isn't required for local dev

        self.client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    @staticmethod
    def _table_name(object_name: str, service: str = "salesforce") -> str:
        clean = object_name.lower().replace("-", "_")
        if clean.startswith("bronze_") or clean.startswith("v_") or clean.startswith("slack_"):
            return clean
        return f"bronze_{service.lower()}_{clean}"

    def ensure_table(self, object_name: str, schema: List[SchemaField], service: str = "salesforce") -> None:
        table = self._table_name(object_name, service)
        columns_sql = ", ".join(f"{name} {CH_TYPE_MAP[type_]}" for name, type_ in schema)
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql}) "
            f"ENGINE = ReplacingMergeTree ORDER BY (organisation_id, id) PARTITION BY organisation_id"
        )

    def insert_rows(self, object_name: str, rows: List[Dict], service: str = "salesforce") -> None:
        if not rows:
            return
        from datetime import datetime, timezone

        def _coerce(val):
            if isinstance(val, bool):
                return int(val)
            if isinstance(val, str):
                for fmt in (
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S.%f%z",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f",
                ):
                    try:
                        dt = datetime.strptime(val, fmt)
                        if dt.tzinfo is not None:
                            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                        return dt
                    except ValueError:
                        continue
            if isinstance(val, datetime):
                if val.tzinfo is not None:
                    val = val.astimezone(timezone.utc).replace(tzinfo=None)
                return val
            return val

        table = self._table_name(object_name, service)
        columns = list(rows[0].keys())
        data = [[_coerce(row.get(col)) for col in columns] for row in rows]
        self.client.insert(table, data, column_names=columns)

    def ensure_view(self, object_name: str, service: str = "salesforce") -> None:
        """
        Deduplicated read view. ReplacingMergeTree only merges duplicates in
        the background, so ad-hoc SELECTs on the raw table can still see
        stale rows briefly after a resumed/re-run load — FINAL forces the
        dedup at query time for anyone querying through this view.
        """
        table = self._table_name(object_name, service)
        self.client.command(
            f"CREATE VIEW IF NOT EXISTS v_{table}_latest AS SELECT * FROM {table} FINAL"
        )

    def ensure_analytical_views(self) -> None:
        """
        Builds curated analytical views combining resources across platforms:
        1. v_hubspot_deal_pipeline: Deals + Companies + Pipelines (Task 2 spec)
        2. v_client_360_deals: Salesforce Opportunities/Accounts + HubSpot Deals (Roadmap spec)
        """
        # 1. HubSpot Deal Pipeline Analytical View
        try:
            self.client.command("""
                CREATE VIEW IF NOT EXISTS v_hubspot_deal_pipeline AS
                SELECT
                    d.id AS deal_id,
                    d.organisation_id,
                    d.deal_name,
                    d.stage AS deal_stage,
                    d.amount AS deal_amount,
                    d.pipeline AS pipeline_id,
                    c.id AS company_id,
                    c.name AS company_name,
                    c.industry AS company_industry,
                    c.domain AS company_domain,
                    p.label AS pipeline_label,
                    p.active AS pipeline_active,
                    d.updated_at AS deal_updated_at
                FROM bronze_hubspot_deals d
                LEFT JOIN bronze_hubspot_companies c ON (d.organisation_id = c.organisation_id)
                LEFT JOIN bronze_hubspot_pipelines p ON (d.pipeline = p.id OR d.organisation_id = p.organisation_id)
            """)
        except Exception as exc:
            print(f"[CLICKHOUSE] v_hubspot_deal_pipeline view skipped/pending: {exc}")

        # 2. Client 360 Deals Cross-Platform Analytical View
        try:
            self.client.command("""
                CREATE VIEW IF NOT EXISTS v_client_360_deals AS
                SELECT
                    s.id AS salesforce_opportunity_id,
                    s.name AS opportunity_name,
                    s.stage_name AS salesforce_stage,
                    s.amount AS salesforce_amount,
                    s.probability AS salesforce_probability,
                    s.account_id AS salesforce_account_id,
                    a.name AS account_name,
                    a.industry AS account_industry,
                    h.id AS hubspot_deal_id,
                    h.deal_name AS hubspot_deal_name,
                    h.amount AS hubspot_amount,
                    h.stage AS hubspot_stage,
                    s.organisation_id
                FROM bronze_salesforce_opportunities s
                LEFT JOIN bronze_salesforce_accounts a ON (s.account_id = a.id AND s.organisation_id = a.organisation_id)
                LEFT JOIN bronze_hubspot_deals h ON (s.organisation_id = h.organisation_id)
            """)
        except Exception as exc:
            print(f"[CLICKHOUSE] v_client_360_deals view skipped/pending: {exc}")

    def inspect_table(self, table_or_object_name: str, service: Optional[str] = None) -> dict:
        table = table_or_object_name
        if service and not table.startswith("bronze_") and not table.startswith("v_"):
            table = self._table_name(table_or_object_name, service)

        try:
            exists = bool(self.client.command(f"EXISTS TABLE {table}"))
            if not exists:
                return {
                    "exists": False,
                    "table_name": table,
                    "mode": "live (ClickHouse)",
                    "row_count": 0,
                    "columns": [],
                    "sample": [],
                }

            row_count_res = self.client.command(f"SELECT count() FROM {table}")
            row_count = int(row_count_res) if row_count_res is not None else 0

            desc = self.client.query(f"DESCRIBE TABLE {table}").result_rows
            columns = [{"name": r[0], "type": r[1]} for r in desc]

            sample_query = self.client.query(f"SELECT * FROM {table} LIMIT 10")
            col_names = sample_query.column_names
            sample = [dict(zip(col_names, r)) for r in sample_query.result_rows]

            return {
                "exists": True,
                "mode": "live (ClickHouse)",
                "table_name": table,
                "row_count": row_count,
                "columns": columns,
                "sample": sample,
            }
        except Exception as exc:
            return {
                "exists": False,
                "table_name": table,
                "mode": "live (ClickHouse error)",
                "error": str(exc),
                "row_count": 0,
                "columns": [],
                "sample": [],
            }


def _is_ch_alive(host: str, port: int, timeout: float = 0.5) -> bool:
    import socket
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


_sink_instance: ClickHouseSink | None = None


def get_clickhouse_sink() -> ClickHouseSink:
    global _sink_instance
    if _sink_instance is not None:
        return _sink_instance

    if settings.clickhouse_enabled:
        if _is_ch_alive(settings.clickhouse_host, settings.clickhouse_port, timeout=0.5):
            try:
                _sink_instance = RealClickHouseSink()
            except Exception as exc:
                print(f"[CLICKHOUSE] Connection to real ClickHouse failed ({exc}), falling back to in-memory NullSink")
                _sink_instance = NullClickHouseSink()
        else:
            print(f"[CLICKHOUSE] ClickHouse at {settings.clickhouse_host}:{settings.clickhouse_port} is unreachable. Falling back to NullClickHouseSink")
            _sink_instance = NullClickHouseSink()
    else:
        _sink_instance = NullClickHouseSink()
    return _sink_instance
