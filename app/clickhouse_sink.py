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

import io
import json
from typing import Dict, Iterable, List, Optional, Protocol, Tuple

from app.config import settings
from app.errors import InfrastructureUnavailable

# Maps our internal schema type names to ClickHouse column types.
CH_TYPE_MAP = {
    "string": "String",
    "float": "Float64",
    "int": "Int32",
    "bool": "UInt8",
    "datetime": "DateTime64(3)",
    "date": "Date",
}

SchemaField = Tuple[str, str]  # (field_name, type_name)

# Deduplicating engine for every landed table: replaying a page/file replaces rows by key.
DEFAULT_ENGINE = "ReplacingMergeTree"
DEFAULT_ORDER_BY = "(organisation_id, id)"
DEFAULT_PARTITION_BY = "organisation_id"


def _q(value: str) -> str:
    """Escape a value for use inside a single-quoted ClickHouse string literal."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def build_load_sql(
    table: str,
    schema: List[SchemaField],
    url: str,
    access_key: str,
    secret_key: str,
    fmt: str,
    renames: Optional[Dict[str, str]] = None,
    required: Iterable[str] = ("id", "organisation_id"),
) -> str:
    """
    INSERT ... SELECT that makes ClickHouse itself read a landed file from MinIO/S3.

    * Only the table's columns are selected, so extra columns in the file (dlt adds
      _dlt_id / _dlt_load_id) are ignored, and columns missing from a file load as NULL/default.
    * Datetimes are landed as ISO text and parsed here, so offsets like +00:00 work.
    * Rows lacking a required key are skipped (they were dead-lettered upstream anyway).
    """
    renames = renames or {}
    structure, select = [], []
    for name, type_ in schema:
        src = renames.get(name, name)
        if type_ == "datetime":
            structure.append(f"`{src}` Nullable(String)")
            select.append(f"parseDateTime64BestEffortOrNull(`{src}`, 3) AS `{name}`")
        else:
            structure.append(f"`{src}` Nullable({CH_TYPE_MAP[type_]})")
            select.append(f"`{src}` AS `{name}`")
    where = " AND ".join(f"`{renames.get(c, c)}` IS NOT NULL AND `{renames.get(c, c)}` != ''" for c in required)
    columns = ", ".join(f"`{name}`" for name, _ in schema)
    return (
        f"INSERT INTO {table} ({columns}) "
        f"SELECT {', '.join(select)} "
        f"FROM s3('{_q(url)}', '{_q(access_key)}', '{_q(secret_key)}', '{fmt}', '{', '.join(structure)}') "
        + (f"WHERE {where} " if where else "")
        + "SETTINGS insert_null_as_default = 1, input_format_json_read_bools_as_numbers = 1, "
        # ClickHouse retries failed S3 reads 500 times by default; fail fast so a MinIO outage
        # fails the job in seconds instead of hanging it for minutes.
        "s3_retry_attempts = 3"
    )


def _s3_url(path: str) -> str:
    """URL ClickHouse uses to reach an object. It may differ from the API's own MinIO address
    (e.g. inside Docker, `http://minio:9000`), hence a separate setting."""
    endpoint = settings.clickhouse_s3_endpoint or (
        ("https://" if settings.minio_secure else "http://") + settings.minio_endpoint
    )
    return f"{endpoint.rstrip('/')}/{settings.minio_bucket}/{path}"


def _format_for(path: str) -> str:
    return "Parquet" if path.endswith(".parquet") else "JSONEachRow"


class ClickHouseSink(Protocol):
    def ensure_table(self, object_name: str, schema: List[SchemaField], service: str = "salesforce", **ddl) -> None: ...
    def load_from_storage(self, object_name: str, schema: List[SchemaField], paths: List[str], service: str = "salesforce", **opts) -> None: ...
    def ensure_view(self, object_name: str, service: str = "salesforce") -> None: ...
    def ensure_slack_views(self) -> None: ...
    def ensure_analytical_views(self) -> None: ...
    def inspect_table(self, table_or_object_name: str, service: Optional[str] = None) -> dict: ...
    def _table_name(self, object_name: str, service: str = "salesforce") -> str: ...


def _table_name(object_name: str, service: str = "salesforce") -> str:
    clean = object_name.lower().replace("-", "_")
    if clean.startswith("bronze_") or clean.startswith("v_") or clean.startswith("slack_"):
        return clean
    return f"bronze_{service.lower()}_{clean}"


def _infer_type(values: List) -> str:
    for v in values:
        if isinstance(v, bool):
            return "UInt8"
        if isinstance(v, int):
            return "Int64"
        if isinstance(v, float):
            return "Float64"
        if v is not None:
            return "String"
    return "Nullable(String)"


class NullClickHouseSink:
    """
    Used when ClickHouse is disabled (CLICKHOUSE_ENABLED=false). It does the same job as the
    real sink - reading the landed files back out of storage - but keeps the rows in memory.
    Views are computed with the same join keys as the real SQL; a missing match is None,
    never an invented value. Restarting the app loses this data; it is not a database.
    """

    def __init__(self):
        self.tables_created: set[str] = set()
        self.views_created: set[str] = set()
        self.inserted: Dict[str, List[Dict]] = {}
        self.table_schemas: Dict[str, List[SchemaField]] = {}

    _table_name = staticmethod(_table_name)

    def ensure_table(self, object_name: str, schema: List[SchemaField], service: str = "salesforce", **ddl) -> None:
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

    def insert_rows(self, object_name: str, rows: List[Dict], service: str = "salesforce", key_columns=("id",)) -> None:
        table = self._table_name(object_name, service)
        for key in (object_name, table):
            existing = self.inserted.setdefault(key, [])
            by_key = {tuple(r.get(c) for c in key_columns): r for r in existing}
            for row in rows:
                by_key[tuple(row.get(c) for c in key_columns)] = row  # last write wins, like ReplacingMergeTree
            self.inserted[key] = list(by_key.values())

    def load_from_storage(
        self,
        object_name: str,
        schema: List[SchemaField],
        paths: List[str],
        service: str = "salesforce",
        renames: Optional[Dict[str, str]] = None,
        required: Iterable[str] = ("id", "organisation_id"),
        key_columns: Tuple[str, ...] = ("id",),
        **_ignored,
    ) -> None:
        """Read each landed file back from storage (as ClickHouse would via s3()) into memory."""
        from app.storage import get_storage

        renames = renames or {}
        rows: List[Dict] = []
        for path in paths:
            raw = get_storage().get_object(path)
            if path.endswith(".parquet"):
                import pyarrow.parquet as pq

                records = pq.read_table(io.BytesIO(raw)).to_pylist()
            else:
                loaded = json.loads(raw)
                records = loaded if isinstance(loaded, list) else [loaded]
            for record in records:
                row = {name: record.get(renames.get(name, name)) for name, _ in schema}
                if all(row.get(c) not in (None, "") for c in required):
                    rows.append(row)
        self.insert_rows(object_name, rows, service, key_columns=key_columns)

    def ensure_slack_views(self) -> None:
        self.views_created.add("v_slack_compliance_timeline")

    def ensure_analytical_views(self) -> None:
        self.views_created.update({"v_hubspot_deal_pipeline", "v_client_360_deals", "v_slack_compliance_timeline"})

    # ── views, computed from the loaded rows with the same keys as the real SQL ──────
    def _rows(self, table: str) -> List[Dict]:
        return self.inserted.get(table, [])

    def _view_rows(self, view: str) -> Optional[List[Dict]]:
        if view == "v_hubspot_deal_pipeline":
            companies = {(c.get("organisation_id"), c.get("id")): c for c in self._rows("bronze_hubspot_companies")}
            pipelines = {(p.get("organisation_id"), p.get("id")): p for p in self._rows("bronze_hubspot_pipelines")}
            out = []
            for d in self._rows("bronze_hubspot_deals"):
                org = d.get("organisation_id")
                c, p = companies.get((org, d.get("company_id")), {}), pipelines.get((org, d.get("pipeline")), {})
                out.append({
                    "deal_id": d.get("id"), "organisation_id": org, "deal_name": d.get("deal_name"),
                    "deal_stage": d.get("stage"), "deal_amount": d.get("amount"), "pipeline_id": d.get("pipeline"),
                    "pipeline_label": p.get("label"), "pipeline_active": p.get("active"),
                    "company_id": d.get("company_id"), "company_name": c.get("name"),
                    "company_industry": c.get("industry"), "company_domain": c.get("domain"),
                    "deal_updated_at": d.get("updated_at"),
                })
            return out
        if view == "v_client_360_deals":
            accounts = {(a.get("organisation_id"), a.get("id")): a for a in self._rows("bronze_salesforce_accounts")}
            companies_by_name = {}
            for c in self._rows("bronze_hubspot_companies"):
                companies_by_name.setdefault((c.get("organisation_id"), (c.get("name") or "").lower()), []).append(c)
            deals_by_company = {}
            for d in self._rows("bronze_hubspot_deals"):
                deals_by_company.setdefault((d.get("organisation_id"), d.get("company_id")), []).append(d)
            out = []
            for o in self._rows("bronze_salesforce_opportunities"):
                org = o.get("organisation_id")
                a = accounts.get((org, o.get("account_id")), {})
                matches = companies_by_name.get((org, (a.get("name") or "").lower()), [])
                deals = [d for c in matches for d in deals_by_company.get((org, c.get("id")), [])] or [{}]
                for h in deals:
                    out.append({
                        "salesforce_opportunity_id": o.get("id"), "opportunity_name": o.get("name"),
                        "salesforce_stage": o.get("stage_name"), "salesforce_amount": o.get("amount"),
                        "salesforce_probability": o.get("probability"), "salesforce_account_id": o.get("account_id"),
                        "account_name": a.get("name"), "account_industry": a.get("industry"),
                        "hubspot_deal_id": h.get("id"), "hubspot_deal_name": h.get("deal_name"),
                        "hubspot_amount": h.get("amount"), "hubspot_stage": h.get("stage"), "organisation_id": org,
                    })
            return out
        if view == "v_slack_compliance_timeline":
            channels = {c.get("channel_id"): c for c in self._rows("slack_channels")}
            users = {u.get("user_id"): u for u in self._rows("slack_users")}
            files, reactions = {}, {}
            for f in self._rows("slack_files"):
                files.setdefault(f.get("message_id"), []).append(f.get("file_name"))
            for r in self._rows("slack_reactions"):
                reactions.setdefault(r.get("message_id"), []).append(r.get("emoji"))
            out = []
            for m in self._rows("bronze_slack_messages"):
                out.append({
                    "message_id": m.get("message_id"), "channel_id": m.get("channel_id"),
                    "channel_name": channels.get(m.get("channel_id"), {}).get("name"),
                    "user_id": m.get("user_id"), "user_name": users.get(m.get("user_id"), {}).get("real_name"),
                    "text": m.get("text"), "message_ts": m.get("message_ts"), "thread_ts": m.get("thread_ts"),
                    "message_kind": "thread_reply" if m.get("thread_ts") else "message",
                    "attached_files": files.get(m.get("message_id"), []), "reactions": reactions.get(m.get("message_id"), []),
                    "source": m.get("source"),
                })
            return sorted(out, key=lambda r: r["message_ts"] or "", reverse=True)
        return None

    def inspect_table(self, table_or_object_name: str, service: Optional[str] = None) -> dict:
        target = table_or_object_name
        if service and not target.startswith("bronze_") and not target.startswith("v_"):
            target = self._table_name(table_or_object_name, service)

        view_rows = self._view_rows(target) if target.startswith("v_") else None
        if target.startswith("v_"):
            if view_rows is None:
                return {"exists": False, "mode": "in-memory (ClickHouse disabled)", "table_name": target,
                        "row_count": 0, "columns": [], "sample": []}
            cols = list(view_rows[0].keys()) if view_rows else []
            return {
                "exists": True,
                "mode": "in-memory join (ClickHouse disabled)",
                "table_name": target,
                "row_count": len(view_rows),
                "columns": [{"name": c, "type": _infer_type([r.get(c) for r in view_rows])} for c in cols],
                "sample": view_rows[:10],
            }

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
    def __init__(self, client=None):
        if client is None:
            import clickhouse_connect  # imported lazily so it isn't required for local dev

            client = clickhouse_connect.get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
                database=settings.clickhouse_database,
            )
        self.client = client

    _table_name = staticmethod(_table_name)

    def ensure_table(
        self,
        object_name: str,
        schema: List[SchemaField],
        service: str = "salesforce",
        engine: str = DEFAULT_ENGINE,
        order_by: str = DEFAULT_ORDER_BY,
        partition_by: Optional[str] = DEFAULT_PARTITION_BY,
        **_ignored,
    ) -> None:
        table = self._table_name(object_name, service)
        columns_sql = ", ".join(f"{name} {CH_TYPE_MAP[type_]}" for name, type_ in schema)
        partition_sql = f" PARTITION BY {partition_by}" if partition_by else ""
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql}) "
            f"ENGINE = {engine} ORDER BY {order_by}{partition_sql}"
        )

    def load_from_storage(
        self,
        object_name: str,
        schema: List[SchemaField],
        paths: List[str],
        service: str = "salesforce",
        renames: Optional[Dict[str, str]] = None,
        required: Iterable[str] = ("id", "organisation_id"),
        **_ignored,
    ) -> None:
        """ClickHouse reads each landed file straight from MinIO via the s3() table function."""
        table = self._table_name(object_name, service)
        for path in paths:
            self.client.command(
                build_load_sql(
                    table, schema, _s3_url(path), settings.minio_access_key, settings.minio_secret_key,
                    _format_for(path), renames, required,
                )
            )

    def ensure_view(self, object_name: str, service: str = "salesforce") -> None:
        """
        Deduplicated read view. ReplacingMergeTree only merges duplicates in the background,
        so a plain SELECT can briefly see stale rows after a re-run; FINAL dedups at query time.
        """
        table = self._table_name(object_name, service)
        self.client.command(f"CREATE OR REPLACE VIEW v_{table}_latest AS SELECT * FROM {table} FINAL")

    def ensure_analytical_views(self) -> None:
        """
        Curated cross-source views. Their source tables are created first (empty if that
        source has not run yet) so a view can always be created, and any error is raised.

        v_hubspot_deal_pipeline : Deals -> Companies (company_id) and Pipelines (pipeline id)
        v_client_360_deals      : Salesforce Opportunity -> Account (account_id) -> HubSpot Company
                                  (same name) -> HubSpot Deals (company_id)
        """
        from app.hubspot.schemas import HUBSPOT_SCHEMAS
        from app.salesforce.schemas import SALESFORCE_SCHEMAS

        for name in ("Deals", "Companies", "Pipelines"):
            self.ensure_table(name, HUBSPOT_SCHEMAS[name], service="hubspot")
        for name in ("Opportunities", "Accounts"):
            self.ensure_table(name, SALESFORCE_SCHEMAS[name], service="salesforce")

        self.client.command("""
            CREATE OR REPLACE VIEW v_hubspot_deal_pipeline AS
            SELECT
                d.id AS deal_id, d.organisation_id AS organisation_id, d.deal_name AS deal_name,
                d.stage AS deal_stage, d.amount AS deal_amount, d.pipeline AS pipeline_id,
                p.label AS pipeline_label, p.active AS pipeline_active,
                d.company_id AS company_id, c.name AS company_name, c.industry AS company_industry,
                c.domain AS company_domain, d.updated_at AS deal_updated_at
            FROM bronze_hubspot_deals AS d FINAL
            LEFT JOIN bronze_hubspot_companies AS c FINAL
                ON d.company_id = c.id AND d.organisation_id = c.organisation_id
            LEFT JOIN bronze_hubspot_pipelines AS p FINAL
                ON d.pipeline = p.id AND d.organisation_id = p.organisation_id
            SETTINGS join_use_nulls = 1
        """)
        self.client.command("""
            CREATE OR REPLACE VIEW v_client_360_deals AS
            SELECT
                s.id AS salesforce_opportunity_id, s.name AS opportunity_name,
                s.stage_name AS salesforce_stage, s.amount AS salesforce_amount,
                s.probability AS salesforce_probability, s.account_id AS salesforce_account_id,
                a.name AS account_name, a.industry AS account_industry,
                h.id AS hubspot_deal_id, h.deal_name AS hubspot_deal_name,
                h.amount AS hubspot_amount, h.stage AS hubspot_stage, s.organisation_id AS organisation_id
            FROM bronze_salesforce_opportunities AS s FINAL
            LEFT JOIN bronze_salesforce_accounts AS a FINAL
                ON s.account_id = a.id AND s.organisation_id = a.organisation_id
            LEFT JOIN bronze_hubspot_companies AS hc FINAL
                ON lower(hc.name) = lower(a.name) AND hc.organisation_id = a.organisation_id
            LEFT JOIN bronze_hubspot_deals AS h FINAL
                ON h.company_id = hc.id AND h.organisation_id = hc.organisation_id
            SETTINGS join_use_nulls = 1
        """)
        self.ensure_slack_views()

    def ensure_slack_views(self) -> None:
        from app.slack.tables import SLACK_TABLES

        for table, spec in SLACK_TABLES.items():
            self.ensure_table(table, spec["schema"], engine=spec["engine"], order_by=spec["order_by"],
                              partition_by=spec["partition_by"])
        self.client.command("""
            CREATE OR REPLACE VIEW v_slack_compliance_timeline AS
            SELECT
                m.message_id AS message_id, m.channel_id AS channel_id, c.name AS channel_name,
                m.user_id AS user_id, u.real_name AS user_name, m.text AS text,
                m.message_ts AS message_ts, m.thread_ts AS thread_ts,
                if(m.thread_ts IS NULL OR m.thread_ts = '', 'message', 'thread_reply') AS message_kind,
                f.attached_files AS attached_files, r.reactions AS reactions,
                m.source AS source, m.processing_date AS processing_date, m.inserted_at AS inserted_at
            FROM bronze_slack_messages AS m FINAL
            LEFT JOIN slack_channels AS c FINAL ON m.channel_id = c.channel_id
            LEFT JOIN slack_users AS u FINAL ON m.user_id = u.user_id
            LEFT JOIN (
                SELECT message_id, groupArray(file_name) AS attached_files FROM slack_files FINAL GROUP BY message_id
            ) AS f ON m.message_id = f.message_id
            LEFT JOIN (
                SELECT message_id, groupArray(emoji) AS reactions FROM slack_reactions FINAL GROUP BY message_id
            ) AS r ON m.message_id = r.message_id
            ORDER BY m.message_ts DESC
            SETTINGS join_use_nulls = 1
        """)

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
        if not _is_ch_alive(settings.clickhouse_host, settings.clickhouse_port, timeout=1.0):
            raise InfrastructureUnavailable(
                f"ClickHouse at {settings.clickhouse_host}:{settings.clickhouse_port} is unreachable. "
                f"Start it (docker compose up -d) or set CLICKHOUSE_ENABLED=false for development without Docker."
            )
        try:
            _sink_instance = RealClickHouseSink()
        except Exception as exc:
            raise InfrastructureUnavailable(f"ClickHouse rejected the connection: {exc}") from exc
    else:
        _sink_instance = NullClickHouseSink()
    return _sink_instance
