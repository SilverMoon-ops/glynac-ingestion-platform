"""
ClickHouse loader, abstracted the same way storage.py is: a no-op sink so the
pipeline is fully runnable/testable without Docker, and a real sink for when
`docker compose up -d` is running and CLICKHOUSE_ENABLED=true.
"""
from typing import Dict, List, Protocol, Tuple

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
    def ensure_table(self, object_name: str, schema: List[SchemaField]) -> None: ...
    def insert_rows(self, object_name: str, rows: List[Dict]) -> None: ...


class NullClickHouseSink:
    """
    Used whenever ClickHouse isn't running (default for local dev). Keeps an
    in-memory record of what *would* have been created/inserted so tests and
    the UI can still report something sensible, without needing a real server.
    """

    def __init__(self):
        self.tables_created: set[str] = set()
        self.inserted: Dict[str, List[Dict]] = {}

    def ensure_table(self, object_name: str, schema: List[SchemaField]) -> None:
        self.tables_created.add(object_name)

    def insert_rows(self, object_name: str, rows: List[Dict]) -> None:
        self.inserted.setdefault(object_name, []).extend(rows)


class RealClickHouseSink:
    def __init__(self):
        import clickhouse_connect  # type: ignore[import-not-found]  # imported lazily so it isn't required for local dev

        self.client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    @staticmethod
    def _table_name(object_name: str) -> str:
        return f"bronze_salesforce_{object_name.lower()}"

    def ensure_table(self, object_name: str, schema: List[SchemaField]) -> None:
        columns_sql = ", ".join(f"{name} {CH_TYPE_MAP[type_]}" for name, type_ in schema)
        self.client.command(
            f"CREATE TABLE IF NOT EXISTS {self._table_name(object_name)} ({columns_sql}) "
            f"ENGINE = MergeTree ORDER BY (organisation_id, id) PARTITION BY organisation_id"
        )

    def insert_rows(self, object_name: str, rows: List[Dict]) -> None:
        if not rows:
            return
        columns = list(rows[0].keys())
        data = [[row.get(col) for col in columns] for row in rows]
        self.client.insert(self._table_name(object_name), data, column_names=columns)


_sink_instance: ClickHouseSink | None = None


def get_clickhouse_sink() -> ClickHouseSink:
    global _sink_instance
    if _sink_instance is not None:
        return _sink_instance

    if settings.clickhouse_enabled:
        _sink_instance = RealClickHouseSink()
    else:
        _sink_instance = NullClickHouseSink()
    return _sink_instance
