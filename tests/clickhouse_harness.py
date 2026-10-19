"""
Real-ClickHouse test harness.

* chdb is the actual ClickHouse engine, embedded in-process.
* moto's S3 server stands in for MinIO (same S3 HTTP API); ClickHouse reads files from it
  with the s3() table function over HTTP, exactly as it does against MinIO.
* ChdbClient exposes the small slice of the clickhouse_connect API that RealClickHouseSink
  uses, so the production sink code runs unchanged.
"""
import json
import threading


class _Result:
    def __init__(self, columns, rows):
        self.column_names = columns
        self.result_rows = rows


class ChdbClient:
    def __init__(self, session, lock=None):
        self._session = session
        self._lock = lock or threading.Lock()  # the embedded engine is process-wide: serialise access

    def _run(self, sql):
        with self._lock:
            return str(self._session.query(sql, "JSONCompact"))

    def command(self, sql):
        out = self._run(sql).strip()
        if not out:
            return None
        data = json.loads(out)["data"]
        return data[0][0] if len(data) == 1 and len(data[0]) == 1 else data

    def query(self, sql):
        data = json.loads(self._run(sql))
        return _Result([m["name"] for m in data["meta"]], data["data"])

    def rows(self, sql):
        """Test helper: list of dicts."""
        q = self.query(sql)
        return [dict(zip(q.column_names, r)) for r in q.result_rows]

    def reset(self):
        """The embedded engine is process-wide; give every test an empty database."""
        for _ in range(2):  # views first, then the tables they referenced
            for r in self.rows("SELECT name, engine FROM system.tables WHERE database = currentDatabase()"):
                kind = "VIEW" if r["engine"] == "View" else "TABLE"
                self._run(f"DROP {kind} IF EXISTS `{r['name']}`")


class StrictChdbClient(ChdbClient):
    """
    Behaves like a real clickhouse_connect client: ONE running query per client (= per ClickHouse
    session). A second thread using the same client while it is busy fails with the same error the
    real server gives. The engine lock only serialises the embedded engine itself; it must not
    hide the conflict, which is why `busy` is set BEFORE waiting for that lock.
    """

    def __init__(self, session, lock=None):
        super().__init__(session, lock)
        self._busy = False
        self._guard = threading.Lock()

    def _run(self, sql):
        with self._guard:
            if self._busy:
                raise RuntimeError("Attempt to execute concurrent queries within the same session. "
                                   "Please use a separate client instance per thread/process.")
            self._busy = True
        try:
            return super()._run(sql)
        finally:
            with self._guard:
                self._busy = False
