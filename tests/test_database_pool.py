import threading

from sqlalchemy import text

from app.database import make_engine


def test_sqlite_pool_holds_far_more_than_the_default_fifteen_connections(tmp_path):
    """
    One click on "Sync all" runs 10 jobs that each hold a connection while waiting on the network, plus
    HubSpot and Slack workers and web requests. With SQLAlchemy's default pool (5 + 10) that exhausts
    the pool and every request then fails with a 500. A short pool_timeout makes the old behaviour
    fail fast here instead of after 30 seconds.
    """
    engine = make_engine(f"sqlite:///{tmp_path}/pool.db", pool_timeout=3)
    held, errors, ready, release = [], [], threading.Barrier(41), threading.Event()

    def hold_a_connection():
        try:
            with engine.connect() as conn:
                conn.execute(text("select 1"))
                ready.wait()
                release.wait(10)
        except Exception as exc:  # noqa: BLE001
            errors.append(repr(exc)[:120])

    threads = [threading.Thread(target=hold_a_connection) for _ in range(40)]
    [t.start() for t in threads]
    ready.wait(15)  # all 40 connections are open at the same moment
    release.set()
    [t.join() for t in threads]
    assert errors == []


def test_sqlite_waits_for_competing_writers_instead_of_failing(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path}/busy.db")
    with engine.connect() as conn:
        assert conn.execute(text("PRAGMA busy_timeout")).scalar() == 30000
