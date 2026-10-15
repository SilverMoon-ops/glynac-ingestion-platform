#!/usr/bin/env python
"""
Pin the `latest` image tags in docker-compose.yml to the versions that are actually running.

Run it AFTER `docker compose up -d --build` has worked for you:

    python scripts/pin_images.py --dry-run     # show what would change
    python scripts/pin_images.py               # rewrite docker-compose.yml (keeps a .bak copy)

Why: `latest` can silently change the next time someone runs `docker compose pull`. Pinning records
the exact MinIO release and ClickHouse version you verified against.
"""
import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

COMPOSE = Path(__file__).resolve().parent.parent / "docker-compose.yml"
MINIO_IMAGE = "quay.io/minio/minio"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server"


def parse_clickhouse_version(output: str) -> str:
    """'25.8.3.66' from the output of `SELECT version()`."""
    m = re.search(r"\b(\d{2}\.\d{1,2}\.\d+\.\d+)\b", output)
    if not m:
        raise ValueError(f"could not find a ClickHouse version in: {output!r}")
    return m.group(1)


def parse_minio_release(output: str) -> str:
    """'RELEASE.2025-09-07T16-13-09Z' from the output of `minio --version`."""
    m = re.search(r"(RELEASE\.\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)", output)
    if not m:
        raise ValueError(f"could not find a MinIO release in: {output!r}")
    return m.group(1)


def pin(compose_text: str, clickhouse_version: str, minio_release: str) -> str:
    """Replace the image tags (whatever they currently are) with the given versions."""
    text, n1 = re.subn(rf"(image:\s*{re.escape(MINIO_IMAGE)}):\S+", rf"\1:{minio_release}", compose_text)
    text, n2 = re.subn(rf"(image:\s*{re.escape(CLICKHOUSE_IMAGE)}):\S+", rf"\1:{clickhouse_version}", text)
    if n1 != 1 or n2 != 1:
        raise ValueError(f"expected one MinIO and one ClickHouse image line, found {n1} and {n2}")
    return text


def _docker_exec(container: str, command: str) -> str:
    result = subprocess.run(["docker", "exec", container, "sh", "-c", command], capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"`docker exec {container}` failed: {result.stderr.strip() or result.stdout.strip()}\n"
                 f"Is the stack running? Check: docker compose ps")
    return result.stdout + result.stderr


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="print the new image lines without writing")
    ap.add_argument("--clickhouse-container", default="glynac-clickhouse")
    ap.add_argument("--minio-container", default="glynac-minio")
    args = ap.parse_args()

    ch = parse_clickhouse_version(_docker_exec(
        args.clickhouse_container,
        'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SELECT version()"'))
    minio = parse_minio_release(_docker_exec(args.minio_container, "minio --version"))

    old = COMPOSE.read_text()
    new = pin(old, ch, minio)
    print(f"  {MINIO_IMAGE}:{minio}")
    print(f"  {CLICKHOUSE_IMAGE}:{ch}")
    if new == old:
        print("docker-compose.yml already pins these versions; nothing to change.")
        return
    if args.dry_run:
        print("(dry run: docker-compose.yml not changed)")
        return
    shutil.copy(COMPOSE, COMPOSE.with_suffix(".yml.bak"))
    COMPOSE.write_text(new)
    print(f"Updated {COMPOSE.name} (previous version saved as {COMPOSE.name}.bak).")
    print("Next: docker compose up -d   (re-run scripts/smoke_test.py to confirm nothing changed)")


if __name__ == "__main__":
    main()
