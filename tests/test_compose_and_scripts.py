"""Static checks on docker-compose.yml, and the image-pinning helper (no Docker needed)."""
import importlib.util
import os
import re

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMPOSE = os.path.join(ROOT, "docker-compose.yml")


def _load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def test_compose_has_no_duplicate_host_ports_and_all_volumes_are_declared():
    d = _load(COMPOSE)
    host_ports = [str(p).split(":")[0] for s in d["services"].values() for p in s.get("ports", [])]
    assert len(host_ports) == len(set(host_ports)), host_ports
    used = {v.split(":")[0] for s in d["services"].values() for v in s.get("volumes", []) if not v.startswith(".")}
    assert used <= set(d["volumes"]), used - set(d["volumes"])


def test_compose_api_restarts_and_keeps_its_database_off_host_bind_mounts():
    api = _load(COMPOSE)["services"]["api"]
    assert api["restart"] == "unless-stopped"  # crash recovery then resumes the jobs by itself
    assert not any(v.startswith(".") for v in api["volumes"])  # SQLite WAL breaks on Windows/macOS bind mounts
    assert "--reload" not in api["command"]


def test_compose_service_dependencies_exist_and_mock_services_reachable():
    d = _load(COMPOSE)
    names = set(d["services"])
    for s in d["services"].values():
        assert set(s.get("depends_on", {})) <= names
    assert d["services"]["api"]["environment"]["MOCK_SERVICES_URL"] == "http://mock-services:9000"


def _pin_module():
    spec = importlib.util.spec_from_file_location("pin_images", os.path.join(ROOT, "scripts", "pin_images.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_pin_images_parses_version_output_and_rewrites_only_the_two_image_lines():
    pin = _pin_module()
    assert pin.parse_clickhouse_version("25.8.3.66\n") == "25.8.3.66"
    assert pin.parse_minio_release("minio version RELEASE.2025-09-07T16-13-09Z (commit-id=abc)") == \
        "RELEASE.2025-09-07T16-13-09Z"
    original = open(COMPOSE).read()
    new = pin.pin(original, "25.8.3.66", "RELEASE.2025-09-07T16-13-09Z")
    assert "image: quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z" in new
    assert "image: clickhouse/clickhouse-server:25.8.3.66" in new
    assert ":latest" not in new
    assert re.sub(r"image:.*", "", original) == re.sub(r"image:.*", "", new)  # nothing else touched
    assert pin.pin(new, "25.8.3.66", "RELEASE.2025-09-07T16-13-09Z") == new  # idempotent


def test_pin_images_rejects_unparseable_output():
    pin = _pin_module()
    with pytest.raises(ValueError):
        pin.parse_clickhouse_version("error: connection refused")
    with pytest.raises(ValueError):
        pin.parse_minio_release("unknown command")


def test_mock_services_receive_the_demo_knobs_from_dot_env():
    env = _load(COMPOSE)["services"]["mock-services"]["environment"]
    assert {"HUBSPOT_MOCK_LATENCY_SECONDS", "HUBSPOT_MOCK_TOTAL_RECORDS", "SLACK_MOCK_LATENCY_SECONDS",
            "SLACK_MOCK_MESSAGES_PER_CHANNEL"} <= set(env)


def test_dockerignore_keeps_secrets_and_local_state_out_of_the_image():
    lines = {l.strip() for l in open(os.path.join(ROOT, ".dockerignore")) if l.strip() and not l.startswith("#")}
    assert {".env", ".git", "data", "*.db"} <= lines
    assert ".env.example" not in lines or "!.env.example" in lines
