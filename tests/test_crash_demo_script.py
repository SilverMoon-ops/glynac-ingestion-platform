"""The real thing: boots the API as a separate process, SIGKILLs it mid-ingestion, restarts it,
and checks row counts and file counts. Takes ~30s because it is a genuine process kill."""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_kill_9_mid_ingestion_recovers_with_no_loss_and_no_duplicates():
    result = subprocess.run(
        [sys.executable, "scripts/crash_recovery_demo.py"], cwd=ROOT, capture_output=True, text=True, timeout=240
    )
    assert result.returncode == 0, result.stdout[-2500:]
    assert "ALL CHECKS PASSED" in result.stdout
