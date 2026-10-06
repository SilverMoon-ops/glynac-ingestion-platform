from __future__ import annotations

from typing import Dict, List, Optional
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.security import require_signed_request
from app.clickhouse_sink import get_clickhouse_sink
from app.config import settings
from app.storage import get_storage

router = APIRouter(
    prefix="/api/analytics",
    tags=["analytics"],
    dependencies=[Depends(require_signed_request)],
)


@router.get("/views")
def list_analytical_views():
    """
    Returns the catalog of curated ClickHouse analytical views created across
    all three ingestion tasks (Salesforce, HubSpot, Slack).
    """
    sink = get_clickhouse_sink()
    sink.ensure_analytical_views()
    return {
        "views": [
            {
                "name": "v_hubspot_deal_pipeline",
                "task": "Task 2 (HubSpot)",
                "description": "Combines Deals, Companies, and Pipelines for instant deal revenue & pipeline stage reporting.",
                "sources": ["bronze_hubspot_deals", "bronze_hubspot_companies", "bronze_hubspot_pipelines"],
            },
            {
                "name": "v_client_360_deals",
                "task": "Roadmap / Cross-Platform (Task 1 + Task 2)",
                "description": "Cross-platform Client 360 view joining Salesforce Accounts/Opportunities with HubSpot Deals.",
                "sources": ["bronze_salesforce_opportunities", "bronze_salesforce_accounts", "bronze_hubspot_deals"],
            },
            {
                "name": "v_slack_compliance_timeline",
                "task": "Task 3 (Slack)",
                "description": "Full compliance audit timeline joining message text with channel info, user profiles, files, and reactions.",
                "sources": ["bronze_slack_messages", "slack_channels", "slack_users", "slack_files", "slack_reactions"],
            },
        ]
    }


@router.get("/inspect/{target_name}")
def inspect_table_or_view(target_name: str, service: Optional[str] = None):
    """
    Inspect any table or curated analytical view. Returns column definitions,
    row counts, and a sample of live records.
    """
    sink = get_clickhouse_sink()
    return sink.inspect_table(target_name, service=service)


@router.get("/status")
def system_components_status():
    """Returns infrastructure runtime modes for the monitoring UI."""
    storage = get_storage()
    sink = get_clickhouse_sink()
    is_ch_real = hasattr(sink, "client")

    return {
        "storage_backend": settings.storage_backend,
        "storage_type": type(storage).__name__,
        "clickhouse_enabled": settings.clickhouse_enabled,
        "clickhouse_mode": "Live ClickHouse Server" if is_ch_real else "In-Memory NullSink",
        "dlt_pipelines_dir": settings.dlt_pipelines_dir,
    }
