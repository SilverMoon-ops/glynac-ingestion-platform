import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.database import init_db
from app.errors import InfrastructureUnavailable
from app.recovery import recover_interrupted_jobs, start_background_loop, validate_heartbeat_settings
from app.security import validate_security_settings
from app.routers import auth, system, jobs, salesforce, hubspot, slack, slack_events, analytics, storage_browser


def _maybe_start_embedded_mocks() -> None:
    """One-command dev: run the mock HTTP services in-process unless an external URL is configured."""
    from app.config import settings

    if settings.mock_services_url or os.getenv("DISABLE_EMBEDDED_MOCKS"):
        return
    if not (settings.salesforce_mock_enabled or settings.hubspot_mock_enabled or settings.slack_mock_enabled):
        return
    from mock_services.embedded import port_in_use, start_in_thread

    if port_in_use("127.0.0.1", settings.mock_services_port):
        print(f"[MOCKS] something is already listening on :{settings.mock_services_port}; using it")
        return
    start_in_thread(settings.mock_services_port)
    print(f"[MOCKS] mock services listening on http://127.0.0.1:{settings.mock_services_port}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    validate_security_settings()  # refuse to start with an empty / default HMAC_SECRET
    init_db()
    _maybe_start_embedded_mocks()
    stop = threading.Event()
    if not os.getenv("DISABLE_RECOVERY"):
        validate_heartbeat_settings()
        recover_interrupted_jobs()      # jobs whose owner has been silent longer than the heartbeat timeout
        start_background_loop(stop)     # keep our own jobs' heartbeat fresh; adopt jobs of instances that die later
    try:
        yield
    finally:
        stop.set()


app = FastAPI(
    title="Glynac Ingestion Platform",
    description="Salesforce / HubSpot / Slack ingestion services with ClickHouse analytical views.",
    version="1.0.0",
    lifespan=lifespan,
)

@app.exception_handler(InfrastructureUnavailable)
async def _infra_unavailable(_: Request, exc: InfrastructureUnavailable):
    return JSONResponse(status_code=503, content={"detail": str(exc)})


app.include_router(auth.router)
app.include_router(system.router)
app.include_router(jobs.router)
app.include_router(salesforce.router)
app.include_router(hubspot.router)
app.include_router(slack.router)
app.include_router(slack_events.router)
app.include_router(storage_browser.router)
app.include_router(analytics.router)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/ui", StaticFiles(directory=STATIC_DIR, html=True), name="ui")