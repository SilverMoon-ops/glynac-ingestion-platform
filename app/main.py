import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.database import init_db
from app.routers import system, jobs, salesforce, hubspot, slack, analytics


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Glynac Ingestion Platform",
    description="Salesforce / HubSpot / Slack ingestion services with ClickHouse analytical views.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(system.router)
app.include_router(jobs.router)
app.include_router(salesforce.router)
app.include_router(hubspot.router)
app.include_router(slack.router)
app.include_router(analytics.router)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/ui", StaticFiles(directory=STATIC_DIR, html=True), name="ui")