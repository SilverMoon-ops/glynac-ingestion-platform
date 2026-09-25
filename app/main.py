import os

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.database import init_db
from app.routers import system, jobs, salesforce

app = FastAPI(
    title="Glynac Ingestion Platform",
    description="Salesforce / HubSpot / Slack ingestion services.",
    version="0.2.0",
)


@app.on_event("startup")
def on_startup():
    init_db()


app.include_router(system.router)
app.include_router(jobs.router)
app.include_router(salesforce.router)

# Day 2: app.include_router(hubspot.router)
# Day 3: app.include_router(slack.router)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/ui", StaticFiles(directory=STATIC_DIR, html=True), name="ui")
