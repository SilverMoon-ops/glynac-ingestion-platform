from fastapi import FastAPI

from app.database import init_db
from app.routers import system, jobs

app = FastAPI(
    title="Glynac Ingestion Platform",
    description="Salesforce / HubSpot / Slack ingestion services — Day 0 shared scaffold.",
    version="0.1.0",
)


@app.on_event("startup")
def on_startup():
    init_db()


app.include_router(system.router)
app.include_router(jobs.router)

# Day 1: app.include_router(salesforce.router)
# Day 2: app.include_router(hubspot.router)
# Day 3: app.include_router(slack.router)
