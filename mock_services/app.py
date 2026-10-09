from fastapi import FastAPI

from mock_services import salesforce


def create_app() -> FastAPI:
    """One process, one port; each service lives under its own prefix (/salesforce, /hubspot, /slack)."""
    app = FastAPI(title="Glynac mock services")
    app.mount("/salesforce", salesforce.create_app())

    @app.get("/health")
    def health():
        return {"status": "ok", "services": ["salesforce"]}

    return app
