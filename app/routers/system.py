from fastapi import APIRouter, Depends

from app.security import require_signed_request

router = APIRouter(tags=["system"])


@router.get("/health")
def health():
    """Deliberately unauthenticated — load balancers/monitors need this open."""
    return {"status": "ok"}


@router.post("/api/validate-credentials", dependencies=[Depends(require_signed_request)])
def validate_credentials():
    """
    Placeholder for Day 1-3: each service will plug in a real credential
    check here (mock OAuth token exchange, HubSpot API key ping, Slack
    bot-token auth.test call). For now it just proves the auth layer works.
    """
    return {"status": "ok", "message": "Signature valid. Wire up real credential checks per service here."}
