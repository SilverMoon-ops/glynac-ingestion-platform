import os

import uvicorn

from mock_services.app import create_app

if __name__ == "__main__":
    uvicorn.run(create_app(), host=os.getenv("MOCK_SERVICES_HOST", "0.0.0.0"), port=int(os.getenv("MOCK_SERVICES_PORT", "9000")))
