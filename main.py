import logging

import uvicorn
from fastapi import FastAPI

from transactional_clock.controller.api import operations

logging.basicConfig(level=logging.DEBUG)


api = FastAPI()
api.include_router(operations)

if __name__ == "__main__":
    uvicorn.run("main:api", host="0.0.0.0", port=5001, reload=False)
