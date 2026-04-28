from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import pipeline_jobs
from .api import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    pipeline_jobs.startup()
    pipeline_jobs.start_poller()
    try:
        yield
    finally:
        pipeline_jobs.shutdown()


def create_app() -> FastAPI:
    app = FastAPI(title="Thesis Webserver", version="0.2.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)
    return app
