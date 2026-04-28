from __future__ import annotations

from fastapi import APIRouter
from fastapi import Body
from fastapi import File
from fastapi import Form
from fastapi import UploadFile
from fastapi.responses import Response

from . import instance_service
from . import runtime
from . import run_service
from . import sync_service
from . import upload_service


router = APIRouter()


@router.get("/api/health")
def health() -> dict[str, str]:
    return sync_service.health()


@router.get("/api/sync/index")
def sync_index() -> dict[str, object]:
    return sync_service.file_index()


@router.get("/api/sync/file")
def sync_file(path: str) -> Response:
    content, media_type = sync_service.file_content(path)
    return Response(content=content, media_type=media_type)


@router.post("/api/instances")
def create_instance(payload: dict[str, object] = Body(...)) -> dict[str, object]:
    return instance_service.create_instance(payload)


@router.get("/api/instances")
def list_instances() -> dict[str, object]:
    return instance_service.list_instances()


@router.delete("/api/instances/{instance_id}")
def delete_instance(instance_id: str) -> dict[str, object]:
    return instance_service.delete_instance(instance_id)


@router.post("/api/instances/{instance_id}/runs")
def create_run_for_instance(
    instance_id: str,
    runtime_id: str = Form("local"),
    parameters_json: str = Form(""),
) -> dict[str, object]:
    return run_service.create_run(instance_id=instance_id, runtime_id=runtime_id, parameters_json=parameters_json)


@router.post("/api/runs/{run_id}/stop")
def stop_run(run_id: str) -> dict[str, object]:
    return run_service.stop_run(run_id)


@router.delete("/api/runs/{run_id}")
def delete_run(run_id: str) -> dict[str, object]:
    return run_service.delete_run(run_id)


@router.get("/api/runtimes")
def runtimes() -> dict[str, object]:
    return runtime.list_runtime_rows()


@router.post("/api/runtimes/{runtime_id}/probe")
def probe_runtime(runtime_id: str) -> dict[str, object]:
    return runtime.probe_runtime_row(runtime_id, repair=False)


@router.post("/api/uploads/file")
async def upload_file(
    relative_path: str = Form(...),
    file: UploadFile = File(...),
) -> dict[str, object]:
    data = await file.read()
    return upload_service.upload_artifact(relative_path=relative_path, data=data)


@router.delete("/api/uploads/file")
def delete_file(relative_path: str) -> dict[str, object]:
    return upload_service.delete_artifact(relative_path=relative_path)


@router.post("/api/uploads/instance-json")
async def upload_instance_json(
    run_id: str = Form(...),
    kind: str = Form(...),
    file: UploadFile = File(...),
) -> dict[str, object]:
    data = await file.read()
    return upload_service.upload_instance_json(run_id=run_id, kind=kind, filename=file.filename or "", data=data)
