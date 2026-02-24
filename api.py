"""
api.py — FastAPI backend for AutoGTM.

Uses PipelineOrchestrator for DRY pipeline execution, TaskManager for
thread-safe state, and BrowserPool for efficient Playwright lifecycle.
"""
import json
import asyncio
import urllib.parse
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, Form, HTTPException
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.background import BackgroundTask
from pydantic import BaseModel
from typing import Dict, Any

from task_manager import (
    TaskManager, TaskStatus,
    CapacityExceededError, TaskNotFoundError, InvalidTransitionError,
)
from browser_pool import BrowserPool
from core_pipeline import PipelineOrchestrator, PipelineConfig, PipelineStage, PipelineResult
from cleanup import cleanup_session


# ── Globals ─────────────────────────────────────────────────────────────────

task_manager = TaskManager(max_concurrent=5, ttl_seconds=3600)
_gc_task: asyncio.Task | None = None


# ── FastAPI Event Listener ──────────────────────────────────────────────────

class FastAPIPipelineListener:
    """Adapts PipelineEventListener to TaskState mutations for the polling API."""

    def __init__(self, task):
        self._task = task

    def on_step_start(self, stage: PipelineStage, message: str) -> None:
        self._task.add_log(message)

    def on_step_complete(self, stage: PipelineStage, message: str) -> None:
        self._task.add_log(message)

    def on_log(self, message: str) -> None:
        self._task.add_log(message)

    def on_error(self, stage: PipelineStage, error: Exception) -> None:
        self._task.status = TaskStatus.ERROR
        self._task.error = str(error)
        self._task.add_log(f"CRITICAL ERROR in {stage.value}: {error}")

    def on_review_ready(self, draft_plan: list[dict], crawler_data: dict) -> None:
        self._task.tracking_plan = {"tracking_plan": draft_plan}
        self._task.crawler_data = crawler_data
        self._task.status = TaskStatus.REVIEW_REQUIRED

    def on_complete(self, result: PipelineResult) -> None:
        self._task.status = TaskStatus.COMPLETED


# ── Background Worker ───────────────────────────────────────────────────────

async def process_task(task_id: str, gemini_key: str):
    """Background worker: runs Phase 1 (crawl + AI) via PipelineOrchestrator."""
    task = await task_manager.get_task(task_id)
    if not task:
        return

    listener = FastAPIPipelineListener(task)
    config = PipelineConfig(
        target_url=task.target_url,
        gtm_data=task.gtm_data,
        gemini_api_key=gemini_key,
    )
    orchestrator = PipelineOrchestrator(
        config, listener, browser_pool=BrowserPool.get_instance()
    )
    await task_manager.update_task(task_id, orchestrator=orchestrator, session_id=config.session_id)
    await orchestrator.analyze()


# ── App Lifespan ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app):
    global _gc_task
    pool = BrowserPool.get_instance(max_contexts=4)
    _gc_task = asyncio.create_task(task_manager.gc_loop(interval=60))
    yield
    _gc_task.cancel()
    await pool.shutdown()


app = FastAPI(title="AutoGTM Backend API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Endpoints ───────────────────────────────────────────────────────────────

@app.post("/api/analyze")
async def analyze_url(
    target_url: str = Form(...),
    gemini_key: str = Form(...),
    gtm_file: UploadFile = Form(...),
):
    contents = await gtm_file.read()
    try:
        gtm_data = json.loads(contents.decode("utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid GTM Container JSON file")

    try:
        task = await task_manager.create_task(target_url=target_url, gtm_data=gtm_data)
    except CapacityExceededError as e:
        raise HTTPException(status_code=429, detail=str(e))

    asyncio.create_task(process_task(task.task_id, gemini_key))

    return {
        "status": "success",
        "task_id": task.task_id,
        "message": "Task queued for background processing.",
    }


@app.get("/api/status/{task_id}")
async def get_task_status(task_id: str):
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task.to_api_dict()


class CompileRequest(BaseModel):
    approved_plan: Dict[str, Any]


@app.post("/api/compile/{task_id}")
async def compile_gtm(task_id: str, request: CompileRequest):
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status not in (TaskStatus.REVIEW_REQUIRED, TaskStatus.COMPLETED):
        raise HTTPException(status_code=400, detail="Task is not ready for compilation.")

    orchestrator = task.orchestrator
    if not orchestrator:
        raise HTTPException(status_code=400, detail="No orchestrator found — re-run analysis.")

    plan = request.approved_plan
    items = plan.get("tracking_plan", []) if isinstance(plan, dict) else []

    try:
        task.status = TaskStatus.COMPILING
        result = await orchestrator.compile(items)
        await task_manager.update_task(
            task_id,
            compiled_gtm=result.compiled_gtm,
            validation_report=result.validation_report,
        )
        task.status = TaskStatus.COMPLETED

        return {
            "status": "success",
            "validation_report": result.validation_report,
            "modified_gtm": result.compiled_gtm,
        }
    except Exception as e:
        task.status = TaskStatus.ERROR
        task.error = str(e)
        raise HTTPException(status_code=500, detail=f"Compilation Error: {str(e)}")


@app.get("/api/download/{task_id}")
async def download_gtm(task_id: str):
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    compiled = task.compiled_gtm
    if not compiled:
        raise HTTPException(
            status_code=400,
            detail="GTM container not yet compiled. Run Approve & Compile first.",
        )

    json_bytes = json.dumps(compiled, indent=2, ensure_ascii=False).encode("utf-8")

    domain = urllib.parse.urlparse(task.target_url).netloc or "unknown_domain"
    domain = domain.replace("www.", "")
    filename = f"AutoGTM_Enhanced_Container_{domain}.json"

    # Cleanup debug artifacts after the response is sent
    bg_cleanup = None
    if task.session_id:
        bg_cleanup = BackgroundTask(cleanup_session, task.session_id)

    return Response(
        content=json_bytes,
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "application/json; charset=utf-8",
        },
        background=bg_cleanup,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
