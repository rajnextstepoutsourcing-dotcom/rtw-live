"""
app.py — RTW Check Service (NextStep SaaS)
Single-check full integration version: auth/session acceptance, embedded worker queue,
configurable concurrency, rerun-aware billing, isolated storage, ownership checks.
"""
import asyncio
import json
import logging
import os
import shutil
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from starlette.middleware.base import BaseHTTPMiddleware

from rtw_extract import extract_rtw_fields, normalize_share_code

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger(__name__)

APP_DIR = Path(__file__).resolve().parent
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
STORAGE_ROOT = Path("/tmp/nextstep")
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
BACKEND_VALIDATE_URL = os.environ.get("BACKEND_VALIDATE_URL", "https://nextstep-backend-e75l.onrender.com/api/validate-session")
APP_DASHBOARD_URL = os.environ.get("APP_DASHBOARD_URL", "https://nextstep-backend-e75l.onrender.com/dashboard")
APP_LOGIN_URL = os.environ.get("APP_LOGIN_URL", "https://nextstep-backend-e75l.onrender.com/login")
MAX_CONCURRENT_TASKS = max(1, int(os.environ.get("MAX_CONCURRENT_TASKS", "1")))
WORKER_POLL_INTERVAL = max(1, int(os.environ.get("WORKER_POLL_INTERVAL", "2")))

JOB_PREFIX = "nextstep:rtw:job:"
OWNER_PREFIX = "nextstep:rtw:owner:"
_redis = None
_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_TASKS)


class PersistNextStepTokenMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        token = request.query_params.get("ns_token")
        if token:
            response.set_cookie(key="ns_token", value=token, httponly=True, samesite="lax", secure=True, max_age=60 * 60 * 8)
        return response


app = FastAPI(title="RTW Check — NextStep")
app.add_middleware(PersistNextStepTokenMiddleware)
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def get_redis():
    global _redis
    if _redis is None:
        try:
            import redis as rl
            _redis = rl.Redis.from_url(REDIS_URL, decode_responses=False)
            _redis.ping()
        except Exception as e:
            log.error("[Redis] %s", e)
            _redis = None
    return _redis


def _job_key(job_id: str) -> str:
    return f"{JOB_PREFIX}{job_id}"


def _owner_key(job_id: str) -> str:
    return f"{OWNER_PREFIX}{job_id}"


def _jget(job_id: str) -> Optional[dict]:
    r = get_redis()
    if not r:
        return None
    try:
        raw = r.get(_job_key(job_id))
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


def _jset(job_id: str, state: dict):
    r = get_redis()
    if not r:
        return
    try:
        r.setex(_job_key(job_id), 60 * 60 * 8, json.dumps(state))
    except Exception:
        pass


def _owner_set(job_id: str, tenant_id: int):
    r = get_redis()
    if not r:
        return
    try:
        r.setex(_owner_key(job_id), 60 * 60 * 8, str(tenant_id))
    except Exception:
        pass


def _owner_get(job_id: str):
    r = get_redis()
    if not r:
        return None
    try:
        v = r.get(_owner_key(job_id))
        return int(v) if v else None
    except Exception:
        return None


def _storage(tenant_id: int, user_id: int, job_id: str) -> Path:
    p = STORAGE_ROOT / str(tenant_id) / str(user_id) / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _cleanup(storage_path: Path, job_id: str):
    time.sleep(5)
    try:
        if storage_path.exists():
            shutil.rmtree(storage_path, ignore_errors=True)
    except Exception as e:
        log.warning("[Cleanup] %s", e)
    r = get_redis()
    if r:
        try:
            r.delete(_job_key(job_id))
            r.delete(_owner_key(job_id))
        except Exception:
            pass


def _validate_via_backend(token: str):
    if not token:
        return None
    try:
        import requests
        resp = requests.get(BACKEND_VALIDATE_URL, params={"token": token}, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data.get("valid"):
            return None
        user = data.get("user") or {}
        tenant = data.get("tenant") or {}
        return {
            "user_id": user.get("id"),
            "tenant_id": tenant.get("id"),
            "role": user.get("role", "admin"),
            "email": user.get("email"),
            "name": user.get("name"),
        }
    except Exception as e:
        log.warning("[Auth backend] %s", e)
        return None


def _get_ctx(request: Request):
    token = request.headers.get("X-NextStep-Token") or request.cookies.get("ns_token") or request.query_params.get("ns_token") or ""
    if not token:
        return None
    ctx = _validate_via_backend(token)
    if ctx:
        return ctx
    try:
        import db
        return db.validate_user_token(token)
    except Exception as e:
        log.warning("[Auth db] %s", e)
        return None


def _auth(request: Request):
    ctx = _get_ctx(request)
    if not ctx:
        raise HTTPException(401, f"Not authenticated. Please log in at {APP_LOGIN_URL}")
    return ctx


def _set_job_state(job_id: str, **updates):
    state = _jget(job_id) or {}
    state.update(updates)
    _jset(job_id, state)


def _queue_position_message() -> str:
    return "Queued — processing starts shortly..." if MAX_CONCURRENT_TASKS == 1 else "Queued — waiting for a free RTW worker slot..."


def _process_job(job_id: str, payload: dict):
    queued = not _semaphore.acquire(blocking=False)
    if queued:
        _set_job_state(job_id, state="queued", message=_queue_position_message())
        _semaphore.acquire()
    try:
        _set_job_state(job_id, state="running", message="Running RTW check on GOV.UK…")
        db_job_id = payload.get("db_job_id")
        try:
            import db
            if db_job_id:
                db.update_job_status(db_job_id=db_job_id, status="running", successful_items=0, failed_items=0)
        except Exception as e:
            log.warning("[RTW] update running failed: %s", e)

        from rtw_runner import run_rtw_check_and_download_pdf
        result = asyncio.run(run_rtw_check_and_download_pdf(
            share_code=payload["share_code"],
            dob_day=payload["dob_day"],
            dob_month=payload["dob_month"],
            dob_year=payload["dob_year"],
            company_name=payload["company_name"],
            out_dir=Path(payload["storage_path"]),
        ))

        if not isinstance(result, dict) or not result.get("ok"):
            err = (result.get("error") or "Unknown error") if isinstance(result, dict) else "Unknown error"
            error_pdf = (result.get("error_pdf") or "") if isinstance(result, dict) else ""
            pdf_url = ""
            filename = ""
            if error_pdf and Path(error_pdf).exists():
                filename = Path(error_pdf).name
                pdf_url = f"/rtw/download/{job_id}/{filename}"
            _set_job_state(job_id, state="failed", message=err, pdf_url=pdf_url, filename=filename, error=err, billable=False)
            try:
                import db
                if db_job_id:
                    db.update_job_status(db_job_id=db_job_id, status="failed", successful_items=0, failed_items=1)
            except Exception as e:
                log.warning("[RTW] update failed status error: %s", e)
            return

        pdf_path = (result.get("pdf_path") or "").strip()
        if not pdf_path or not Path(pdf_path).exists():
            _set_job_state(job_id, state="failed", message="RTW finished but PDF not found.", pdf_url="", filename="", error="PDF missing", billable=False)
            try:
                import db
                if db_job_id:
                    db.update_job_status(db_job_id=db_job_id, status="failed", successful_items=0, failed_items=1)
            except Exception as e:
                log.warning("[RTW] missing pdf update error: %s", e)
            return

        filename = Path(pdf_path).name
        pdf_url = f"/rtw/download/{job_id}/{filename}"
        _set_job_state(job_id, state="done", message="Complete — your RTW PDF is ready.", pdf_url=pdf_url, filename=filename, error="", billable=True)

        try:
            import db
            old_job_id = payload.get("previous_job_id") if payload.get("edited_after_previous") else None
            if old_job_id:
                old_state = _jget(old_job_id) or {}
                if old_state.get("billable") and not old_state.get("billing_waived"):
                    old_db_job_id = old_state.get("db_job_id")
                    db.reverse_usage(tenant_id=payload["tenant_id"], user_id=payload["user_id"], db_job_id=old_db_job_id, reversed_outputs=1)
                    old_state["billing_waived"] = True
                    old_state["message"] = "Earlier charge waived after user edit and rerun."
                    _jset(old_job_id, old_state)
            if db_job_id:
                db.update_job_status(db_job_id=db_job_id, status="completed", successful_items=1, failed_items=0)
                db.record_usage(tenant_id=payload["tenant_id"], user_id=payload["user_id"], db_job_id=db_job_id, successful_outputs=1)
        except Exception as e:
            log.warning("[RTW] usage update failed: %s", e)
    except Exception as e:
        err = str(e)
        log.exception("[RTW] job failed")
        _set_job_state(job_id, state="failed", message=f"RTW automation failed: {err}", pdf_url="", filename="", error=err, billable=False)
        try:
            import db
            db_job_id = payload.get("db_job_id")
            if db_job_id:
                db.update_job_status(db_job_id=db_job_id, status="failed", successful_items=0, failed_items=1)
        except Exception:
            pass
    finally:
        _semaphore.release()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "dashboard_url": APP_DASHBOARD_URL,
        "login_url": APP_LOGIN_URL,
    })


@app.get("/health")
def health():
    r = get_redis()
    ok = False
    try:
        if r:
            r.ping()
            ok = True
    except Exception:
        pass
    return {"ok": True, "redis": ok, "db": bool(os.getenv("DATABASE_URL")), "max_concurrent_tasks": MAX_CONCURRENT_TASKS}


@app.post("/rtw/extract")
async def rtw_extract(request: Request):
    _auth(request)
    form = await request.form()
    share_up = (form.get("sharecode_file") or form.get("sharecode_file1") or form.get("share_file") or form.get("share_file1"))
    dob_up = (form.get("dob_file") or form.get("dob_file1") or form.get("dobfile") or form.get("dobfile1"))
    if not share_up or not dob_up:
        return JSONResponse(status_code=422, content={"error": "Missing files"})
    try:
        share_bytes = await share_up.read()
        dob_bytes = await dob_up.read()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    result = extract_rtw_fields(
        share_file_bytes=share_bytes,
        dob_file_bytes=dob_bytes,
        share_filename=getattr(share_up, "filename", "share"),
        dob_filename=getattr(dob_up, "filename", "dob"),
    )
    return result


@app.post("/rtw/run")
async def rtw_run(request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]
    user_id = ctx["user_id"]

    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            data = await request.json()
        except Exception:
            data = {}
    else:
        form = await request.form()
        data = dict(form)

    share_code = str(data.get("share_code", "") or "").strip()
    dob_day = str(data.get("dob_day", "") or "").strip()
    dob_month = str(data.get("dob_month", "") or "").strip()
    dob_year = str(data.get("dob_year", "") or "").strip()
    company_name = str(data.get("company_name", "") or "").strip()
    previous_job_id = str(data.get("previous_job_id", "") or "").strip()
    edited_after_previous = str(data.get("edited_after_previous", "") or "").strip().lower() in ("1", "true", "yes")

    sc = normalize_share_code(share_code)
    if not sc:
        raise HTTPException(400, "Invalid share code (must be 9 alphanumeric characters).")
    try:
        d = int(dob_day)
        m = int(dob_month)
        y = int(dob_year)
    except Exception:
        raise HTTPException(400, "DOB must be numeric Day/Month/Year.")
    if not (1 <= d <= 31 and 1 <= m <= 12 and 1900 <= y <= 2100):
        raise HTTPException(400, "DOB out of range.")
    if not company_name:
        raise HTTPException(400, "Company name is required.")

    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0:
            raise HTTPException(402, "No tokens remaining. Please contact NextStep to top up.")
    except HTTPException:
        raise
    except Exception as e:
        log.warning("[Run] Token check skipped: %s", e)

    job_id = str(uuid.uuid4())
    storage_path = _storage(tenant_id, user_id, job_id)
    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=1)
    except Exception as e:
        log.warning("[Run] DB record failed: %s", e)

    state = {
        "state": "queued",
        "message": _queue_position_message(),
        "pdf_url": "",
        "filename": "",
        "error": "",
        "billable": False,
        "billing_waived": False,
        "db_job_id": db_job_id,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "previous_job_id": previous_job_id,
        "edited_after_previous": edited_after_previous,
    }
    _jset(job_id, state)
    _owner_set(job_id, tenant_id)

    payload = {
        "tenant_id": tenant_id,
        "user_id": user_id,
        "share_code": sc,
        "dob_day": str(d),
        "dob_month": str(m),
        "dob_year": str(y),
        "company_name": company_name,
        "storage_path": str(storage_path),
        "db_job_id": db_job_id,
        "previous_job_id": previous_job_id,
        "edited_after_previous": edited_after_previous,
    }
    threading.Thread(target=_process_job, args=(job_id, payload), daemon=True).start()

    return JSONResponse({
        "ok": True,
        "job_id": job_id,
        "status_url": f"/rtw/status/{job_id}",
        "queued": True,
    })


@app.get("/rtw/status/{job_id}")
async def rtw_status(job_id: str, request: Request):
    _auth(request)
    state = _jget(job_id)
    if not state:
        raise HTTPException(404, "Job expired or not found.")
    return JSONResponse(state)


@app.get("/rtw/download/{job_id}/{name}")
async def rtw_download(job_id: str, name: str, request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]
    job_tenant = _owner_get(job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        raise HTTPException(403, "Access denied.")

    tenant_root = STORAGE_ROOT / str(tenant_id)
    file_path = None
    for p in tenant_root.rglob(name):
        if job_id in str(p):
            file_path = p
            break
    if not file_path or not file_path.exists():
        raise HTTPException(404, "Download expired or not found.")

    sp = STORAGE_ROOT / str(tenant_id) / str(ctx["user_id"]) / job_id
    bg = BackgroundTask(_cleanup, sp, job_id)
    media = "application/pdf" if str(file_path).lower().endswith(".pdf") else "application/octet-stream"
    return FileResponse(str(file_path), filename=name, media_type=media, background=bg)
