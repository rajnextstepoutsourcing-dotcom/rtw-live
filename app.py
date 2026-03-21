
"""
app.py — RTW Check Service (NextStep SaaS)
Single-check tool with dashboard auth integration, embedded dispatcher,
configurable concurrency, and rerun-aware billing.
"""
import asyncio, logging, json, os, shutil, uuid, time, threading
from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import FastAPI, Request, HTTPException
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
TASK_TIMEOUT_SECONDS = max(60, int(os.environ.get("TASK_TIMEOUT_SECONDS", "600")))

JOB_PREFIX = "nextstep:rtw:job:"
OWNER_PREFIX = "nextstep:rtw:owner:"
PAYLOAD_PREFIX = "nextstep:rtw:payload:"
QUEUE_KEY = "nextstep:rtw:queue"
ACTIVE_KEY = "nextstep:rtw:active"

_redis = None
_dispatcher_started = False
_dispatcher_lock = threading.Lock()
_local_active = set()
_local_active_lock = threading.Lock()

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


def _payload_key(job_id: str) -> str:
    return f"{PAYLOAD_PREFIX}{job_id}"


def _decode(raw):
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


def _jget(job_id: str):
    r = get_redis()
    return _decode(r.get(_job_key(job_id))) if r else None


def _jset(job_id: str, state: Dict[str, Any]):
    r = get_redis()
    if not r:
        return
    try:
        r.setex(_job_key(job_id), 60 * 60 * 12, json.dumps(state))
    except Exception:
        pass


def _owner_set(job_id: str, tenant_id: int):
    r = get_redis()
    if not r:
        return
    try:
        r.setex(_owner_key(job_id), 60 * 60 * 12, str(tenant_id))
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


def _save_payload(job_id: str, payload: Dict[str, Any]):
    r = get_redis()
    if not r:
        return
    r.setex(_payload_key(job_id), 60 * 60 * 12, json.dumps(payload))


def _load_payload(job_id: str):
    r = get_redis()
    return _decode(r.get(_payload_key(job_id))) if r else None


def _queue_push(job_id: str):
    r = get_redis()
    if not r:
        return
    r.rpush(QUEUE_KEY, job_id)


def _queue_all():
    r = get_redis()
    if not r:
        return []
    vals = r.lrange(QUEUE_KEY, 0, -1) or []
    out = []
    for v in vals:
        if isinstance(v, bytes):
            v = v.decode("utf-8")
        out.append(v)
    return out


def _queue_remove(job_id: str):
    r = get_redis()
    if r:
        try:
            r.lrem(QUEUE_KEY, 0, job_id)
        except Exception:
            pass


def _active_count() -> int:
    with _local_active_lock:
        return len(_local_active)


def _set_local_active(job_id: str, active: bool):
    with _local_active_lock:
        if active:
            _local_active.add(job_id)
        else:
            _local_active.discard(job_id)


def _storage(tenant_id, user_id, job_id):
    p = STORAGE_ROOT / str(tenant_id) / str(user_id) / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _cleanup(storage_path: Path, job_id: str):
    time.sleep(5)
    try:
        if storage_path.exists():
            shutil.rmtree(storage_path, ignore_errors=True)
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
    token = (request.headers.get("X-NextStep-Token") or request.cookies.get("ns_token") or request.query_params.get("ns_token") or "")
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


def _normalize_dob_day(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    try:
        n = int(s)
        return f"{n:02d}" if 1 <= n <= 31 else ""
    except Exception:
        return ""


def _normalize_dob_month(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.isdigit():
        n = int(s)
        return f"{n:02d}" if 1 <= n <= 12 else ""
    token = ''.join(ch for ch in s.upper() if ch.isalnum())
    token = token.replace('0', 'O').replace('1', 'I').replace('5', 'S').replace('8', 'B')
    aliases = {"AUGI": "AUG", "AUGL": "AUG", "SEPTEM8ER": "SEPTEMBER", "0CT": "OCT"}
    token = aliases.get(token, token)
    months = {
        "JAN": "01", "JANUARY": "01", "FEB": "02", "FEBRUARY": "02", "MAR": "03", "MARCH": "03",
        "APR": "04", "APRIL": "04", "MAY": "05", "JUN": "06", "JUNE": "06", "JUL": "07", "JULY": "07",
        "AUG": "08", "AUGUST": "08", "SEP": "09", "SEPT": "09", "SEPTEMBER": "09", "OCT": "10", "OCTOBER": "10",
        "NOV": "11", "NOVEMBER": "11", "DEC": "12", "DECEMBER": "12",
    }
    return months.get(token, "")


def _normalize_dob_year(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    try:
        n = int(s)
        if n < 100:
            n = 2000 + n if n <= 30 else 1900 + n
        return str(n) if 1900 <= n <= 2100 else ""
    except Exception:
        return ""


def _is_billable(state: Dict[str, Any]) -> bool:
    return bool((state.get("pdf_url") or "").strip()) and state.get("state") == "done"


def _run_job_sync(job_id: str):
    payload = _load_payload(job_id) or {}
    if not payload:
        _jset(job_id, {"state": "failed", "message": "Missing job payload.", "pdf_url": "", "filename": "", "error": "Missing payload"})
        return
    tenant_id = payload["tenant_id"]
    user_id = payload["user_id"]
    db_job_id = payload.get("db_job_id")
    prev_job_id = payload.get("previous_job_id") or ""
    edited_after_run = bool(payload.get("edited_after_run"))
    storage_path = _storage(tenant_id, user_id, job_id)
    try:
        import db
        if db_job_id:
            db.update_job_status(db_job_id=db_job_id, status="running", successful_items=0, failed_items=0)
    except Exception as e:
        log.warning("[RTW] mark running failed: %s", e)

    _jset(job_id, {"state": "running", "message": "Running RTW check on GOV.UK…", "pdf_url": "", "filename": "", "error": "", "billable": False})
    try:
        from rtw_runner import run_rtw_check_and_download_pdf
        result = asyncio.run(asyncio.wait_for(run_rtw_check_and_download_pdf(
            share_code=payload["share_code"],
            dob_day=payload["dob_day"],
            dob_month=payload["dob_month"],
            dob_year=payload["dob_year"],
            company_name=payload["company_name"],
            out_dir=storage_path,
        ), timeout=TASK_TIMEOUT_SECONDS))
    except Exception as e:
        result = {"ok": False, "error": str(e)}

    if not isinstance(result, dict) or not result.get("ok"):
        err = (result.get("error") or "RTW automation failed") if isinstance(result, dict) else "RTW automation failed"
        error_pdf = (result.get("error_pdf") or "") if isinstance(result, dict) else ""
        has_error_pdf = bool(error_pdf and Path(error_pdf).exists())
        state = {
            "state": "failed",
            "message": "",
            "pdf_url": "",
            "filename": "",
            "error": "",
            "billable": has_error_pdf,
            "billing_waived": False,
        }
        if has_error_pdf:
            err_name = Path(error_pdf).name
            state.update({"pdf_url": f"/rtw/download/{job_id}/{err_name}", "filename": err_name})
        _jset(job_id, state)
        try:
            import db
            if prev_job_id and edited_after_run:
                prev_payload = _load_payload(prev_job_id) or {}
                prev_db_job_id = prev_payload.get("db_job_id")
                prev_state = _jget(prev_job_id) or {}
                if prev_db_job_id and prev_state.get("billable") and not bool(prev_state.get("billing_waived")):
                    db.reverse_usage(tenant_id=tenant_id, user_id=user_id, db_job_id=prev_db_job_id, reversed_outputs=1)
                    prev_state["billing_waived"] = True
                    prev_state["billable"] = False
                    prev_state["message"] = ""
                    _jset(prev_job_id, prev_state)
            if db_job_id:
                db.update_job_status(db_job_id=db_job_id, status="failed", successful_items=0, failed_items=1)
                if has_error_pdf:
                    db.record_usage(tenant_id=tenant_id, user_id=user_id, db_job_id=db_job_id, successful_outputs=1)
        except Exception as e:
            log.warning("[RTW] DB update/usage failed: %s", e)
        return

    pdf_path = str(result.get("pdf_path") or "").strip()
    if not pdf_path or not Path(pdf_path).exists():
        _jset(job_id, {"state": "failed", "message": "RTW finished but PDF not found.", "pdf_url": "", "filename": "", "error": "PDF not found", "billable": False})
        try:
            import db
            if db_job_id:
                db.update_job_status(db_job_id=db_job_id, status="failed", successful_items=0, failed_items=1)
        except Exception:
            pass
        return

    filename = Path(pdf_path).name
    pdf_url = f"/rtw/download/{job_id}/{filename}"
    state = {"state": "done", "message": "Complete — your RTW PDF is ready.", "pdf_url": pdf_url, "filename": filename, "error": "", "billable": True, "billing_waived": False}
    _jset(job_id, state)
    try:
        import db
        if prev_job_id and edited_after_run:
            prev_payload = _load_payload(prev_job_id) or {}
            prev_db_job_id = prev_payload.get("db_job_id")
            prev_state = _jget(prev_job_id) or {}
            if prev_db_job_id and prev_state.get("billable") and not bool(prev_state.get("billing_waived")):
                db.reverse_usage(tenant_id=tenant_id, user_id=user_id, db_job_id=prev_db_job_id, reversed_outputs=1)
                prev_state["billing_waived"] = True
                prev_state["billable"] = False
                prev_state["message"] = (prev_state.get("message") or "") + " Waived after edited rerun."
                _jset(prev_job_id, prev_state)
        if db_job_id:
            db.update_job_status(db_job_id=db_job_id, status="completed", successful_items=1, failed_items=0)
            db.record_usage(tenant_id=tenant_id, user_id=user_id, db_job_id=db_job_id, successful_outputs=1)
    except Exception as e:
        log.warning("[RTW] DB update/usage failed: %s", e)


def _worker_loop():
    log.info("[Dispatcher] RTW dispatcher started with max=%s", MAX_CONCURRENT_TASKS)
    while True:
        try:
            q = _queue_all()
            if not q:
                time.sleep(WORKER_POLL_INTERVAL)
                continue
            while _active_count() < MAX_CONCURRENT_TASKS:
                q = _queue_all()
                if not q:
                    break
                job_id = q[0]
                state = _jget(job_id) or {}
                if state.get("state") != "queued":
                    _queue_remove(job_id)
                    continue
                _queue_remove(job_id)
                _set_local_active(job_id, True)
                def runner(jid=job_id):
                    try:
                        _run_job_sync(jid)
                    finally:
                        _set_local_active(jid, False)
                threading.Thread(target=runner, daemon=True).start()
            time.sleep(WORKER_POLL_INTERVAL)
        except Exception as e:
            log.warning("[Dispatcher] %s", e)
            time.sleep(WORKER_POLL_INTERVAL)


def _ensure_dispatcher_started():
    global _dispatcher_started
    with _dispatcher_lock:
        if _dispatcher_started:
            return
        threading.Thread(target=_worker_loop, daemon=True).start()
        _dispatcher_started = True


@app.on_event("startup")
def _startup():
    _ensure_dispatcher_started()


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "dashboard_url": APP_DASHBOARD_URL, "login_url": APP_LOGIN_URL})


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
    return {"ok": True, "redis": ok, "db": bool(os.getenv("DATABASE_URL")), "dispatcher": _dispatcher_started, "max_concurrent": MAX_CONCURRENT_TASKS}


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
    return extract_rtw_fields(share_file_bytes=share_bytes, dob_file_bytes=dob_bytes, share_filename=getattr(share_up, "filename", "share"), dob_filename=getattr(dob_up, "filename", "dob"))


@app.post("/rtw/run")
async def rtw_run(request: Request):
    _ensure_dispatcher_started()
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

    share_code = normalize_share_code(str(data.get("share_code", "") or "").strip())
    dob_day = _normalize_dob_day(data.get("dob_day"))
    dob_month = _normalize_dob_month(data.get("dob_month"))
    dob_year = _normalize_dob_year(data.get("dob_year"))
    company_name = str(data.get("company_name", "") or "").strip()
    previous_job_id = str(data.get("previous_job_id", "") or "").strip()
    edited_after_run = bool(data.get("edited_after_run"))

    if not share_code:
        raise HTTPException(400, "Invalid share code (must be 9 alphanumeric characters).")
    if not (dob_day and dob_month and dob_year):
        raise HTTPException(400, "DOB must be valid Day/Month/Year.")
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
    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=1)
    except Exception as e:
        log.warning("[Run] DB record failed: %s", e)

    payload = {
        "job_id": job_id,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "db_job_id": db_job_id,
        "share_code": share_code,
        "dob_day": dob_day,
        "dob_month": dob_month,
        "dob_year": dob_year,
        "company_name": company_name,
        "previous_job_id": previous_job_id,
        "edited_after_run": edited_after_run,
    }
    _save_payload(job_id, payload)
    _owner_set(job_id, tenant_id)
    queued_pos = len(_queue_all()) + _active_count() + 1
    _jset(job_id, {"state": "queued", "message": f"Queued — position {queued_pos}.", "pdf_url": "", "filename": "", "error": "", "billable": False})
    _queue_push(job_id)
    return JSONResponse({"ok": True, "job_id": job_id, "status_url": f"/rtw/status/{job_id}"})


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
    return FileResponse(str(file_path), filename=name, media_type="application/pdf", background=bg)
