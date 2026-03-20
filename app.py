"""
app.py — RTW Check Service (NextStep SaaS)
Single check per run. Auth, isolated storage, Redis job tracking,
DB record, token deduction, ownership check on download, DBS dark theme.
"""
import asyncio, logging, json, os, shutil, uuid, time
from pathlib import Path
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger(__name__)

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask

from rtw_extract import extract_rtw_fields, normalize_share_code

APP_DIR      = Path(__file__).resolve().parent
REDIS_URL    = os.environ.get("REDIS_URL", "redis://localhost:6379")
STORAGE_ROOT = Path("/tmp/nextstep")
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="RTW Check — NextStep")
app.mount("/static",    StaticFiles(directory=str(APP_DIR / "static")),    name="static")
app.mount("/templates_docx", StaticFiles(directory=str(APP_DIR / "templates_docx")), name="templates_docx") \
    if (APP_DIR / "templates_docx").exists() else None
templates = Jinja2Templates(directory=str(APP_DIR / "templates"))

# Semaphore — 1 RTW job at a time (single check, lightweight)
_sem = asyncio.Semaphore(1)

# ── Redis ─────────────────────────────────────────────────────────────────────
_redis = None
def get_redis():
    global _redis
    if _redis is None:
        try:
            import redis as rl
            _redis = rl.Redis.from_url(REDIS_URL, decode_responses=False)
            _redis.ping()
        except Exception as e:
            log.error("[Redis] %s", e); _redis = None
    return _redis

def _jget(job_id):
    r = get_redis()
    if not r: return None
    try:
        raw = r.get(f"nextstep:rtw:job:{job_id}")
        return json.loads(raw) if raw else None
    except: return None

def _jset(job_id, state):
    r = get_redis()
    if not r: return
    try: r.setex(f"nextstep:rtw:job:{job_id}", 3600, json.dumps(state))
    except: pass

def _owner_set(job_id, tenant_id):
    r = get_redis()
    if not r: return
    try: r.setex(f"nextstep:rtw:owner:{job_id}", 3600, str(tenant_id))
    except: pass

def _owner_get(job_id):
    r = get_redis()
    if not r: return None
    try:
        v = r.get(f"nextstep:rtw:owner:{job_id}")
        return int(v) if v else None
    except: return None

# ── Auth ──────────────────────────────────────────────────────────────────────
def _get_ctx(request: Request):
    token = (request.headers.get("X-NextStep-Token")
             or request.cookies.get("ns_token")
             or request.query_params.get("ns_token") or "")
    if not token: return None
    try:
        import db; return db.validate_user_token(token)
    except Exception as e:
        log.warning("[Auth] %s", e); return None

def _auth(request: Request):
    ctx = _get_ctx(request)
    if not ctx:
        raise HTTPException(401, "Not authenticated. Please log in at nextstep.co.uk")
    return ctx

# ── Storage ───────────────────────────────────────────────────────────────────
def _storage(tenant_id, user_id, job_id):
    p = STORAGE_ROOT / str(tenant_id) / str(user_id) / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p

def _cleanup(storage_path: Path, job_id: str):
    time.sleep(5)
    try:
        if storage_path.exists(): shutil.rmtree(storage_path, ignore_errors=True)
        log.info("[Cleanup] Deleted %s", storage_path)
    except Exception as e: log.warning("[Cleanup] %s", e)
    r = get_redis()
    if r:
        try:
            r.delete(f"nextstep:rtw:job:{job_id}")
            r.delete(f"nextstep:rtw:owner:{job_id}")
        except: pass

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
def health():
    r = get_redis(); ok = False
    try:
        if r: r.ping(); ok = True
    except: pass
    return {"ok": True, "redis": ok, "db": bool(os.getenv("DATABASE_URL"))}

@app.post("/rtw/extract")
async def rtw_extract(request: Request):
    _auth(request)
    form = await request.form()
    share_up = (form.get("sharecode_file") or form.get("sharecode_file1")
                or form.get("share_file") or form.get("share_file1"))
    dob_up   = (form.get("dob_file") or form.get("dob_file1")
                or form.get("dobfile") or form.get("dobfile1"))
    if not share_up or not dob_up:
        return JSONResponse(status_code=422, content={"error": "Missing files"})
    try:
        share_bytes = await share_up.read()
        dob_bytes   = await dob_up.read()
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
    tenant_id = ctx["tenant_id"]; user_id = ctx["user_id"]

    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try: data = await request.json()
        except: data = {}
    else:
        form = await request.form(); data = dict(form)

    share_code   = str(data.get("share_code","") or "").strip()
    dob_day      = str(data.get("dob_day","") or "").strip()
    dob_month    = str(data.get("dob_month","") or "").strip()
    dob_year     = str(data.get("dob_year","") or "").strip()
    company_name = str(data.get("company_name","") or "").strip()

    sc = normalize_share_code(share_code)
    if not sc:
        raise HTTPException(400, "Invalid share code (must be 9 alphanumeric characters).")
    try:
        d = int(dob_day); m = int(dob_month); y = int(dob_year)
    except:
        raise HTTPException(400, "DOB must be numeric Day/Month/Year.")
    if not (1 <= d <= 31 and 1 <= m <= 12 and 1900 <= y <= 2100):
        raise HTTPException(400, "DOB out of range.")
    if not company_name:
        raise HTTPException(400, "Company name is required.")

    # Token check
    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0:
            raise HTTPException(402, "No tokens remaining. Please contact NextStep to top up.")
    except HTTPException: raise
    except Exception as e: log.warning("[Run] Token check skipped: %s", e)

    job_id = str(uuid.uuid4())
    storage_path = _storage(tenant_id, user_id, job_id)

    # DB record
    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=1)
    except Exception as e: log.warning("[Run] DB record failed: %s", e)

    # Set initial state
    _jset(job_id, {"state": "running", "message": "Connecting to GOV.UK RTW portal…",
                   "pdf_url": "", "filename": "", "error": ""})
    _owner_set(job_id, tenant_id)

    log.info("[Run] RTW job %s tenant=%d share=%s", job_id, tenant_id, sc[:3]+"***")

    # Run with semaphore — 1 at a time
    try:
        from rtw_runner import run_rtw_check_and_download_pdf
    except Exception as e:
        raise HTTPException(500, f"rtw_runner import failed: {e}")

    async with _sem:
        _jset(job_id, {"state": "running", "message": "Running RTW check on GOV.UK…",
                       "pdf_url": "", "filename": "", "error": ""})
        try:
            result = await run_rtw_check_and_download_pdf(
                share_code=sc,
                dob_day=str(d),
                dob_month=str(m),
                dob_year=str(y),
                company_name=company_name,
                out_dir=storage_path,
            )
        except Exception as e:
            _jset(job_id, {"state": "failed", "message": f"RTW check failed: {e}",
                           "pdf_url": "", "filename": "", "error": str(e)})
            try:
                import db
                db.update_job_status(db_job_id=db_job_id, status="failed",
                                     successful_items=0, failed_items=1)
            except: pass
            raise HTTPException(500, f"RTW automation failed: {e}")

    # Handle result
    if not isinstance(result, dict) or not result.get("ok"):
        err = (result.get("error") or "Unknown error") if isinstance(result, dict) else "Unknown error"
        error_pdf = (result.get("error_pdf") or "") if isinstance(result, dict) else ""

        _jset(job_id, {"state": "failed", "message": err,
                       "pdf_url": "", "filename": "", "error": err})
        try:
            import db
            db.update_job_status(db_job_id=db_job_id, status="failed",
                                 successful_items=0, failed_items=1)
        except: pass

        # Return error PDF if available
        if error_pdf and Path(error_pdf).exists():
            _owner_set(job_id, tenant_id)
            err_name = Path(error_pdf).name
            _jset(job_id, {"state": "failed", "message": err,
                           "pdf_url": f"/rtw/download/{job_id}/{err_name}",
                           "filename": err_name, "error": err})
            bg = BackgroundTask(_cleanup, storage_path, job_id)
            return JSONResponse({
                "ok": False, "error": err,
                "job_id": job_id,
                "pdf_url": f"/rtw/download/{job_id}/{err_name}",
                "filename": err_name,
            }, background=bg)

        raise HTTPException(500, f"RTW failed: {err}")

    pdf_path = (result.get("pdf_path") or "").strip()
    if not pdf_path or not Path(pdf_path).exists():
        raise HTTPException(500, "RTW finished but PDF not found.")

    filename = Path(pdf_path).name
    pdf_url  = f"/rtw/download/{job_id}/{filename}"

    # Update state to done
    _jset(job_id, {"state": "done", "message": "Complete — your RTW PDF is ready.",
                   "pdf_url": pdf_url, "filename": filename, "error": ""})

    # DB update + token deduction
    try:
        import db
        db.update_job_status(db_job_id=db_job_id, status="completed",
                             successful_items=1, failed_items=0)
        db.record_usage(tenant_id=tenant_id, user_id=user_id,
                        db_job_id=db_job_id, successful_outputs=1)
    except Exception as e: log.warning("[Run] DB update failed: %s", e)

    log.info("[Run] RTW job %s complete: %s", job_id, filename)

    return JSONResponse({
        "ok": True,
        "job_id": job_id,
        "pdf_url": pdf_url,
        "filename": filename,
        "status_url": f"/rtw/status/{job_id}",
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
    ctx = _auth(request); tenant_id = ctx["tenant_id"]

    # Ownership check
    job_tenant = _owner_get(job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        log.warning("[DL] Tenant %d tried job owned by %d", tenant_id, job_tenant)
        raise HTTPException(403, "Access denied.")

    tenant_root = STORAGE_ROOT / str(tenant_id)
    file_path = None
    for p in tenant_root.rglob(name):
        if job_id in str(p): file_path = p; break

    if not file_path or not file_path.exists():
        raise HTTPException(404, "Download expired or not found.")

    sp = STORAGE_ROOT / str(tenant_id) / str(ctx["user_id"]) / job_id
    bg = BackgroundTask(_cleanup, sp, job_id)
    return FileResponse(str(file_path), filename=name,
                        media_type="application/pdf", background=bg)
