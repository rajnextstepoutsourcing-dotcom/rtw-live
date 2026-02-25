import uuid
import shutil
import tempfile
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from rtw_extract import extract_rtw_fields, normalize_share_code


# ----------------------------
# Paths / App
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR / "data"
DATA_ROOT.mkdir(parents=True, exist_ok=True)

app = FastAPI()

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def new_job_dir(prefix: str = "rtw") -> Path:
    job_id = f"{prefix}_{uuid.uuid4().hex}"
    job_dir = DATA_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


def save_upload_to_temp(upload) -> Path:
    """
    Save an UploadFile-like object (Starlette/FastAPI UploadFile) to a temp path.
    """
    suffix = Path(getattr(upload, "filename", "") or "").suffix.lower()
    tmp = Path(tempfile.mkstemp(suffix=suffix)[1])
    with tmp.open("wb") as f:
        shutil.copyfileobj(upload.file, f)
    return tmp


def read_file_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except Exception:
        return b""


# ----------------------------
# Pages / Health
# ----------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
def health():
    return {"ok": True}


# ----------------------------
# RTW: Extract (Flexible form field names)
# ----------------------------
@app.post("/rtw/extract")
async def rtw_extract(request: Request):
    """
    Accepts multipart/form-data with two files.
    Frontend field names have varied historically, so we accept multiple aliases
    to avoid 422 errors:

    Share doc: sharecode_file / sharecode_file1 / share_file / share_file1
    DOB doc:   dob_file / dob_file1 / dobfile / dobfile1
    """
    form = await request.form()

    share_up = (
        form.get("sharecode_file")
        or form.get("sharecode_file1")
        or form.get("share_file")
        or form.get("share_file1")
    )
    dob_up = (
        form.get("dob_file")
        or form.get("dob_file1")
        or form.get("dobfile")
        or form.get("dobfile1")
    )

    if not share_up or not dob_up:
        return JSONResponse(
            status_code=422,
            content={
                "error": "Missing files",
                "expected_any_of": {
                    "share": ["sharecode_file", "sharecode_file1", "share_file", "share_file1"],
                    "dob": ["dob_file", "dob_file1", "dobfile", "dobfile1"],
                },
            },
        )

    # Read bytes directly from uploaded files (no OCR/system deps)
    try:
        share_bytes = await share_up.read()
        dob_bytes = await dob_up.read()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Failed to read uploaded files: {e}"})

    result = extract_rtw_fields(
        share_file_bytes=share_bytes,
        dob_file_bytes=dob_bytes,
        share_filename=getattr(share_up, "filename", "share"),
        dob_filename=getattr(dob_up, "filename", "dob"),
    )
    return result


# ----------------------------
# RTW: Run automation (Playwright)
# ----------------------------
@app.post("/rtw/run")
async def rtw_run(request: Request):
    """
    Run RTW automation.

    Accepts either:
    - multipart/form-data (recommended)
    - application/json (supported for backward compatibility)
    """
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

    sc = normalize_share_code(share_code)
    if not sc:
        raise HTTPException(status_code=400, detail="Invalid share code (must be 9 alphanumeric characters)")

    try:
        d = int(dob_day)
        m = int(dob_month)
        y = int(dob_year)
    except Exception:
        raise HTTPException(status_code=400, detail="DOB must be numeric Day/Month/Year")

    if not (1 <= d <= 31 and 1 <= m <= 12 and 1900 <= y <= 2100):
        raise HTTPException(status_code=400, detail="DOB out of range")

    if not company_name:
        raise HTTPException(status_code=400, detail="Company name is required")

    try:
        from rtw_runner import run_rtw_check_and_download_pdf as run_rtw_check
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"rtw_runner missing or import failed: {e}")

    job_dir = new_job_dir("rtw")

    try:
        result = run_rtw_check(
            share_code=sc,
            dob_day=str(d),
            dob_month=str(m),
            dob_year=str(y),
            company_name=company_name,
            out_dir=job_dir,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"RTW automation failed: {e}")

    if not isinstance(result, dict) or not result.get("ok"):
        err = (result or {}).get("error") if isinstance(result, dict) else None
        raise HTTPException(status_code=500, detail=f"RTW automation failed: {err or 'Unknown error'}")

    pdf_path = (result.get("pdf_path") or "").strip()
    if not pdf_path or not Path(pdf_path).exists():
        raise HTTPException(status_code=500, detail="RTW automation finished but PDF not found")

    return FileResponse(
        path=pdf_path,
        media_type="application/pdf",
        filename=result.get("filename") or "RTW_Result.pdf",
    )
