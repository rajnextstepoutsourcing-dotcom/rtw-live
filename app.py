import sys
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
import io
import re
import datetime
from typing import Dict, Any, Optional, Tuple, List

import anyio
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

import pymupdf as fitz
import pdfplumber

# Gemini (google-genai)
try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None
    types = None

APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.append(APP_DIR)
OUT_DIR = os.path.join(APP_DIR, "output")
os.makedirs(OUT_DIR, exist_ok=True)

app = FastAPI(title="RTW Check")
app.mount("/static", StaticFiles(directory=os.path.join(APP_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(APP_DIR, "templates"))

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return v

# --- Env (FAST -> STRONG fallback) ---
GEMINI_API_KEY = _env("GEMINI_API_KEY")
GEMINI_MODEL_FAST = _env("GEMINI_MODEL_FAST", "gemini-2.0-flash-001")
GEMINI_MODEL_STRONG = _env("GEMINI_MODEL_STRONG", "gemini-2.5-pro")

def get_gemini_client():
    if not GEMINI_API_KEY or genai is None or types is None:
        return None
    try:
        return genai.Client(api_key=GEMINI_API_KEY)
    except Exception:
        return None

GEMINI_CLIENT = get_gemini_client()

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

MONTHS = {
    # English full
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12,
    # English short
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "SEPT": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    # French short/common on passports
    "JANV": 1, "FEV": 2, "FÉV": 2, "MARS": 3, "AVR": 4, "MAI": 5, "JUIN": 6,
    "JUIL": 7, "AOUT": 8, "AOÛT": 8, "SEPT": 9, "OCT": 10, "NOV": 11, "DEC": 12, "DÉC": 12,
}

SHARE_CODE_RE = re.compile(r"\b([A-Z0-9]{3})\s*([A-Z0-9]{3})\s*([A-Z0-9]{3})\b")
SHARE_CODE_RAW_RE = re.compile(r"\b([A-Z0-9]{9})\b")

def _normalize_share_code(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s

def _format_share_code(raw9: str) -> str:
    raw9 = _normalize_share_code(raw9)
    if len(raw9) != 9:
        return ""
    return f"{raw9[0:3]} {raw9[3:6]} {raw9[6:9]}"

def _score_field(val: str) -> int:
    return 95 if (val or "").strip() else 0

def _two_digit_year_to_four(yy: int) -> int:
    # Rule: if YY > current YY -> 1900s else 2000s
    now = datetime.datetime.utcnow()
    cur = now.year % 100
    if yy > cur:
        return 1900 + yy
    return 2000 + yy

def _parse_dob_from_text(text: str) -> Optional[Tuple[str, str, str]]:
    if not text:
        return None
    t = text.replace("\u00a0", " ")
    up = t.upper()

    # 1) Try label-based line capture (DOB / Date of birth / Date de naissance)
    labels = ["DATE OF BIRTH", "DATE DE NAISSANCE", "DOB"]
    lines = [normalize_ws(l) for l in t.splitlines() if normalize_ws(l)]
    for i, line in enumerate(lines):
        uline = line.upper()
        for lab in labels:
            if lab in uline:
                # take this line after label or next line
                after = re.split(re.escape(lab), uline, maxsplit=1)
                cand = ""
                if len(after) == 2:
                    cand = after[1].strip(" :.-")
                if not cand and i + 1 < len(lines):
                    cand = lines[i + 1]
                parsed = _parse_dob_candidate(cand)
                if parsed:
                    return parsed

    # 2) Global pattern scan
    return _parse_dob_candidate(up)

def _parse_dob_candidate(s: str) -> Optional[Tuple[str, str, str]]:
    if not s:
        return None
    s = normalize_ws(s).upper()

    # Pattern A: "13 AUG / AOUT 92" or "22 JAN / JAN 96"
    m = re.search(r"\b(\d{1,2})\s+([A-ZÉÛÔÀÇ]{3,9})\s*/\s*([A-ZÉÛÔÀÇ]{3,9})\s+(\d{2,4})\b", s)
    if m:
        dd = int(m.group(1))
        mon = m.group(2)
        yy = m.group(4)
        mm = MONTHS.get(mon)
        if mm:
            year = int(yy)
            if year < 100:
                year = _two_digit_year_to_four(year)
            return (f"{dd:02d}", f"{mm:02d}", f"{year:04d}")

    # Pattern B: "08 AUGUST 1992" / "01 MARCH 1986"
    m = re.search(r"\b(\d{1,2})\s+([A-ZÉÛÔÀÇ]{3,12})\s+(\d{2,4})\b", s)
    if m:
        dd = int(m.group(1))
        mon = m.group(2)
        yy = int(m.group(3))
        mm = MONTHS.get(mon)
        if mm:
            year = yy
            if year < 100:
                year = _two_digit_year_to_four(year)
            return (f"{dd:02d}", f"{mm:02d}", f"{year:04d}")

    # Pattern C: numeric "03.06.1978" or "03/06/1978"
    m = re.search(r"\b(\d{1,2})[\./-](\d{1,2})[\./-](\d{2,4})\b", s)
    if m:
        dd = int(m.group(1))
        mm = int(m.group(2))
        yy = int(m.group(3))
        year = yy if yy >= 100 else _two_digit_year_to_four(yy)
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return (f"{dd:02d}", f"{mm:02d}", f"{year:04d}")

    return None

def extract_text_from_pdf(content: bytes, max_pages: int = 2) -> str:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            texts = []
            for page in pdf.pages[:max_pages]:
                t = page.extract_text() or ""
                if t.strip():
                    texts.append(t)
            return "\n".join(texts)
    except Exception:
        return ""

def pdf_to_images_bytes(content: bytes, max_pages: int = 1, dpi: int = 240) -> List[bytes]:
    images: List[bytes] = []
    doc = fitz.open(stream=content, filetype="pdf")
    try:
        for i in range(min(max_pages, doc.page_count)):
            page = doc.load_page(i)
            pix = page.get_pixmap(dpi=dpi)
            images.append(pix.tobytes("png"))
    finally:
        doc.close()
    return images

def _gemini_model_order() -> List[str]:
    # try fast then strong
    models = []
    if GEMINI_MODEL_FAST:
        models.append(GEMINI_MODEL_FAST)
    if GEMINI_MODEL_STRONG and GEMINI_MODEL_STRONG not in models:
        models.append(GEMINI_MODEL_STRONG)
    return models

def gemini_extract_share_code(images: List[Tuple[bytes, str]]) -> Dict[str, Any]:
    if GEMINI_CLIENT is None or types is None:
        return {}
    prompt = (
        "Extract the UK Right to Work share code from the attached document image(s).\n"
        "Return ONLY valid JSON with keys: share_code\n"
        "Rules: share_code must be 9 characters total (A-Z,0-9) excluding spaces. "
        "If not clearly visible, return empty string."
    )
    for model in _gemini_model_order():
        try:
            parts = [types.Part.from_text(prompt)]
            for b, mime in images:
                parts.append(types.Part.from_bytes(data=b, mime_type=mime))
            resp = GEMINI_CLIENT.models.generate_content(
                model=model,
                contents=[types.Content(role="user", parts=parts)],
            )
            txt = getattr(resp, "text", "") or ""
            data = _safe_json(txt)
            sc = _normalize_share_code(str((data or {}).get("share_code", "") or ""))
            if len(sc) == 9:
                return {"share_code": sc, "_model": model}
        except Exception:
            continue
    return {}

def gemini_extract_dob(images: List[Tuple[bytes, str]]) -> Dict[str, Any]:
    if GEMINI_CLIENT is None or types is None:
        return {}
    prompt = (
        "Extract the person's date of birth (DOB) from the attached passport/DBS document image(s).\n"
        "Return ONLY valid JSON with keys: dob_day, dob_month, dob_year\n"
        "Rules:\n"
        "- dob_day: 2 digits (01-31)\n"
        "- dob_month: 2 digits (01-12)\n"
        "- dob_year: 4 digits (e.g. 1996)\n"
        "- If the year is shown as 2 digits (e.g. 96), treat it as 1996 unless it clearly indicates otherwise.\n"
        "- Do NOT guess. If not clearly visible, return empty strings."
    )
    for model in _gemini_model_order():
        try:
            parts = [types.Part.from_text(prompt)]
            for b, mime in images:
                parts.append(types.Part.from_bytes(data=b, mime_type=mime))
            resp = GEMINI_CLIENT.models.generate_content(
                model=model,
                contents=[types.Content(role="user", parts=parts)],
            )
            txt = getattr(resp, "text", "") or ""
            data = _safe_json(txt) or {}
            dd = re.sub(r"\D", "", str(data.get("dob_day", "") or ""))
            mm = re.sub(r"\D", "", str(data.get("dob_month", "") or ""))
            yy = re.sub(r"\D", "", str(data.get("dob_year", "") or ""))
            if dd and mm and yy:
                if len(yy) == 2:
                    yy = str(_two_digit_year_to_four(int(yy)))
                if len(dd) <= 2 and len(mm) <= 2 and len(yy) == 4:
                    return {"dob_day": dd.zfill(2), "dob_month": mm.zfill(2), "dob_year": yy, "_model": model}
        except Exception:
            continue
    return {}

def _safe_json(text: str) -> Any:
    if not text:
        return None
    s = text.strip()
    # strip code fences
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    try:
        import json
        return json.loads(s)
    except Exception:
        # try to find first {...}
        m = re.search(r"\{.*\}", s, re.DOTALL)
        if not m:
            return None
        try:
            import json
            return json.loads(m.group(0))
        except Exception:
            return None

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/rtw/extract")
async def rtw_extract(
    share_file: UploadFile = File(...),
    dob_file: UploadFile = File(...),
):
    share_bytes = await share_file.read()
    dob_bytes = await dob_file.read()

    for content, label in [(share_bytes, "Share code document"), (dob_bytes, "DOB document")]:
        if len(content) > 25 * 1024 * 1024:
            raise HTTPException(status_code=413, detail=f"{label} is too large. Upload under 25MB.")

    resp: Dict[str, Any] = {
        "share_code": "",
        "dob_day": "",
        "dob_month": "",
        "dob_year": "",
        "confidence": {"share_code": 0, "dob": 0},
        "source": {"share_code": "", "dob": ""},
    }

    # --- Share code extraction ---
    sc = ""
    sc_src = ""
    share_name = (share_file.filename or "").lower()
    if share_name.endswith(".pdf"):
        txt = extract_text_from_pdf(share_bytes)
        if txt:
            m = SHARE_CODE_RE.search(txt.upper())
            if m:
                sc = _normalize_share_code("".join(m.groups()))
            else:
                m2 = SHARE_CODE_RAW_RE.search(_normalize_share_code(txt.upper()))
                if m2:
                    sc = _normalize_share_code(m2.group(1))
        if len(sc) == 9:
            sc_src = "PDF text"
        else:
            imgs = pdf_to_images_bytes(share_bytes, max_pages=1, dpi=240)
            images = [(b, "image/png") for b in imgs]
            vision = gemini_extract_share_code(images)
            sc = vision.get("share_code", "") or ""
            sc_src = f'AI (Gemini Vision: {vision.get("_model","")})'.strip() if sc else ""
    else:
        mime = "image/png" if share_bytes[:8] == b"\x89PNG\r\n\x1a\n" else "image/jpeg"
        vision = gemini_extract_share_code([(share_bytes, mime)])
        sc = vision.get("share_code", "") or ""
        sc_src = f'AI (Gemini Vision: {vision.get("_model","")})'.strip() if sc else ""

    if len(sc) == 9:
        resp["share_code"] = _format_share_code(sc)  # display-friendly
        resp["confidence"]["share_code"] = 95
        resp["source"]["share_code"] = sc_src
    else:
        resp["share_code"] = ""
        resp["confidence"]["share_code"] = 0
        resp["source"]["share_code"] = sc_src

    # --- DOB extraction ---
    dd = mm = yyyy = ""
    dob_src = ""
    dob_name = (dob_file.filename or "").lower()
    if dob_name.endswith(".pdf"):
        txt = extract_text_from_pdf(dob_bytes)
        parsed = _parse_dob_from_text(txt or "")
        if parsed:
            dd, mm, yyyy = parsed
            dob_src = "PDF text"
        if not (dd and mm and yyyy):
            imgs = pdf_to_images_bytes(dob_bytes, max_pages=1, dpi=240)
            images = [(b, "image/png") for b in imgs]
            vision = gemini_extract_dob(images)
            dd = vision.get("dob_day", "") or ""
            mm = vision.get("dob_month", "") or ""
            yyyy = vision.get("dob_year", "") or ""
            if dd and mm and yyyy:
                dob_src = f'AI (Gemini Vision: {vision.get("_model","")})'.strip()
    else:
        mime = "image/png" if dob_bytes[:8] == b"\x89PNG\r\n\x1a\n" else "image/jpeg"
        vision = gemini_extract_dob([(dob_bytes, mime)])
        dd = vision.get("dob_day", "") or ""
        mm = vision.get("dob_month", "") or ""
        yyyy = vision.get("dob_year", "") or ""
        if dd and mm and yyyy:
            dob_src = f'AI (Gemini Vision: {vision.get("_model","")})'.strip()

    resp["dob_day"] = dd
    resp["dob_month"] = mm
    resp["dob_year"] = yyyy
    resp["source"]["dob"] = dob_src
    resp["confidence"]["dob"] = 95 if (dd and mm and yyyy) else 0

    return JSONResponse(resp)

# -------------------------
# Run RTW (Playwright) -> PDF
# -------------------------
from rtw_runner import run_rtw_check_and_download_pdf

@app.post("/rtw/run")
async def rtw_run(request: Request):
    payload: Dict[str, Any] = {}
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
    else:
        form = await request.form()
        payload = dict(form)

    company_name = (payload.get("company_name") or payload.get("company") or "").strip()
    share_code = (payload.get("share_code") or "").strip()
    dob_day = (payload.get("dob_day") or "").strip()
    dob_month = (payload.get("dob_month") or "").strip()
    dob_year = (payload.get("dob_year") or "").strip()

    raw_sc = _normalize_share_code(share_code)
    if len(raw_sc) != 9:
        raise HTTPException(status_code=400, detail="Share code must be 9 characters (letters/numbers), spaces optional.")

    if not (company_name and dob_day and dob_month and dob_year):
        raise HTTPException(status_code=400, detail="Company name and full DOB (day/month/year) are required.")

    out_dir = os.path.join(OUT_DIR, datetime.datetime.utcnow().strftime("rtw-%Y%m%d"))
    os.makedirs(out_dir, exist_ok=True)

    result = await anyio.to_thread.run_sync(
        lambda: run_rtw_check_and_download_pdf(
            share_code=raw_sc,
            dob_day=dob_day,
            dob_month=dob_month,
            dob_year=dob_year,
            company_name=company_name,
            out_dir=Path(out_dir),
        )
    )

    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error") or "RTW runner failed.")

    pdf_path = result.get("pdf_path")
    if not pdf_path or not os.path.exists(pdf_path):
        raise HTTPException(status_code=500, detail="Runner succeeded but PDF was not found on disk.")

    filename = result.get("filename") or os.path.basename(pdf_path)
    return FileResponse(path=pdf_path, filename=filename, media_type="application/pdf")
