import os
import re
import json
import io
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

import pdfplumber
import fitz  # pymupdfb exposes fitz
from dateutil import parser as dateparser

# Gemini (AI fallback, FAST -> STRONG)
try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None
    types = None


# ----------------------------
# Parsing helpers
# ----------------------------
MONTH_MAP = {
    # English
    "JAN": 1, "JANUARY": 1,
    "FEB": 2, "FEBRUARY": 2,
    "MAR": 3, "MARCH": 3,
    "APR": 4, "APRIL": 4,
    "MAY": 5,
    "JUN": 6, "JUNE": 6,
    "JUL": 7, "JULY": 7,
    "AUG": 8, "AUGUST": 8,
    "SEP": 9, "SEPT": 9, "SEPTEMBER": 9,
    "OCT": 10, "OCTOBER": 10,
    "NOV": 11, "NOVEMBER": 11,
    "DEC": 12, "DECEMBER": 12,
    # French (common on passports)
    "JANVIER": 1,
    "FEV": 2, "FÉV": 2, "FEVRIER": 2, "FÉVRIER": 2,
    "MARS": 3,
    "AVR": 4, "AVRIL": 4,
    "MAI": 5,
    "JUIN": 6,
    "JUIL": 7, "JUILLET": 7,
    "AOUT": 8, "AOÛT": 8,
    "SEPTEMBRE": 9,
    "OCTOBRE": 10,
    "NOVEMBRE": 11,
    "DECEMBRE": 12, "DÉCEMBRE": 12,
}

SHARECODE_RE = re.compile(r"\b([A-Z0-9]{3})\s*([A-Z0-9]{3})\s*([A-Z0-9]{3})\b", re.IGNORECASE)
DOB_WORD_RE = re.compile(r"\b(\d{1,2})\s+([A-ZÀ-ÿ]{3,10})\s+(\d{2,4})\b", re.IGNORECASE)
DOB_DOT_RE = re.compile(r"\b(\d{2})\.(\d{2})\.(\d{4})\b")
DOB_SLASH_RE = re.compile(r"\b(\d{2})/(\d{2})/(\d{4})\b")


def _safe_upper(s: str) -> str:
    return (s or "").strip().upper()


def normalize_share_code(candidate: str) -> Optional[str]:
    """Return raw 9-char share code without spaces, or None if invalid."""
    if not candidate:
        return None
    s = re.sub(r"[^A-Z0-9]", "", candidate.upper())
    if len(s) != 9:
        return None
    if not re.fullmatch(r"[A-Z0-9]{9}", s):
        return None
    return s


def format_share_code_spaced(raw9: str) -> str:
    raw9 = normalize_share_code(raw9) or raw9
    if not raw9 or len(raw9) < 9:
        return raw9 or ""
    return f"{raw9[0:3]} {raw9[3:6]} {raw9[6:9]}"


def normalize_year_2digit(y: int) -> int:
    """Map 2-digit years to a realistic 4-digit year."""
    if y < 100:
        from datetime import datetime
        yy = int(datetime.utcnow().strftime("%y"))
        # If y is greater than current YY, it's likely 1900s, else 2000s
        if y > yy:
            return 1900 + y
        return 2000 + y
    return y


def parse_dob_string(date_str: str) -> Tuple[str, str, str]:
    """Parse a date string into (dd, mm, yyyy) strings."""
    s = (date_str or "").strip()
    if not s:
        return "", "", ""
    try:
        dt = dateparser.parse(s, dayfirst=True, fuzzy=True)
    except Exception:
        dt = None
    if not dt:
        return "", "", ""
    y = dt.year
    if y < 100:
        y = 2000 + y if y <= 30 else 1900 + y
    if not (1900 <= y <= 2100):
        return "", "", ""
    return f"{dt.day:02d}", f"{dt.month:02d}", f"{y:04d}"


def parse_dob_from_text(text: str) -> Tuple[Optional[int], Optional[int], Optional[int], float, str]:
    """Return (day, month, year, confidence, reason)."""
    t = _safe_upper(text)

    label_positions: List[int] = []
    for kw in ["DATE OF BIRTH", "DATE DE NAISSANCE", "DOB"]:
        idx = t.find(kw)
        if idx != -1:
            label_positions.append(idx)

    def score_pos(pos: int) -> float:
        if not label_positions:
            return 0.5
        d = min(abs(pos - lp) for lp in label_positions)
        if d < 80:
            return 0.95
        if d < 200:
            return 0.85
        return 0.65

    best = (None, None, None, 0.0, "no_match")

    for m in DOB_DOT_RE.finditer(t):
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= d <= 31 and 1 <= mo <= 12:
            conf = score_pos(m.start())
            if conf > best[3]:
                best = (d, mo, y, conf, "dot_date")

    for m in DOB_SLASH_RE.finditer(t):
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= d <= 31 and 1 <= mo <= 12:
            conf = score_pos(m.start())
            if conf > best[3]:
                best = (d, mo, y, conf, "slash_date")

    for m in DOB_WORD_RE.finditer(t):
        d = int(m.group(1))
        mon_raw = _safe_upper(m.group(2))
        y = int(m.group(3))
        mon = MONTH_MAP.get(mon_raw)
        if mon and 1 <= d <= 31:
            y_norm = normalize_year_2digit(y)
            conf = score_pos(m.start())
            if conf > best[3]:
                best = (d, mon, y_norm, conf, f"word_month:{mon_raw}")

    if best[0] and best[2]:
        if best[2] < 1900 or best[2] > 2100:
            return (None, None, None, 0.0, "year_out_of_range")

    return best


def extract_share_code_from_text(text: str) -> Tuple[Optional[str], float, str]:
    """Return (raw9, confidence, reason)."""
    t = _safe_upper(text)

    candidates = []
    for m in SHARECODE_RE.finditer(t):
        raw = (m.group(1) + m.group(2) + m.group(3)).upper()
        raw = normalize_share_code(raw)
        if raw:
            candidates.append((raw, m.start()))

    if not candidates:
        for m in re.finditer(r"\b[A-Z0-9]{9}\b", t):
            raw = normalize_share_code(m.group(0))
            if raw:
                candidates.append((raw, m.start()))

    if not candidates:
        return (None, 0.0, "no_sharecode_match")

    label_idx = t.find("SHARE CODE")
    best = (None, 0.6, "sharecode_match")
    for raw, pos in candidates:
        conf = 0.75
        if label_idx != -1:
            d = abs(pos - label_idx)
            if d < 80:
                conf = 0.95
            elif d < 200:
                conf = 0.85
            else:
                conf = 0.75
        if conf > best[1]:
            best = (raw, conf, "near_share_code_label")
    return best


# ----------------------------
# PDF / Vision helpers
# ----------------------------
VISION_PROMPT = """You are extracting UK Right to Work details from uploaded documents/images.

Return ONLY strict JSON with keys:
{
  "share_code_raw9": "XXXXXXXXX",
  "dob": "DD/MM/YYYY"
}

Rules:
- SHARE CODE is 9 alphanumeric characters (A-Z0-9). Ignore spaces/dashes.
- DOB must be a real date. If the year is shown as 2 digits, infer a realistic 4-digit year (e.g., 96 -> 1996).
- If multiple candidates exist, choose the most likely.
"""


def _is_pdf_bytes(b: bytes) -> bool:
    return bool(b and b.lstrip().startswith(b"%PDF"))


def extract_text_layer_from_pdf_bytes(pdf_bytes: bytes, max_pages: int = 2) -> str:
    if not pdf_bytes:
        return ""
    parts: List[str] = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:max_pages]:
                try:
                    t = page.extract_text() or ""
                    if t.strip():
                        parts.append(t)
                except Exception:
                    continue
    except Exception:
        return ""
    return "\n".join(parts).strip()


def pdf_bytes_to_vision_images(pdf_bytes: bytes, dpi: int = 240) -> List[bytes]:
    images: List[bytes] = []
    if not pdf_bytes or not _is_pdf_bytes(pdf_bytes):
        return images

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return images

    try:
        if getattr(doc, "is_encrypted", False):
            try:
                doc.authenticate("")
            except Exception:
                pass
            if getattr(doc, "is_encrypted", False):
                return images

        if getattr(doc, "page_count", 0) <= 0:
            return images

        page = doc.load_page(0)
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        rect = page.rect

        bands = [
            (0.0, 0.0, 1.0, 0.35),
            (0.0, 0.30, 1.0, 0.75),
            (0.0, 0.70, 1.0, 1.0),
        ]

        for x0, y0, x1, y1 in bands:
            clip = fitz.Rect(
                rect.x0 + rect.width * x0,
                rect.y0 + rect.height * y0,
                rect.x0 + rect.width * x1,
                rect.y0 + rect.height * y1,
            )
            pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
            images.append(pix.tobytes("png"))

        pix_full = page.get_pixmap(matrix=mat, alpha=False)
        images.append(pix_full.tobytes("png"))
        return images
    finally:
        try:
            doc.close()
        except Exception:
            pass


def file_bytes_to_vision_images(file_bytes: bytes, filename: str = "") -> List[Tuple[bytes, str]]:
    if not file_bytes:
        return []
    if _is_pdf_bytes(file_bytes):
        imgs = pdf_bytes_to_vision_images(file_bytes)
        return [(b, "image/png") for b in imgs if b]

    ext = (Path(filename).suffix or "").lower()
    mime = "image/png"
    if ext in [".jpg", ".jpeg"]:
        mime = "image/jpeg"
    elif ext == ".webp":
        mime = "image/webp"
    return [(file_bytes, mime)]


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


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


def _parse_json_response(txt: str) -> Dict[str, Any]:
    if not txt:
        return {}
    t = txt.strip()
    t = re.sub(r"^```json\s*", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"^```\s*", "", t).strip()
    t = re.sub(r"\s*```$", "", t).strip()
    m = re.search(r"\{[\s\S]*\}", t)
    if m:
        t = m.group(0)
    try:
        data = json.loads(t)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def gemini_vision_extract(share_images: List[Tuple[bytes, str]], dob_images: List[Tuple[bytes, str]]) -> Dict[str, Any]:
    if GEMINI_CLIENT is None:
        raise RuntimeError("Gemini client not available (check GEMINI_API_KEY and google-genai install)")

    def _call(model_name: str) -> Dict[str, Any]:
        parts = [types.Part.from_text(text=VISION_PROMPT)]
        if share_images:
            parts.append(types.Part.from_text(text="SHARE CODE DOCUMENT IMAGES:"))
            for b, mime in share_images:
                parts.append(types.Part.from_bytes(data=b, mime_type=mime))
        if dob_images:
            parts.append(types.Part.from_text(text="DOB DOCUMENT IMAGES:"))
            for b, mime in dob_images:
                parts.append(types.Part.from_bytes(data=b, mime_type=mime))

        resp = GEMINI_CLIENT.models.generate_content(
            model=model_name,
            contents=[types.Content(role="user", parts=parts)],
        )
        txt = getattr(resp, "text", None) or ""
        data = _parse_json_response(txt)
        if isinstance(data, dict):
            data["_model"] = model_name
        return data if isinstance(data, dict) else {}

    data: Dict[str, Any] = {}
    try:
        data = _call(GEMINI_MODEL_FAST)
    except Exception:
        data = {}

    def _missing(d: Dict[str, Any]) -> bool:
        if not d:
            return True
        sc = normalize_share_code(str(d.get("share_code_raw9", "") or ""))
        dob = str(d.get("dob", "") or "").strip()
        dd, mm, yy = parse_dob_string(dob) if dob else ("", "", "")
        return not (sc and dd and mm and yy)

    if _missing(data):
        try:
            data = _call(GEMINI_MODEL_STRONG)
        except Exception:
            pass

    sc = normalize_share_code(str(data.get("share_code_raw9", "") or "")) or ""
    dd, mm, yy = ("", "", "")
    dob = str(data.get("dob", "") or "").strip()
    if dob:
        dd, mm, yy = parse_dob_string(dob)

    return {
        "share_code_raw9": sc,
        "dob_day": dd,
        "dob_month": mm,
        "dob_year": yy,
        "notes": f"model={data.get('_model','')}".strip(),
    }


def extract_rtw_fields(
    share_file_bytes: bytes,
    dob_file_bytes: bytes,
    share_filename: str = "share",
    dob_filename: str = "dob",
    conf_threshold: float = 0.80,
) -> Dict[str, Any]:
    """
    Unified extractor:
    - Try PDF text-layer parsing first
    - If missing/low-confidence -> Gemini Vision FAST -> STRONG
    """
    text_share = extract_text_layer_from_pdf_bytes(share_file_bytes, max_pages=2) if _is_pdf_bytes(share_file_bytes) else ""
    text_dob = extract_text_layer_from_pdf_bytes(dob_file_bytes, max_pages=2) if _is_pdf_bytes(dob_file_bytes) else ""

    sc_raw, sc_conf, sc_reason = extract_share_code_from_text(text_share or "")
    d, m, y, dob_conf, dob_reason = parse_dob_from_text(text_dob or "")

    need_ai = False
    if not sc_raw or sc_conf < conf_threshold:
        need_ai = True
    if not (d and m and y) or dob_conf < conf_threshold:
        need_ai = True

    ai_used = False
    ai_notes = ""

    if need_ai:
        try:
            share_imgs = file_bytes_to_vision_images(share_file_bytes, filename=share_filename)
            dob_imgs = file_bytes_to_vision_images(dob_file_bytes, filename=dob_filename)

            ai = gemini_vision_extract(share_imgs, dob_imgs)

            if ai.get("share_code_raw9"):
                sc_raw = ai["share_code_raw9"]
                sc_conf = max(sc_conf, 0.90)
                sc_reason = sc_reason + "|ai"

            if ai.get("dob_day") and ai.get("dob_month") and ai.get("dob_year"):
                d, m, y = int(ai["dob_day"]), int(ai["dob_month"]), int(ai["dob_year"])
                dob_conf = max(dob_conf, 0.90)
                dob_reason = dob_reason + "|ai"

            ai_used = True
            ai_notes = ai.get("notes", "") or ""
        except Exception as e:
            ai_used = False
            ai_notes = f"AI fallback failed: {str(e)}"

    sc_display = format_share_code_spaced(sc_raw) if sc_raw else ""

    return {
        "ok": True,
        "share_code_raw9": sc_raw or "",
        "share_code_display": sc_display or "",
        "dob_day": f"{int(d):02d}" if d else "",
        "dob_month": f"{int(m):02d}" if m else "",
        "dob_year": f"{int(y):04d}" if y else "",
        "confidence": {
            "share_code": sc_conf,
            "dob": dob_conf,
            "share_reason": sc_reason,
            "dob_reason": dob_reason,
            "text_layer_used": bool((text_share or "").strip() or (text_dob or "").strip()),
        },
        "ai_used": ai_used,
        "ai_notes": ai_notes,
    }
