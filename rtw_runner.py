import re

import time
from pathlib import Path
from typing import Dict, Any, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Response

RTW_START_URL = "https://www.gov.uk/view-right-to-work"


def _tag() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _s(x: Any) -> str:
    return ("" if x is None else str(x)).strip()


def _goto_with_retry(page, url: str, tries: int = 3, timeout: int = 60000) -> Optional[Response]:
    last_resp: Optional[Response] = None
    last_err: Optional[Exception] = None
    for i in range(tries):
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            last_resp = resp
            if resp is None or resp.status < 400:
                return resp
        except Exception as e:
            last_err = e
        page.wait_for_timeout(900 + i * 700)

    if last_resp is not None:
        raise RuntimeError(f"Failed to load {url} (HTTP {last_resp.status})")
    if last_err is not None:
        raise RuntimeError(f"Failed to load {url}: {last_err}")
    raise RuntimeError(f"Failed to load {url}")


def _click_continue(page, timeout: int = 20000) -> None:
    for sel in [
        "button:has-text('Continue')",
        "input[type='submit'][value='Continue']",
        "a.govuk-button:has-text('Continue')",
    ]:
        loc = page.locator(sel).first
        try:
            if loc.count():
                loc.wait_for(state="visible", timeout=timeout)
                loc.click()
                return
        except Exception:
            continue
    raise RuntimeError("Continue button not found")


def _click_start_now(page, timeout: int = 20000) -> None:
    for sel in [
        "a.govuk-button:has-text('Start now')",
        "a:has-text('Start now')",
        "button:has-text('Start now')",
    ]:
        loc = page.locator(sel).first
        try:
            if loc.count():
                loc.wait_for(state="visible", timeout=timeout)
                loc.click()
                return
        except Exception:
            continue
    raise RuntimeError("Start now button not found")


def _fill_share_code(page, share_code: str) -> None:
    # try common GOV.UK field ids/names
    candidates = [
        "input#shareCode",
        "input[name='shareCode']",
        "input[name='share_code']",
        "input#share-code",
        "input[name='share-code']",
        "input[type='text']",
    ]
    for sel in candidates:
        loc = page.locator(sel).first
        try:
            if loc.count():
                loc.fill(share_code)
                return
        except Exception:
            continue
    raise RuntimeError("Share code input not found")


def _fill_dob(page, dd: str, mm: str, yyyy: str) -> None:
    # Prefer labels (most reliable)
    try:
        page.get_by_label("Day").fill(dd)
        page.get_by_label("Month").fill(mm)
        page.get_by_label("Year").fill(yyyy)
        return
    except Exception:
        pass

    # Common ids/names in GOV.UK date inputs
    triplets = [
        ("#dateOfBirth-day", "#dateOfBirth-month", "#dateOfBirth-year"),
        ("input[name='dateOfBirth-day']", "input[name='dateOfBirth-month']", "input[name='dateOfBirth-year']"),
        ("#dob-day", "#dob-month", "#dob-year"),
    ]
    for a, b, c in triplets:
        try:
            if page.locator(a).count() and page.locator(b).count() and page.locator(c).count():
                page.fill(a, dd)
                page.fill(b, mm)
                page.fill(c, yyyy)
                return
        except Exception:
            continue

    # Last resort: first three text inputs
    inputs = page.locator("input[type='text']")
    if inputs.count() >= 3:
        inputs.nth(0).fill(dd)
        inputs.nth(1).fill(mm)
        inputs.nth(2).fill(yyyy)
        return

    raise RuntimeError("DOB inputs not found")


def _fill_company(page, company_name: str) -> None:
    try:
        page.get_by_label("Company name").fill(company_name)
        return
    except Exception:
        pass
    for sel in ["input#companyName", "input[name='companyName']", "input[name='company_name']", "input[type='text']"]:
        loc = page.locator(sel).first
        try:
            if loc.count():
                loc.fill(company_name)
                return
        except Exception:
            continue
    raise RuntimeError("Company name input not found")


def run_rtw_check_and_download_pdf(
    *,
    share_code: str,
    dob_day: str,
    dob_month: str,
    dob_year: str,
    company_name: str,
    out_dir: Path,
) -> Dict[str, Any]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tag = _tag()
    trace_path = out_dir / f"rtw-{tag}.trace.zip"
    error_png = out_dir / f"rtw-{tag}.error.png"
    pdf_path = out_dir / f"RTW-Check-{tag}.pdf"

    sc = _s(share_code)
    dd = _s(dob_day).zfill(2)
    mm = _s(dob_month).zfill(2)
    yyyy = _s(dob_year)
    comp = _s(company_name)

    if len(re.sub(r"[^A-Z0-9]", "", sc.upper())) != 9:
        return {"ok": False, "error": "Invalid share code (must be 9 characters).", "trace_path": str(trace_path)}
    if not (dd and mm and yyyy and comp):
        return {"ok": False, "error": "Missing DOB or company name.", "trace_path": str(trace_path)}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="en-GB",
            timezone_id="Europe/London",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
        )
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        context.tracing.start(screenshots=True, snapshots=True, sources=True)

        page = context.new_page()

        try:
            _goto_with_retry(page, RTW_START_URL, tries=3, timeout=60000)
            page.wait_for_timeout(600)

            _click_start_now(page)
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(600)

            # Share code page
            _fill_share_code(page, sc)
            _click_continue(page)
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(500)

            # DOB page
            _fill_dob(page, dd, mm, yyyy)
            _click_continue(page)
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(500)

            # Company name page
            _fill_company(page, comp)
            _click_continue(page)
            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(1200)

            # Final page: download PDF
            dl = None
            try:
                with page.expect_download(timeout=60000) as dlinfo:
                    # Link text is "Download PDF"
                    page.locator("a:has-text('Download PDF'), a:has-text('Download PDF')").first.click()
                dl = dlinfo.value
            except Exception:
                # alternative: try clicking by role
                with page.expect_download(timeout=60000) as dlinfo:
                    page.get_by_role("link", name="Download PDF").click()
                dl = dlinfo.value

            if dl is None:
                raise RuntimeError("Download did not start")

            dl.save_as(str(pdf_path))

            context.tracing.stop(path=str(trace_path))
            context.close()
            browser.close()

            return {
                "ok": True,
                "pdf_path": str(pdf_path),
                "filename": pdf_path.name,
                "trace_path": str(trace_path),
            }

        except Exception as e:
            try:
                page.screenshot(path=str(error_png), full_page=True)
            except Exception:
                pass
            try:
                context.tracing.stop(path=str(trace_path))
            except Exception:
                pass
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

            return {
                "ok": False,
                "error": str(e),
                "error_png": str(error_png),
                "trace_path": str(trace_path),
            }
