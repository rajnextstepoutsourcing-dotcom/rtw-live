import re
import time
from pathlib import Path
from typing import Dict, Any, Optional

from playwright.async_api import async_playwright, Response

RTW_START_URL = "https://www.gov.uk/view-right-to-work"


def _tag() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _s(x: Any) -> str:
    return ("" if x is None else str(x)).strip()


async def _page_text(page) -> str:
    try:
        return (await page.inner_text("body")).strip()
    except Exception:
        try:
            return (await page.content())[:5000]
        except Exception:
            return ""


async def _detect_rtw_error_message(page) -> Optional[str]:
    """
    Detect common GOV.UK RTW validation failures like expired share code / details mismatch.
    Returns a short error message if found.
    """
    txt = (await _page_text(page)).lower()

    needles = [
        ("expired", "Share code appears to be expired."),
        ("has expired", "Share code appears to be expired."),
        ("does not match", "Details do not match the share code."),
        ("do not match", "Details do not match the share code."),
        ("could not find", "Could not find right to work details for these details."),
        ("cannot view", "Cannot view right to work details for these details."),
        ("there is a problem", "There is a problem with the details provided."),
        ("error summary", "There is a problem with the details provided."),
    ]
    for n, msg in needles:
        if n in txt:
            # ensure it's actually an error page by checking error summary component or heading
            try:
                if await page.locator(".govuk-error-summary").count():
                    return msg
            except Exception:
                pass
            # fallback: if the page contains strong error signals
            if "govuk-error-summary" in txt or "there is a problem" in txt:
                return msg

    return None


async def _try_save_error_pdf(page, path: Path) -> bool:
    """
    Try to save the current page as a PDF (works in Chromium).
    """
    try:
        await page.emulate_media(media="screen")
    except Exception:
        pass
    try:
        await page.pdf(path=str(path), format="A4", print_background=True)
        return True
    except Exception:
        return False


async def _goto_with_retry(page, url: str, tries: int = 3, timeout: int = 60000) -> Optional[Response]:
    last_resp: Optional[Response] = None
    last_err: Optional[Exception] = None
    for i in range(tries):
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            last_resp = resp
            if resp is None or resp.status < 400:
                return resp
        except Exception as e:
            last_err = e
        await page.wait_for_timeout(900 + i * 700)

    if last_resp is not None:
        raise RuntimeError(f"Failed to load {url} (HTTP {last_resp.status})")
    if last_err is not None:
        raise RuntimeError(f"Failed to load {url}: {last_err}")
    raise RuntimeError(f"Failed to load {url}")


async def _click_continue(page, timeout: int = 20000) -> None:
    for sel in [
        "button:has-text('Continue')",
        "input[type='submit'][value='Continue']",
        "a.govuk-button:has-text('Continue')",
    ]:
        loc = page.locator(sel).first
        try:
            if await loc.count():
                await loc.wait_for(state="visible", timeout=timeout)
                await loc.click()
                return
        except Exception:
            continue
    raise RuntimeError("Continue button not found")


async def _click_start_now(page, timeout: int = 20000) -> None:
    for sel in [
        "a.govuk-button:has-text('Start now')",
        "a:has-text('Start now')",
        "button:has-text('Start now')",
    ]:
        loc = page.locator(sel).first
        try:
            if await loc.count():
                await loc.wait_for(state="visible", timeout=timeout)
                await loc.click()
                return
        except Exception:
            continue
    raise RuntimeError("Start now button not found")


async def _fill_share_code(page, share_code: str) -> None:
    candidates = [
        "input#shareCode",
        "input[name='shareCode']",
        "input[name='share_code']",
        "input#share-code",
        "input[name='share-code']",
    ]
    for sel in candidates:
        loc = page.locator(sel).first
        try:
            if await loc.count():
                await loc.fill(share_code)
                return
        except Exception:
            continue

    # last resort: first visible text input
    loc = page.locator("input[type='text']").first
    if await loc.count():
        await loc.fill(share_code)
        return

    raise RuntimeError("Share code input not found")


async def _fill_dob(page, dd: str, mm: str, yyyy: str) -> None:
    # best: GOV.UK labels
    try:
        await page.get_by_label("Day").fill(dd)
        await page.get_by_label("Month").fill(mm)
        await page.get_by_label("Year").fill(yyyy)
        return
    except Exception:
        pass

    # common id/name triplets
    triplets = [
        ("#dateOfBirth-day", "#dateOfBirth-month", "#dateOfBirth-year"),
        ("input[name='dateOfBirth-day']", "input[name='dateOfBirth-month']", "input[name='dateOfBirth-year']"),
        ("#dob-day", "#dob-month", "#dob-year"),
        ("input[name='dob_day']", "input[name='dob_month']", "input[name='dob_year']"),
    ]
    for a, b, c in triplets:
        try:
            if (await page.locator(a).count()) and (await page.locator(b).count()) and (await page.locator(c).count()):
                await page.fill(a, dd)
                await page.fill(b, mm)
                await page.fill(c, yyyy)
                return
        except Exception:
            continue

    # fallback: first 3 visible text inputs
    inputs = page.locator("input[type='text']")
    if await inputs.count() >= 3:
        try:
            await inputs.nth(0).fill(dd)
            await inputs.nth(1).fill(mm)
            await inputs.nth(2).fill(yyyy)
            return
        except Exception:
            pass

    raise RuntimeError("DOB inputs not found")


async def _fill_company(page, company_name: str) -> None:
    try:
        await page.get_by_label("Company name").fill(company_name)
        return
    except Exception:
        pass

    for sel in ["input#companyName", "input[name='companyName']", "input[name='company_name']"]:
        loc = page.locator(sel).first
        try:
            if await loc.count():
                await loc.fill(company_name)
                return
        except Exception:
            continue

    # fallback: first visible text input
    loc = page.locator("input[type='text']").first
    if await loc.count():
        await loc.fill(company_name)
        return

    raise RuntimeError("Company name input not found")


async def run_rtw_check_and_download_pdf(
    *,
    share_code: str,
    dob_day: str,
    dob_month: str,
    dob_year: str,
    company_name: str,
    out_dir: Path,
) -> Dict[str, Any]:
    """Async Playwright runner (FastAPI/Render-safe)."""

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tag = _tag()
    trace_path = out_dir / f"rtw-{tag}.trace.zip"
    error_png = out_dir / f"rtw-{tag}.error.png"
    error_pdf = out_dir / f"RTW-Error-{tag}.pdf"
    pdf_path = out_dir / f"RTW-Check-{tag}.pdf"

    sc = _s(share_code)
    dd = _s(dob_day).zfill(2)
    mm = _s(dob_month).zfill(2)
    yyyy = _s(dob_year)
    comp = _s(company_name)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
                "--disable-setuid-sandbox",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="en-GB",
            timezone_id="Europe/London",
            extra_http_headers={"Accept-Language": "en-GB,en;q=0.9"},
            accept_downloads=True,
        )
        await context.tracing.start(screenshots=True, snapshots=True, sources=True)
        page = await context.new_page()

        try:
            await _goto_with_retry(page, RTW_START_URL, tries=3, timeout=60000)
            await page.wait_for_timeout(600)

            await _click_start_now(page)
            await page.wait_for_load_state("domcontentloaded", timeout=60000)
            await page.wait_for_timeout(600)

            # Share code page
            await _fill_share_code(page, sc)
            await _click_continue(page)
            await page.wait_for_load_state("domcontentloaded", timeout=60000)
            await page.wait_for_timeout(500)

            err = await _detect_rtw_error_message(page)
            if err:
                await _try_save_error_pdf(page, error_pdf)
                await context.tracing.stop(path=str(trace_path))
                await context.close()
                await browser.close()
                return {
                    "ok": False,
                    "error": err,
                    "error_pdf": str(error_pdf),
                    "error_png": str(error_png),
                    "trace_path": str(trace_path),
                }


            # DOB page
            await _fill_dob(page, dd, mm, yyyy)
            await _click_continue(page)
            await page.wait_for_load_state("domcontentloaded", timeout=60000)
            await page.wait_for_timeout(500)

            err = await _detect_rtw_error_message(page)
            if err:
                await _try_save_error_pdf(page, error_pdf)
                await context.tracing.stop(path=str(trace_path))
                await context.close()
                await browser.close()
                return {
                    "ok": False,
                    "error": err,
                    "error_pdf": str(error_pdf),
                    "error_png": str(error_png),
                    "trace_path": str(trace_path),
                }


            # Company name page
            await _fill_company(page, comp)
            await _click_continue(page)
            await page.wait_for_load_state("domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1200)

            err = await _detect_rtw_error_message(page)
            if err:
                await _try_save_error_pdf(page, error_pdf)
                await context.tracing.stop(path=str(trace_path))
                await context.close()
                await browser.close()
                return {
                    "ok": False,
                    "error": err,
                    "error_pdf": str(error_pdf),
                    "error_png": str(error_png),
                    "trace_path": str(trace_path),
                }
            # Final page: download PDF
            await page.wait_for_load_state("domcontentloaded", timeout=90000)
            try:
                await page.wait_for_load_state("networkidle", timeout=90000)
            except Exception:
                pass
            await page.wait_for_timeout(800)

            # Cookie banner can block the link on GOV.UK
            try:
                for cookie_sel in [
                    "button:has-text('Accept additional cookies')",
                    "button:has-text('Accept analytics cookies')",
                    "button:has-text('Accept cookies')",
                    "button:has-text('Hide cookie message')",
                ]:
                    loc = page.locator(cookie_sel).first
                    if await loc.count():
                        try:
                            await loc.click(timeout=3000)
                            await page.wait_for_timeout(400)
                        except Exception:
                            pass
            except Exception:
                pass

            download = None

            # Wait until a download link/button is present
            candidates = [
                lambda: page.get_by_role("link", name=re.compile(r"Download\s+PDF", re.I)),
                lambda: page.get_by_role("button", name=re.compile(r"Download\s+PDF", re.I)),
                lambda: page.locator("a:has-text('Download PDF')"),
                lambda: page.locator("a[href$='.pdf' i]"),
                lambda: page.locator("a[href*='pdf' i]:has-text('Download')"),
            ]

            clicked = False
            last_click_err: Exception | None = None

            for get_loc in candidates:
                loc = get_loc().first
                try:
                    if await loc.count():
                        await loc.scroll_into_view_if_needed(timeout=10000)
                        await loc.wait_for(state="visible", timeout=60000)
                        async with page.expect_download(timeout=120000) as dlinfo:
                            await loc.click(timeout=60000)
                        download = await dlinfo.value
                        clicked = True
                        break
                except Exception as e:
                    last_click_err = e
                    continue

            if download is None and not clicked:
                raise RuntimeError(f"Download PDF link not clickable: {last_click_err}")

            if download is None:
                raise RuntimeError("Download did not start")

            await download.save_as(str(pdf_path))

            await context.tracing.stop(path=str(trace_path))
            await context.close()
            await browser.close()

            return {
                "ok": True,
                "pdf_path": str(pdf_path),
                "filename": pdf_path.name,
                "error_pdf": str(error_pdf) if error_pdf.exists() else None,
                "trace_path": str(trace_path),
            }

        except Exception as e:
            try:
                await page.screenshot(path=str(error_png), full_page=True)
            except Exception:
                pass
            try:
                await _try_save_error_pdf(page, error_pdf)
            except Exception:
                pass
            try:
                await context.tracing.stop(path=str(trace_path))
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass

            return {
                "ok": False,
                "error": str(e),
                "error_png": str(error_png),
                "error_pdf": str(error_pdf) if error_pdf.exists() else None,
                "trace_path": str(trace_path),
            }
