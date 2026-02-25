# RTW Check (Right to Work)

Upload two documents:
1) Share Code document (PDF/image)
2) DOB document (passport/DBS PDF/image)

Click **Extract**, review/edit the extracted values, then click **Run & Download PDF**.
The app will open GOV.UK Right to Work service in a headless browser, complete the check, and download the official PDF.

## Local run
```bash
pip install -r requirements.txt
playwright install --with-deps chromium
uvicorn app:app --reload
```

## Render
Set `GEMINI_API_KEY` if you want reliable extraction from scanned images/PDFs.
