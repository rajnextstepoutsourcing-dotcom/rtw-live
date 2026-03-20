/* RTW Check — app.js (NextStep SaaS) */
"use strict";

const $ = id => document.getElementById(id);
function setText(id, msg) { const el=$(id); if(el) el.textContent=msg||""; }

function setConf(id, value) {
  if (!$(id)) return;
  let v = Number(value);
  if (!isFinite(v) || v <= 0) { $(id).textContent = ""; return; }
  if (v > 0 && v <= 1) v = v * 100;
  const pct = Math.round(v);
  $(id).textContent = pct ? `Confidence: ${pct}%` : "";
}

// ── Progress ──────────────────────────────────────────────────────────────────
let _pollTimer = null;

function _showProgress(state, msg) {
  const wrap = $("progressWrap"); if (!wrap) return;
  wrap.classList.remove("hidden");
  const badge = $("progressBadge"); const pmsg = $("progressMsg");
  const bar   = $("progressBar");
  if (pmsg) pmsg.textContent = msg || "";
  if (state === "running") {
    if (badge) { badge.className = "badge running"; badge.textContent = "⚙️ Processing"; }
    // Animate bar slowly while running
    let pct = 0;
    if (bar) {
      const anim = setInterval(() => {
        pct = Math.min(pct + 2, 85); // never reaches 100 until done
        bar.style.width = pct + "%";
        if (pct >= 85) clearInterval(anim);
      }, 800);
    }
  } else if (state === "done") {
    if (badge) { badge.className = "badge clear"; badge.textContent = "✓ Complete"; }
    if (bar) bar.style.width = "100%";
  } else if (state === "failed") {
    if (badge) { badge.className = "badge portal_unavailable"; badge.textContent = "✗ Failed"; }
    if (bar) bar.style.width = "100%";
  }
}

function _hideProgress() {
  const w = $("progressWrap"); if (w) w.classList.add("hidden");
}

function _stopPoll() {
  if (_pollTimer) { clearTimeout(_pollTimer); _pollTimer = null; }
}

function _pollStatus(jobId) {
  _pollTimer = setTimeout(async function poll() {
    try {
      const r = await fetch(`/rtw/status/${jobId}`);
      if (!r.ok) { _pollTimer = setTimeout(poll, 2000); return; }
      const data = await r.json();
      const state = data.state || "running";
      _showProgress(state, data.message || "");

      if (state === "done") {
        _stopPoll();
        // Auto-trigger download
        if (data.pdf_url) {
          const a = document.createElement("a");
          a.href = data.pdf_url;
          a.download = data.filename || "RTW-Check.pdf";
          document.body.appendChild(a);
          a.click();
          a.remove();
        }
        setText("runStatus", "Downloaded. Run again to check another candidate.");
        const btn = $("btnRun");
        if (btn) { btn.disabled = false; btn.textContent = "Run & Download PDF"; }
        return;
      }

      if (state === "failed") {
        _stopPoll();
        const msg = data.error || data.message || "RTW check failed.";
        setText("runStatus", msg);
        // If error PDF available, offer download
        if (data.pdf_url) {
          const a = document.createElement("a");
          a.href = data.pdf_url;
          a.download = data.filename || "RTW-Error.pdf";
          a.textContent = "⬇ Download Error Report";
          a.className = "btnSmall downloadBtn";
          a.style.marginTop = "8px";
          a.style.display = "inline-block";
          const statusEl = $("runStatus");
          if (statusEl && statusEl.parentNode) {
            statusEl.parentNode.insertBefore(a, statusEl.nextSibling);
          }
        }
        const btn = $("btnRun");
        if (btn) { btn.disabled = false; btn.textContent = "Run & Download PDF"; }
        return;
      }

      // Still running
      _pollTimer = setTimeout(poll, 2000);
    } catch(e) {
      console.warn("[Poll]", e);
      _pollTimer = setTimeout(poll, 3000);
    }
  }, 1500);
}

// ── Extract ───────────────────────────────────────────────────────────────────
$("btnExtract")?.addEventListener("click", async () => {
  const shareFile = $("share_file")?.files[0];
  const dobFile   = $("dob_file")?.files[0];
  if (!shareFile || !dobFile) {
    alert("Please upload both Share Code document and DOB document.");
    return;
  }
  setText("extractStatus", "Extracting…");
  setDisabled("btnExtract", true);

  const form = new FormData();
  form.append("share_file", shareFile);
  form.append("dob_file",   dobFile);

  try {
    const res  = await fetch("/rtw/extract", { method: "POST", body: form });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();

    $("share_code").value = (data.share_code_display || data.share_code_raw9 || data.share_code || "").trim();
    $("dob_day").value    = data.dob_day   || "";
    $("dob_month").value  = data.dob_month || "";
    $("dob_year").value   = data.dob_year  || "";

    setConf("conf_share", data.confidence?.share_code);
    setConf("conf_dob",   data.confidence?.dob);

    setText("extractStatus", "Done — review and edit fields if needed.");
  } catch(e) {
    console.error(e);
    setText("extractStatus", "Extraction failed. Please enter details manually.");
  } finally {
    setDisabled("btnExtract", false);
  }
});

function setDisabled(id, v) { const el=$(id); if(el) el.disabled=!!v; }

// ── Run ───────────────────────────────────────────────────────────────────────
$("btnRun")?.addEventListener("click", async () => {
  _stopPoll();
  _hideProgress();

  const payload = {
    company_name: $("company_name")?.value.trim() || "",
    share_code:   $("share_code")?.value.trim()   || "",
    dob_day:      $("dob_day")?.value.trim()      || "",
    dob_month:    $("dob_month")?.value.trim()    || "",
    dob_year:     $("dob_year")?.value.trim()     || "",
  };

  if (!payload.company_name) { alert("Please enter Company name."); return; }
  if (!payload.share_code || !payload.dob_day || !payload.dob_month || !payload.dob_year) {
    alert("Please fill Share code and full Date of Birth."); return;
  }

  // Remove any previous error download links
  document.querySelectorAll("a.downloadBtn").forEach(a => a.remove());

  const btn = $("btnRun");
  if (btn) { btn.disabled = true; btn.textContent = "Running…"; }
  setText("runStatus", "");
  _showProgress("running", "Connecting to GOV.UK RTW portal…");

  try {
    const res  = await fetch("/rtw/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();

    if (!res.ok) {
      const msg = data?.detail || "Run failed.";
      _showProgress("failed", msg);
      setText("runStatus", msg);
      if (btn) { btn.disabled = false; btn.textContent = "Run & Download PDF"; }
      return;
    }

    // Job submitted — poll for status
    if (data.job_id) {
      _pollStatus(data.job_id);
    }

  } catch(e) {
    console.error(e);
    const msg = e?.message || "Run failed.";
    _showProgress("failed", msg);
    setText("runStatus", msg);
    if (btn) { btn.disabled = false; btn.textContent = "Run & Download PDF"; }
  }
});
