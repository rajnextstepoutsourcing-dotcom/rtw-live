const $ = (id) => document.getElementById(id);

function setStatus(el, msg) {
  el.textContent = msg;
}

function fillField(id, value) {
  $(id).value = value || "";
}

function setConf(id, value) {
  if (!$(id)) return;
  const pct = Math.round(Number(value) || 0);
  $(id).textContent = pct ? `Confidence: ${pct}%` : "";
}

$('btnExtract').addEventListener('click', async () => {
  const shareFile = $('share_file').files[0];
  const dobFile = $('dob_file').files[0];

  if (!shareFile || !dobFile) {
    alert('Please upload both Share Code document and DOB document.');
    return;
  }

  setStatus($('extractStatus'), 'Extracting...');

  const form = new FormData();
  form.append('share_file', shareFile);
  form.append('dob_file', dobFile);

  try {
    const res = await fetch('/rtw/extract', { method: 'POST', body: form });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();

    fillField('share_code', data.share_code);
    fillField('dob_day', data.dob_day);
    fillField('dob_month', data.dob_month);
    fillField('dob_year', data.dob_year);

    setConf('conf_share', data.confidence?.share_code);
    setConf('conf_dob', data.confidence?.dob);

    setStatus($('extractStatus'), 'Done. Review/edit fields if needed.');
  } catch (e) {
    console.error(e);
    setStatus($('extractStatus'), 'Extraction failed. Check console/logs.');
    alert('Extraction failed. If these are scanned images/PDFs, set GEMINI_API_KEY on Render.');
  }
});

$('btnRun').addEventListener('click', async () => {
  const payload = {
    company_name: $('company_name').value.trim(),
    share_code: $('share_code').value.trim(),
    dob_day: $('dob_day').value.trim(),
    dob_month: $('dob_month').value.trim(),
    dob_year: $('dob_year').value.trim(),
  };

  if (!payload.company_name) {
    alert('Please enter Company name.');
    return;
  }
  if (!payload.share_code || !payload.dob_day || !payload.dob_month || !payload.dob_year) {
    alert('Please extract and ensure Share code and full DOB are filled.');
    return;
  }

  setStatus($('runStatus'), 'Running... this may take a bit.');

  try {
    const res = await fetch('/rtw/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (!res.ok) throw new Error(await res.text());

    const blob = await res.blob();

    // Get filename from Content-Disposition
    let filename = 'RTW-Check.pdf';
    const cd = res.headers.get('Content-Disposition') || res.headers.get('content-disposition');
    if (cd) {
      const match = cd.match(/filename="?([^";]+)"?/i);
      if (match) filename = match[1];
    }

    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);

    setStatus($('runStatus'), 'Downloaded.');
  } catch (e) {
    console.error(e);
    setStatus($('runStatus'), 'Run failed. Check logs.');
    alert('Run failed. If GOV.UK RTW site changed or blocks automation, we will need to update selectors/flow.');
  }
});
