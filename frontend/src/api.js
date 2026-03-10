const API_BASE = "/api";

export async function getReport() {
  const res = await fetch(`${API_BASE}/report`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || res.statusText);
  }
  return res.json();
}

export async function processData(options) {
  const { sourceType, sourceUrl, startDate, endDate, file, apiKey } = options;

  const formData = new FormData();
  formData.append("source_type", sourceType);
  if (sourceUrl) formData.append("source_url", sourceUrl);
  if (startDate) formData.append("start_date", startDate);
  if (endDate) formData.append("end_date", endDate);
  if (apiKey) formData.append("api_key", apiKey);
  if (file) formData.append("file", file);

  const res = await fetch(`${API_BASE}/process`, {
    method: "POST",
    body: formData, // fetch automatically sets Content-Type for FormData
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || res.statusText || "Process failed");
  }
  if (!data.success) {
    throw new Error(data.error || "Process failed");
  }
  return {
    id: data.id,
    report: data.report,
    raw_report: data.raw_report,
    cleaned_report: data.cleaned_report,
    raw_data: data.raw_data,
    cleaned_data: data.cleaned_data,
    report_url: data.report_url,
    eda_url: data.eda_url,
    raw_eda_url: data.raw_eda_url,
  };
}

export async function retrieveAnalysis(fileId) {
  const res = await fetch(`${API_BASE}/retrieve/${fileId}`);
  const data = await res.json().catch(() => ({ error: "Retrieval failed" }));

  if (!res.ok) {
    throw new Error(data.error || data.detail || "Retrieval failed");
  }

  // Backend returns { success: false, error: "...", expired: true } for expired records
  if (!data.success) {
    throw new Error(data.error || "Retrieval failed");
  }

  return data.record;
}

export async function sendAnalysisIdEmail(email, analysisId) {
  const res = await fetch(`${API_BASE}/send-analysis-id`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, analysis_id: analysisId }),
  });
  const data = await res.json().catch(() => ({ error: "Request failed" }));
  if (!res.ok || !data.success) {
    throw new Error(data.error || "Failed to send email");
  }
  return data;
}

export async function getRawData() {
  const res = await fetch(`${API_BASE}/raw-data`);
  const data = await res.json().catch(() => ({ data: [] }));
  return data.data || data.properties || [];
}
