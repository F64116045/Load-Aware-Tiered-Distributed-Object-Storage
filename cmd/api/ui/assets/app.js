const apiBaseInput = document.getElementById("apiBase");
const putObjectId = document.getElementById("putObjectId");
const putContentType = document.getElementById("putContentType");
const putTextPayload = document.getElementById("putTextPayload");
const putFilePayload = document.getElementById("putFilePayload");
const getObjectId = document.getElementById("getObjectId");
const deleteObjectId = document.getElementById("deleteObjectId");
const adminObjectId = document.getElementById("adminObjectId");
const getPreview = document.getElementById("getPreview");
const logPanel = document.getElementById("logPanel");
const downloadBtn = document.getElementById("downloadBtn");

const putBtn = document.getElementById("putBtn");
const getBtn = document.getElementById("getBtn");
const deleteBtn = document.getElementById("deleteBtn");
const healthBtn = document.getElementById("healthBtn");
const nodesBtn = document.getElementById("nodesBtn");
const tasksBtn = document.getElementById("tasksBtn");
const objectViewBtn = document.getElementById("objectViewBtn");
const clearLogBtn = document.getElementById("clearLogBtn");
const useCurrentOriginBtn = document.getElementById("useCurrentOriginBtn");

let lastBlob = null;
let lastBlobName = "";
let lastBlobType = "application/octet-stream";

function defaultObjectId() {
  return `demo-object-${Date.now()}`;
}

function nowTag() {
  return new Date().toLocaleTimeString();
}

function setDefaultValues() {
  const id = defaultObjectId();
  apiBaseInput.value = window.location.origin;
  putObjectId.value = id;
  getObjectId.value = id;
  deleteObjectId.value = id;
  adminObjectId.value = id;
}

function logLine(message, data) {
  const formatted = data === undefined ? "" : `\n${JSON.stringify(data, null, 2)}`;
  const next = `[${nowTag()}] ${message}${formatted}\n\n`;
  logPanel.textContent = next + logPanel.textContent;
}

function getApiBase() {
  const v = apiBaseInput.value.trim();
  return v || window.location.origin;
}

async function callJson(path, options = {}) {
  const url = `${getApiBase()}${path}`;
  const res = await fetch(url, options);
  const text = await res.text();
  let body = text;
  try {
    body = text ? JSON.parse(text) : {};
  } catch (err) {
    // keep plain text body
  }
  return { ok: res.ok, status: res.status, body };
}

function useSameIdAcrossForms(id) {
  if (!id) {
    return;
  }
  getObjectId.value = id;
  deleteObjectId.value = id;
  adminObjectId.value = id;
}

function enableDownload(blob, fileName, contentType) {
  lastBlob = blob;
  lastBlobName = fileName;
  lastBlobType = contentType || "application/octet-stream";
  downloadBtn.disabled = false;
}

function inferFileName(objectId, contentType) {
  if (!contentType) {
    return `${objectId}.bin`;
  }
  if (contentType.includes("json")) {
    return `${objectId}.json`;
  }
  if (contentType.includes("text/plain")) {
    return `${objectId}.txt`;
  }
  return `${objectId}.bin`;
}

async function onPutObject() {
  const objectId = putObjectId.value.trim();
  if (!objectId) {
    logLine("PUT skipped: object id is required.");
    return;
  }
  useSameIdAcrossForms(objectId);

  let payload = null;
  let contentType = putContentType.value.trim() || "application/octet-stream";
  const file = putFilePayload.files && putFilePayload.files[0];

  if (file) {
    payload = await file.arrayBuffer();
    if (!putContentType.value.trim() && file.type) {
      contentType = file.type;
      putContentType.value = file.type;
    }
  } else {
    payload = new TextEncoder().encode(putTextPayload.value || "");
  }

  const path = `/v2/objects/${encodeURIComponent(objectId)}`;
  const result = await callJson(path, {
    method: "PUT",
    headers: { "Content-Type": contentType },
    body: payload
  });

  logLine(`PUT ${path} -> ${result.status}`, result.body);
}

async function onGetObject() {
  const objectId = getObjectId.value.trim();
  if (!objectId) {
    logLine("GET skipped: object id is required.");
    return;
  }
  useSameIdAcrossForms(objectId);

  const path = `/v2/objects/${encodeURIComponent(objectId)}`;
  const url = `${getApiBase()}${path}`;
  const res = await fetch(url);
  const contentType = res.headers.get("content-type") || "application/octet-stream";

  if (!res.ok) {
    const errText = await res.text();
    let errBody = errText;
    try {
      errBody = errText ? JSON.parse(errText) : {};
    } catch (err) {
      // keep raw text
    }
    logLine(`GET ${path} -> ${res.status}`, errBody);
    getPreview.textContent = `Request failed: ${res.status}\n${typeof errBody === "string" ? errBody : JSON.stringify(errBody, null, 2)}`;
    return;
  }

  const blob = await res.blob();
  const fileName = inferFileName(objectId, contentType);
  enableDownload(blob, fileName, contentType);

  const isTextLike = contentType.includes("json") || contentType.startsWith("text/");
  if (isTextLike) {
    const text = await blob.text();
    getPreview.textContent = text || "(empty payload)";
  } else {
    getPreview.textContent = `Binary payload received.\ncontent-type: ${contentType}\nsize: ${blob.size} bytes`;
  }

  logLine(`GET ${path} -> ${res.status}`, {
    object_id: objectId,
    content_type: contentType,
    size_bytes: blob.size
  });
}

async function onDeleteObject() {
  const objectId = deleteObjectId.value.trim();
  if (!objectId) {
    logLine("DELETE skipped: object id is required.");
    return;
  }
  useSameIdAcrossForms(objectId);

  const path = `/v2/objects/${encodeURIComponent(objectId)}`;
  const result = await callJson(path, { method: "DELETE" });
  logLine(`DELETE ${path} -> ${result.status}`, result.body);
}

async function onHealth() {
  const result = await callJson("/health");
  logLine(`GET /health -> ${result.status}`, result.body);
}

async function onNodes() {
  const result = await callJson("/v2/admin/nodes?limit=50");
  logLine(`GET /v2/admin/nodes -> ${result.status}`, result.body);
}

async function onTasks() {
  const objectId = adminObjectId.value.trim();
  const suffix = objectId ? `?object_id=${encodeURIComponent(objectId)}&limit=100` : "?limit=100";
  const path = `/v2/admin/tasks${suffix}`;
  const result = await callJson(path);
  logLine(`GET ${path} -> ${result.status}`, result.body);
}

async function onObjectView() {
  const objectId = adminObjectId.value.trim();
  if (!objectId) {
    logLine("Admin object view skipped: object id is required.");
    return;
  }
  const path = `/v2/admin/objects/${encodeURIComponent(objectId)}`;
  const result = await callJson(path);
  logLine(`GET ${path} -> ${result.status}`, result.body);
}

function onDownloadLast() {
  if (!lastBlob) {
    return;
  }
  const link = document.createElement("a");
  const objectUrl = URL.createObjectURL(lastBlob.slice(0, lastBlob.size, lastBlobType));
  link.href = objectUrl;
  link.download = lastBlobName || `download-${Date.now()}.bin`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(objectUrl);
}

putBtn.addEventListener("click", onPutObject);
getBtn.addEventListener("click", onGetObject);
deleteBtn.addEventListener("click", onDeleteObject);
healthBtn.addEventListener("click", onHealth);
nodesBtn.addEventListener("click", onNodes);
tasksBtn.addEventListener("click", onTasks);
objectViewBtn.addEventListener("click", onObjectView);
downloadBtn.addEventListener("click", onDownloadLast);
clearLogBtn.addEventListener("click", () => {
  logPanel.textContent = "Ready.";
});
useCurrentOriginBtn.addEventListener("click", () => {
  apiBaseInput.value = window.location.origin;
  logLine(`API base switched to ${window.location.origin}`);
});

setDefaultValues();
logLine("Demo UI loaded.");

