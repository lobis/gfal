const POC_IDS = {
  runtimeButton: "poc-load-runtime",
  runtimeStatus: "poc-runtime-status",
  remoteUrl: "poc-remote-url",
  proxyBase: "poc-proxy-base",
  presetPhenix: "poc-preset-phenix",
  presetAtlas: "poc-preset-atlas",
  statUrl: "poc-stat-url",
  checksumUrl: "poc-checksum-url",
  algorithmUrl: "poc-algorithm-url",
  fileInput: "poc-file-input",
  statFile: "poc-stat-file",
  checksumFile: "poc-checksum-file",
  algorithmFile: "poc-algorithm-file",
  output: "poc-output",
};

const PHENIX_FILE =
  "https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C";
const ATLAS_FILE =
  "https://eospublic.cern.ch/eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019892._000001.pool.root.1";
const MAX_CHECKSUM_BYTES = 16 * 1024 * 1024;
const PYODIDE_INDEX_URL = "https://cdn.jsdelivr.net/pyodide/v0.29.3/full/";

const PY_HELPERS = `
import base64
import hashlib
import json
import zlib


def format_stat(target, headers, status, source):
    size = headers.get("content-length")
    try:
        size = int(size) if size not in (None, "") else None
    except (TypeError, ValueError):
        size = None

    payload = {
        "command": "stat",
        "source": source,
        "target": target,
        "ok": 200 <= int(status) < 400,
        "status": int(status),
        "size": size,
        "content_type": headers.get("content-type"),
        "etag": headers.get("etag"),
        "last_modified": headers.get("last-modified"),
        "accept_ranges": headers.get("accept-ranges"),
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def checksum_from_base64(payload_b64, algorithm):
    raw = base64.b64decode(payload_b64)
    algo = algorithm.lower()

    if algo == "md5":
        value = hashlib.md5(raw).hexdigest()
    elif algo == "sha256":
        value = hashlib.sha256(raw).hexdigest()
    elif algo == "adler32":
        value = f"{zlib.adler32(raw) & 0xFFFFFFFF:08x}"
    else:
        raise ValueError(f"Unsupported checksum algorithm: {algorithm}")

    payload = {
        "command": "checksum",
        "algorithm": algorithm.upper(),
        "bytes": len(raw),
        "value": value,
    }
    return json.dumps(payload, indent=2, sort_keys=True)
`;

const state = {
  pyodide: null,
};

function byId(id) {
  return document.getElementById(id);
}

function setStatus(message) {
  byId(POC_IDS.runtimeStatus).textContent = message;
}

function setOutput(value) {
  byId(POC_IDS.output).textContent = value;
}

function bytesToBase64(bytes) {
  let binary = "";
  const chunk = 0x8000;
  for (let offset = 0; offset < bytes.length; offset += chunk) {
    const slice = bytes.subarray(offset, offset + chunk);
    binary += String.fromCharCode(...slice);
  }
  return btoa(binary);
}

function buildFetchUrl(targetUrl) {
  const proxyBase = byId(POC_IDS.proxyBase).value.trim();
  if (!proxyBase) {
    return targetUrl;
  }
  const proxy = new URL(proxyBase, window.location.href);
  proxy.searchParams.set("url", targetUrl);
  return proxy.toString();
}

async function ensurePyodide() {
  if (state.pyodide) {
    return state.pyodide;
  }

  setStatus("Loading Pyodide runtime...");
  const pyodide = await loadPyodide({ indexURL: PYODIDE_INDEX_URL });
  await pyodide.runPythonAsync(PY_HELPERS);
  state.pyodide = pyodide;
  setStatus("Pyodide ready.");
  return pyodide;
}

async function runStatForUrl() {
  const targetUrl = byId(POC_IDS.remoteUrl).value.trim();
  if (!targetUrl) {
    setOutput("Enter a URL first.");
    return;
  }

  try {
    setOutput("Running stat(URL)...");
    const pyodide = await ensurePyodide();
    const response = await fetch(buildFetchUrl(targetUrl), { method: "HEAD" });
    const headers = Object.fromEntries(response.headers.entries());
    const formatter = pyodide.globals.get("format_stat");
    const result = formatter(targetUrl, headers, response.status, "url");
    setOutput(result);
    formatter.destroy();
  } catch (error) {
    setOutput(
      [
        "stat(URL) failed.",
        "",
        String(error),
        "",
        "If this is an EOS public URL, the browser is likely blocking the request on CORS.",
        "Try a same-origin proxy in the optional proxy field or use the local-file flow below.",
      ].join("\n"),
    );
  }
}

async function runChecksumForUrl() {
  const targetUrl = byId(POC_IDS.remoteUrl).value.trim();
  const algorithm = byId(POC_IDS.algorithmUrl).value;
  if (!targetUrl) {
    setOutput("Enter a URL first.");
    return;
  }

  try {
    setOutput("Running checksum(URL)...");
    const pyodide = await ensurePyodide();
    const response = await fetch(buildFetchUrl(targetUrl), { method: "GET" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const contentLength = Number(response.headers.get("content-length") || 0);
    if (contentLength && contentLength > MAX_CHECKSUM_BYTES) {
      throw new Error(
        `Remote payload is ${contentLength} bytes, which exceeds the ${MAX_CHECKSUM_BYTES}-byte PoC limit.`,
      );
    }

    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.length > MAX_CHECKSUM_BYTES) {
      throw new Error(
        `Fetched ${bytes.length} bytes, which exceeds the ${MAX_CHECKSUM_BYTES}-byte PoC limit.`,
      );
    }

    const checksum = pyodide.globals.get("checksum_from_base64");
    const result = checksum(bytesToBase64(bytes), algorithm);
    setOutput(result);
    checksum.destroy();
  } catch (error) {
    setOutput(
      [
        "checksum(URL) failed.",
        "",
        String(error),
        "",
        "This PoC only hashes small payloads and still depends on browser CORS for remote URLs.",
      ].join("\n"),
    );
  }
}

async function runStatForFile() {
  const file = byId(POC_IDS.fileInput).files[0];
  if (!file) {
    setOutput("Choose a local file first.");
    return;
  }

  const pyodide = await ensurePyodide();
  const formatter = pyodide.globals.get("format_stat");
  const result = formatter(
    file.name,
    {
      "content-length": String(file.size),
      "content-type": file.type || "application/octet-stream",
      "last-modified": new Date(file.lastModified).toUTCString(),
    },
    200,
    "local-file",
  );
  setOutput(result);
  formatter.destroy();
}

async function runChecksumForFile() {
  const file = byId(POC_IDS.fileInput).files[0];
  const algorithm = byId(POC_IDS.algorithmFile).value;
  if (!file) {
    setOutput("Choose a local file first.");
    return;
  }
  if (file.size > MAX_CHECKSUM_BYTES) {
    setOutput(
      `The selected file is ${file.size} bytes. This PoC only hashes files up to ${MAX_CHECKSUM_BYTES} bytes.`,
    );
    return;
  }

  const pyodide = await ensurePyodide();
  const checksum = pyodide.globals.get("checksum_from_base64");
  const bytes = new Uint8Array(await file.arrayBuffer());
  const result = checksum(bytesToBase64(bytes), algorithm);
  setOutput(result);
  checksum.destroy();
}

function wirePoc() {
  byId(POC_IDS.runtimeButton).addEventListener("click", async () => {
    try {
      await ensurePyodide();
      setOutput("Pyodide loaded. Try stat(URL), checksum(URL), or the local file flow.");
    } catch (error) {
      setOutput(`Failed to load Pyodide: ${error}`);
      setStatus("Pyodide failed to load.");
    }
  });

  byId(POC_IDS.presetPhenix).addEventListener("click", () => {
    byId(POC_IDS.remoteUrl).value = PHENIX_FILE;
  });

  byId(POC_IDS.presetAtlas).addEventListener("click", () => {
    byId(POC_IDS.remoteUrl).value = ATLAS_FILE;
  });

  byId(POC_IDS.statUrl).addEventListener("click", () => {
    runStatForUrl();
  });
  byId(POC_IDS.checksumUrl).addEventListener("click", () => {
    runChecksumForUrl();
  });
  byId(POC_IDS.statFile).addEventListener("click", () => {
    runStatForFile();
  });
  byId(POC_IDS.checksumFile).addEventListener("click", () => {
    runChecksumForFile();
  });
}

window.addEventListener("DOMContentLoaded", wirePoc);
