# Pyodide Browser PoC

This is a small browser-side proof of concept for a **very limited** subset of
`gfal`-like behavior.

What it currently demonstrates:

- `stat` for a local file or a CORS-enabled URL
- `checksum` for a local file or a small CORS-enabled URL payload
- Python running in the browser via Pyodide

What it does **not** do:

- full `gfal` execution in the browser
- `root://` / XRootD
- local `file://` paths outside the browser file picker
- recursive copy / directory traversal
- direct EOS public access without a proxy if the browser blocks the request on CORS

!!! note "EOS public and CORS"
    As tested on April 22, 2026, `eospublic.cern.ch` responds to plain `HEAD`
    and `OPTIONS`, but does **not** advertise browser CORS headers in the
    responses we checked. In practice, that means this page can preset EOS
    public URLs for convenience, but a real browser fetch may still require a
    same-origin relay/proxy to succeed.

!!! note "Checksum size limit"
    The checksum action deliberately refuses large payloads to keep this PoC
    responsive in the browser. It is meant for interactive experimentation, not
    bulk data transfer.

<div class="admonition tip">
  <p class="admonition-title">Suggested inputs</p>
  <p>
    Small PHENIX file:
    <code>https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C</code>
  </p>
  <p>
    Medium ATLAS family reference:
    <code>https://eospublic.cern.ch/eos/opendata/atlas/rucio/data16_13TeV/DAOD_PHYSLITE.37019892._000001.pool.root.1</code>
  </p>
</div>

<div id="pyodide-gfal-poc" class="gfal-poc">
  <div class="gfal-poc__row">
    <button type="button" id="poc-load-runtime">Load Pyodide</button>
    <span id="poc-runtime-status">Runtime not loaded yet.</span>
  </div>

  <div class="gfal-poc__section">
    <h2>Remote URL</h2>
    <label for="poc-remote-url">Target URL</label>
    <input
      id="poc-remote-url"
      type="url"
      value="https://eospublic.cern.ch/eos/opendata/phenix/emcal-finding-pi0s-and-photons/single_cluster_r5.C"
    />

    <label for="poc-proxy-base">Optional proxy base URL</label>
    <input
      id="poc-proxy-base"
      type="url"
      placeholder="Leave empty for direct fetch, or use something like https://your-host/proxy"
    />

    <div class="gfal-poc__row">
      <button type="button" id="poc-preset-phenix">Use PHENIX preset</button>
      <button type="button" id="poc-preset-atlas">Use ATLAS preset</button>
      <button type="button" id="poc-stat-url">stat(URL)</button>
    </div>

    <div class="gfal-poc__row">
      <label for="poc-algorithm-url">Checksum algorithm</label>
      <select id="poc-algorithm-url">
        <option value="md5">MD5</option>
        <option value="sha256">SHA256</option>
        <option value="adler32">ADLER32</option>
      </select>
      <button type="button" id="poc-checksum-url">checksum(URL)</button>
    </div>
  </div>

  <div class="gfal-poc__section">
    <h2>Local File</h2>
    <label for="poc-file-input">Choose a local file</label>
    <input id="poc-file-input" type="file" />

    <div class="gfal-poc__row">
      <button type="button" id="poc-stat-file">stat(file)</button>
      <label for="poc-algorithm-file">Checksum algorithm</label>
      <select id="poc-algorithm-file">
        <option value="md5">MD5</option>
        <option value="sha256">SHA256</option>
        <option value="adler32">ADLER32</option>
      </select>
      <button type="button" id="poc-checksum-file">checksum(file)</button>
    </div>
  </div>

  <div class="gfal-poc__section">
    <h2>Output</h2>
    <pre id="poc-output">Click "Load Pyodide" to start.</pre>
  </div>
</div>

<style>
  .gfal-poc {
    display: grid;
    gap: 1rem;
  }

  .gfal-poc__section {
    border: 1px solid var(--md-default-fg-color--lightest);
    border-radius: 0.5rem;
    padding: 1rem;
  }

  .gfal-poc__row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    align-items: center;
    margin: 0.75rem 0;
  }

  .gfal-poc input,
  .gfal-poc select,
  .gfal-poc button {
    font: inherit;
  }

  .gfal-poc input[type="url"] {
    width: 100%;
    max-width: 64rem;
    padding: 0.55rem 0.7rem;
  }

  .gfal-poc button {
    padding: 0.55rem 0.8rem;
    cursor: pointer;
  }

  #poc-output {
    min-height: 20rem;
    overflow: auto;
    white-space: pre-wrap;
  }
</style>

<script src="https://cdn.jsdelivr.net/pyodide/v0.29.3/full/pyodide.js"></script>
<script src="assets/pyodide-gfal-poc.js"></script>
