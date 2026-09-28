"use strict";
const $ = (id) => document.getElementById(id);
let busy = false,
  comparisonRunning = false,
  comparison = null;
const bytes = (n) =>
  n < 1024
    ? `${n} B`
    : n < 1048576
      ? `${(n / 1024).toFixed(1)} KiB`
      : `${(n / 1048576).toFixed(2)} MiB`;
const escape = (s) =>
  String(s).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
function error(e) {
  $("notice").textContent = e.message;
  $("notice").hidden = false;
}
async function api(path, body) {
  const response = await fetch(
    `/api/${path}`,
    body === undefined
      ? {}
      : {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-KV-Demo": "1" },
          body: JSON.stringify(body),
        },
  );
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Request failed");
  return data;
}
function rows(records) {
  return records.length
    ? records
        .map(
          (r) =>
            `<tr><td>${escape(r.key)}</td><td>${r.value === null ? '<span class="tombstone">DELETED</span>' : escape(r.value)}${r.truncated ? "…" : ""}</td><td>${r.sequence}</td></tr>`,
        )
        .join("")
    : '<tr><td colspan="3" class="empty">No records in memory.<br>Save a value or write a sample batch to begin.</td></tr>';
}
function render(s) {
  $("sequence").textContent = s.sequence.toLocaleString();
  $("mem-bytes").textContent = bytes(s.active.bytes);
  $("mem-caption").textContent =
    `${s.active.records} records · 64 KiB threshold`;
  $("sst-count").textContent = s.tables.length;
  $("sst-size").textContent = `${bytes(s.stats.sst_bytes)} on disk`;
  $("written").textContent = bytes(
    s.stats.flush_bytes + s.stats.compaction_output_bytes,
  );
  $("wal-label").textContent = `${bytes(s.wal_bytes)} · synced writes`;
  $("active-label").textContent = `${s.active.records} active records`;
  $("frozen-label").textContent = `${s.frozen.length} WAITING TO FLUSH`;
  $("memory-fill").style.width =
    `${Math.min(100, (s.active.bytes / s.memtable_limit) * 100)}%`;
  $("memory-rows").innerHTML = rows(s.active.preview);
  $("frozen-preview").innerHTML = s.frozen
    .map(
      (m) =>
        `<p class="fine">Frozen WAL ${m.wal_id}: ${m.records} records · ${bytes(m.bytes)}</p>`,
    )
    .join("");
  const max = Math.max(1, ...s.tables.map((t) => t.bytes));
  $("sst-files").innerHTML = s.tables.length
    ? s.tables
        .map(
          (t) =>
            `<button class="file" data-table="${t.id}" aria-label="Inspect SST ${t.id}"><span class="file-id">SST ${String(t.id).padStart(4, "0")}</span><span class="bar-wrap"><span class="bar" style="display:block;width:${(t.bytes / max) * 100}%"></span></span><small>${bytes(t.bytes)} · ${t.records} records</small></button>`,
        )
        .join("")
    : '<div class="empty">No SST files yet.<br>Flush your writes to create the first sorted table.</div>';
  $("events").innerHTML = s.events.length
    ? s.events
        .map(
          (e) =>
            `<div class="event"><span>${String(e.id).padStart(2, "0")}</span><strong>${escape(e.action)}</strong><span>${escape(e.detail)}</span><time>${(e.micros / 1000).toFixed(2)} ms</time></div>`,
        )
        .join("")
    : '<p class="empty">Your operations will appear here.</p>';
  $("dot").classList.remove("off");
  $("connection").textContent = "Local engine connected";
  $("table-detail").hidden = true;
}
async function perform(action, body = {}) {
  if (busy) return;
  busy = true;
  $("notice").hidden = true;
  document
    .querySelectorAll("#explorer button")
    .forEach((b) => (b.disabled = true));
  try {
    const result = await api(action, body);
    render(result.state);
    if (action === "get")
      $("read-result").textContent =
        result.value === null
          ? "Not found — no live value for this key."
          : JSON.stringify(result.value);
    if (action === "reset")
      $("read-result").textContent = "Read a key to see its value.";
  } catch (e) {
    error(e);
  } finally {
    busy = false;
    document
      .querySelectorAll("#explorer button")
      .forEach((b) => (b.disabled = false));
  }
}
$("kv-form").addEventListener("submit", (e) => {
  e.preventDefault();
  perform("put", { key: $("key").value, value: $("value").value });
});
for (const action of ["get", "delete"])
  $(action).onclick = () => perform(action, { key: $("key").value });
for (const action of ["flush", "compact", "batch", "reopen"])
  $(action).onclick = () => perform(action);
$("reset").onclick = () => {
  if (confirm("Discard this demo sandbox and start a new one?")) {
    comparisonRunning = false;
    comparison = null;
    perform("reset");
    resetComparison();
  }
};
$("sst-files").onclick = async (e) => {
  const button = e.target.closest("[data-table]");
  if (!button || busy) return;
  try {
    const data = await api(`table/${button.dataset.table}`);
    $("disk-rows").innerHTML = rows(data.records);
    $("table-detail-title").textContent =
      `SST ${String(data.id).padStart(4, "0")} · first 32 records`;
    $("table-detail").hidden = false;
  } catch (e) {
    error(e);
  }
};
document.querySelectorAll("[data-tab]").forEach(
  (button) =>
    (button.onclick = () => {
      document.querySelectorAll("[data-tab]").forEach((b) => {
        b.classList.toggle("active", b === button);
        b.setAttribute("aria-selected", String(b === button));
      });
      for (const id of ["explorer", "comparison"])
        $(id).hidden = id !== button.dataset.tab;
    }),
);
function svgElement(name, attrs) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attrs))
    node.setAttribute(key, value);
  return node;
}
function chart(history) {
  const svg = $("chart");
  svg.replaceChildren();
  const max = Math.max(1024, ...history.flatMap((p) => [p.full, p.tiered]));
  for (let i = 0; i <= 4; i++) {
    const y = 220 - i * 50;
    svg.append(
      svgElement("line", { x1: 65, x2: 890, y1: y, y2: y, stroke: "#e5e9e2" }),
    );
    const label = svgElement("text", {
      x: 55,
      y: y + 4,
      "text-anchor": "end",
      fill: "#6f7974",
      "font-size": 10,
      "font-family": "monospace",
    });
    label.textContent = bytes((max * i) / 4);
    svg.append(label);
  }
  for (const [policy, color] of [
    ["full", "#bd7855"],
    ["tiered", "#197958"],
  ]) {
    const points = history
      .map(
        (p) => `${65 + (p.batch / 24) * 825},${220 - (p[policy] / max) * 200}`,
      )
      .join(" ");
    svg.append(
      svgElement("polyline", {
        points,
        fill: "none",
        stroke: color,
        "stroke-width": 3,
        "stroke-linejoin": "round",
      }),
    );
  }
}
function compareRender(c) {
  const max = Math.max(
    1,
    ...c.full.tables.map((t) => t.bytes),
    ...c.tiered.tables.map((t) => t.bytes),
  );
  for (const p of ["full", "tiered"]) {
    const s = c[p].stats;
    $(p + "-total").textContent = bytes(
      s.flush_bytes + s.compaction_output_bytes,
    );
    $(p + "-files").innerHTML = c[p].tables
      .map(
        (t) =>
          `<div class="mini-file" style="height:${Math.max(5, (t.bytes / max) * 90)}px" title="SST ${t.id}: ${bytes(t.bytes)}"></div>`,
      )
      .join("");
    $(p + "-stats").textContent =
      `${s.sst_files} live files · ${bytes(s.sst_bytes)} stored · ${s.compactions} merges`;
  }
  const last = c.history.at(-1);
  $("saving").textContent =
    `${((1 - last.tiered / last.full) * 100).toFixed(1)}% fewer SST bytes`;
  $("compare-status").textContent =
    `Batch ${c.batch} / 24 · ${c.batch * 64} identical updates · values verified`;
  chart(c.history);
}
function resetComparison() {
  for (const p of ["full", "tiered"]) {
    $(p + "-total").textContent = "—";
    $(p + "-files").replaceChildren();
  }
  $("saving").textContent = "—";
  $("compare-status").textContent =
    "Ready · 1,024 seeded keys · 24 batches of 64 updates";
  $("compare-start").textContent = "Run comparison ↗";
  $("compare-pause").hidden = true;
  chart([]);
}
$("compare-pause").onclick = () => {
  comparisonRunning = false;
  $("compare-pause").disabled = true;
};
$("compare-start").onclick = async () => {
  if (busy || comparisonRunning) return;
  busy = true;
  comparisonRunning = true;
  $("notice").hidden = true;
  $("compare-start").disabled = true;
  $("compare-pause").hidden = false;
  $("compare-pause").disabled = false;
  document
    .querySelectorAll("#explorer button")
    .forEach((b) => (b.disabled = true));
  try {
    if (!comparison || comparison.batch >= 24) {
      $("compare-status").textContent = "Seeding two isolated databases…";
      comparison = await api("compare/start", {});
      compareRender(comparison);
    }
    while (comparisonRunning && comparison.batch < 24) {
      comparison = await api("compare/step", {});
      compareRender(comparison);
      await new Promise((r) => setTimeout(r, 100));
    }
    $("compare-start").textContent =
      comparison.batch >= 24 ? "Run again ↗" : "Resume comparison ↗";
  } catch (e) {
    error(e);
    comparison = null;
    $("compare-start").textContent = "Retry comparison ↗";
  } finally {
    busy = false;
    comparisonRunning = false;
    $("compare-start").disabled = false;
    $("compare-pause").hidden = true;
    document
      .querySelectorAll("#explorer button")
      .forEach((b) => (b.disabled = false));
  }
};
chart([]);
api("state")
  .then(render)
  .catch((e) => {
    $("dot").classList.add("off");
    $("connection").textContent = "Engine unavailable";
    error(e);
  });
