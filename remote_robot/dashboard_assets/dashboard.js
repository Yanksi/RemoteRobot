"use strict";

const state = {
  snapshot: null,
  receivedAt: 0,
  source: null,
  connection: "initializing",
  tickerStarted: false,
};

const $ = (selector) => document.querySelector(selector);
const escapeHTML = (value) => String(value ?? "—")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#039;");

function formatTime(ns) {
  if (!ns) return "—";
  return new Date(Number(ns) / 1e6).toLocaleTimeString([], {
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function shortHash(value) {
  if (!value) return "NO HASH";
  return `${String(value).slice(0, 8)}…${String(value).slice(-5)}`;
}

function stateTag(value) {
  const safe = escapeHTML(value || "unknown");
  return `<span class="state-tag state-${safe}"><i class="state-dot"></i>${safe}</span>`;
}

function setConnection(value) {
  state.connection = value;
  const element = $("#connection-state");
  element.className = `connection-state ${value}`;
  const labels = { live: "LIVE / SSE", stale: "STALE", initializing: "INITIALIZING" };
  element.innerHTML = `<i></i>${labels[value] || value.toUpperCase()}`;
}

function renderMetrics(snapshot) {
  const summary = snapshot.summary;
  $("#metric-roots").textContent = summary.root_count;
  $("#metric-active").textContent = summary.active_run_count;
  $("#metric-leaves").textContent = summary.occupied_leaf_count;
  $("#metric-faults").textContent = summary.faulted_run_count;
  $("#metric-attention").textContent = summary.attention_count;
}

function renderNodes(snapshot) {
  const container = $("#node-grid");
  const roots = new Set(snapshot.roots.map((root) => root.node_id));
  const nodes = [...snapshot.nodes].sort((a, b) => {
    const rootDelta = Number(roots.has(b.node_id)) - Number(roots.has(a.node_id));
    return rootDelta || a.node_id.localeCompare(b.node_id);
  });
  if (!nodes.length) {
    container.innerHTML = `<div class="empty-state">NO CONTROL NODES REGISTERED</div>`;
    return;
  }
  container.innerHTML = nodes.map((node) => {
    const groups = (node.groups || []).map((group) => `
      <div class="group-row">
        <strong>${escapeHTML(group.group_id)}</strong>
        <span>${escapeHTML(group.dimension)} AXES</span>
        <span>${escapeHTML(group.control_rate_hz)} HZ</span>
      </div>`).join("");
    const roles = (node.children || []).map((child) =>
      `<b>${escapeHTML(child.role)}</b> → ${escapeHTML(child.node_id)}`
    ).join(" &nbsp; / &nbsp; ");
    const occupied = node.occupied_by?.length
      ? `LEASED BY <b>${node.occupied_by.map(escapeHTML).join(", ")}</b>`
      : "NO ACTIVE LEASE";
    const runtime = node.runtime?.registered
      ? `WORKER ${node.runtime.reachable ? "RESPONDING" : "UNREACHABLE"}`
      : node.kind === "physical" ? "WORKER NOT REGISTERED" : "DERIVED COMPOSITE STATE";
    return `
      <article class="node-card ${roots.has(node.node_id) ? "root-card" : ""}" data-kind="${escapeHTML(node.kind)}">
        <div class="node-title">
          ${stateTag(node.display_state)}
          <h3>${escapeHTML(node.node_id)}</h3>
        </div>
        <div class="node-meta">
          <span>REV ${escapeHTML(node.revision)}</span>
          <span>${escapeHTML(shortHash(node.manifest_hash))}</span>
          <span>${escapeHTML(runtime)}</span>
        </div>
        ${groups ? `<div class="group-list">${groups}</div>` : ""}
        ${roles ? `<div class="role-map">ROLE MAP &nbsp; ${roles}</div>` : ""}
        <div class="lease-strip">${occupied}</div>
      </article>`;
  }).join("");
}

function renderRuns(snapshot) {
  const container = $("#run-ledger");
  if (!snapshot.runs.length) {
    container.innerHTML = `<div class="empty-state">NO RUNS IN JOURNAL</div>`;
    return;
  }
  const rows = snapshot.runs.map((run) => `
    <div class="run-row">
      <span class="run-id" title="${escapeHTML(run.run_id)}">${escapeHTML(run.run_id)}</span>
      ${stateTag(run.state)}
      <span>${escapeHTML(run.root_id || "UNKNOWN ROOT")}</span>
      <span class="run-phase">${escapeHTML(run.current_phase || "NO ACTIVE PHASE")}</span>
      <time class="run-time">${formatTime(run.updated_at_ns)}</time>
    </div>`).join("");
  container.innerHTML = `
    <div class="run-row header"><span>RUN ID</span><span>STATE</span><span>ROOT</span><span>PHASE</span><span>UPDATED</span></div>
    ${rows}`;
}

function renderEvents(snapshot) {
  const container = $("#event-log");
  if (!snapshot.recent_events.length) {
    container.innerHTML = `<li class="empty-state">NO EVENTS RECORDED</li>`;
    return;
  }
  container.innerHTML = snapshot.recent_events.map((event) => {
    const fault = event.type === "run.faulted" || event.error_type;
    return `
      <li class="event-item ${fault ? "fault" : ""}">
        <time class="event-time">${formatTime(event.recorded_at_ns)}</time>
        <div>
          <div class="event-type">${escapeHTML(event.type)}</div>
          <div class="event-detail">${escapeHTML(event.run_id)} · #${escapeHTML(event.event_seq)}${event.phase_id ? ` · ${escapeHTML(event.phase_id)}` : ""}</div>
        </div>
      </li>`;
  }).join("");
}

function renderSafety(snapshot) {
  const container = $("#safety-list");
  const rows = [];
  snapshot.nodes.forEach((node) => {
    if (!node.attention?.length) {
      rows.push(`<div class="safety-row clear"><strong>${escapeHTML(node.node_id)}</strong><span>DECLARED CLEAR</span></div>`);
      return;
    }
    node.attention.forEach((flag) => {
      const labels = {
        cross_collision_unchecked: "CROSS-COLLISION UNCHECKED",
        supervised_only: "SUPERVISED ONLY",
      };
      rows.push(`<div class="safety-row"><strong>${escapeHTML(node.node_id)}</strong><span>${escapeHTML(labels[flag] || flag)}</span></div>`);
    });
  });
  container.innerHTML = rows.join("") || `<div class="empty-state">NO SAFETY DECLARATIONS</div>`;
}

function render(snapshot) {
  state.snapshot = snapshot;
  state.receivedAt = Date.now();
  $("#server-id").textContent = snapshot.server.server_id;
  $("#footer-sequence").textContent = `EVENT SEQ ${snapshot.operations_event_seq}`;
  renderMetrics(snapshot);
  renderNodes(snapshot);
  renderRuns(snapshot);
  renderEvents(snapshot);
  renderSafety(snapshot);
  setConnection("live");
}

async function fetchSnapshot() {
  const response = await fetch("/api/snapshot", { cache: "no-store" });
  if (!response.ok) throw new Error(`snapshot HTTP ${response.status}`);
  render(await response.json());
}

function connectEvents() {
  if (state.source) state.source.close();
  const after = state.snapshot?.operations_event_seq || 0;
  const source = new EventSource(`/api/events?after=${after}`);
  state.source = source;
  source.addEventListener("operations", (message) => {
    const event = JSON.parse(message.data);
    render(event.snapshot);
  });
  source.onopen = () => setConnection("live");
  source.onerror = () => setConnection("stale");
}

function tick() {
  $("#local-clock").textContent = new Date().toLocaleTimeString([], { hour12: false });
  if (!state.receivedAt) return;
  const age = Math.max(0, Date.now() - state.receivedAt);
  $("#snapshot-age").textContent = `SNAPSHOT AGE ${(age / 1000).toFixed(1)} S`;
  if (age > 5000) setConnection("stale");
}

async function boot() {
  if (!state.tickerStarted) {
    state.tickerStarted = true;
    setInterval(tick, 250);
  }
  tick();
  try {
    await fetchSnapshot();
    connectEvents();
  } catch (error) {
    console.error(error);
    setConnection("stale");
    $("#node-grid").innerHTML = `<div class="empty-state">SNAPSHOT UNAVAILABLE — CHECK SERVER LOG</div>`;
    setTimeout(boot, 2000);
  }
}

boot();
