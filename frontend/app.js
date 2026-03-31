/* ============================================================
   GeoRelay — Complete Application Logic
   Features: GPS, Map Click, Simulation, Frequency Control,
   Registration, Debug Panel, Latency Tracking
   ============================================================ */
(() => {
  "use strict";

  // ── App State ─────────────────────────────────────────────
  const S = {
    ws: null,
    username: null,
    sessionId: null,
    seq: 0,
    dbTables: null,
    mode: "gps",           // gps | click | simulate
    streaming: false,
    gpsWatchId: null,
    simInterval: null,
    simStep: 0,
    freqMs: 500,
    subscribers: [],
    locations: {},         // { username: { lat, lon, accuracy, ts } }
    markers: {},
    selfMarker: null,
    accuracyCircle: null,
    map: null,
    debugOpen: false,
    sidebarOpen: false,
    logCollapsed: false,
    ctrlCollapsed: false,
    // Stats
    packetsSent: 0,
    packetsRecv: 0,
    connectedAt: null,
    latencies: [],
    lastSendTs: null,
    terminalWaitingForDump: false,
    // Latency tracking: we record the time when we send each seq
    sendTimestamps: {},    // { seq: timestamp_ms }
  };

  const FREQ_MAP = [100, 250, 500, 1000];
  const FREQ_DESC = ["🔥 Stress test (10/sec)", "⚡ Fast (4/sec)", "⚖️ Balanced (2/sec)", "🐢 Relaxed (1/sec)"];

  const COLORS = ["#f97316", "#06b6d4", "#ec4899", "#22c55e", "#a855f7", "#14b8a6", "#f43f5e", "#3b82f6", "#eab308", "#84cc16"];
  function colorFor(u) {
    let h = 0;
    for (let i = 0; i < u.length; i++) h = u.charCodeAt(i) + ((h << 5) - h);
    return COLORS[Math.abs(h) % COLORS.length];
  }

  // ── Simulation Paths ──────────────────────────────────────
  const SIM_PATHS = {
    circle: { center: [19.076, 72.8777], radius: 0.01, label: "Circle (Mumbai)" },
    zigzag: { center: [28.6139, 77.209], step: 0.0003, label: "Zigzag (Delhi)" },
    random: { center: [12.9716, 77.5946], step: 0.0002, label: "Random Walk (Bangalore)" },
    highway: { start: [26.9124, 75.7873], end: [28.6139, 77.209], label: "Highway (Jaipur→Delhi)" },
  };

  // ── DOM ───────────────────────────────────────────────────
  const $ = (s) => document.querySelector(s);
  const $$ = (s) => document.querySelectorAll(s);

  // Auth
  const authPage = $("#auth-page"), dashPage = $("#dashboard-page");
  const loginForm = $("#login-form"), regForm = $("#register-form");
  const loginBtn = $("#login-btn"), regBtn = $("#register-btn");

  // Dashboard
  const connPill = $("#conn-status");
  const topUser = $("#topbar-username"), topAvatar = $("#user-avatar");
  const sessionDisp = $("#session-display");
  const logList = $("#log-list");

  // Controls
  const ctrlPanel = $("#control-panel"), ctrlBody = $("#ctrl-body");
  const gpsBtn = $("#gps-btn"), simBtn = $("#sim-btn");
  const simPathSel = $("#sim-path");
  const freqSlider = $("#freq-slider"), freqDisplay = $("#freq-display");

  // Debug
  const debugPanel = $("#debug-panel");

  // Sidebar
  const sidebar = $("#sidebar");
  const subInput = $("#sub-input"), subList = $("#sub-list");

  // ── Logging ───────────────────────────────────────────────
  function log(msg, lvl = "info") {
    const li = document.createElement("li");
    li.className = `l-${lvl}`;
    li.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
    logList.prepend(li);
    while (logList.children.length > 80) logList.removeChild(logList.lastChild);
  }

  // ── Connection Status ─────────────────────────────────────
  function setConn(status) {
    connPill.className = `conn-pill conn-${status}`;
    connPill.querySelector(".conn-text").textContent =
      status === "connected" ? "Connected" : status === "connecting" ? "Connecting…" : "Disconnected";
  }

  // ── WebSocket ─────────────────────────────────────────────
  function wsConnect(url, msgType, username, password) {
    return new Promise((resolve, reject) => {
      setConn("connecting");
      log(`Connecting to ${url}…`);

      let done = false;
      const ws = new WebSocket(url);

      ws.onopen = () => {
        log("WebSocket opened", "ok");
        ws.send(JSON.stringify({ type: msgType, username, password }));
      };

      ws.onmessage = (e) => {
        S.packetsRecv++;
        let d;
        try { d = JSON.parse(e.data); } catch { log("Bad JSON from server", "err"); return; }
        routeMessage(d);
        if (!done) {
          if (d.type === "auth_ok" || d.type === "register_ok") { done = true; resolve(d); }
          else if (d.type === "auth_error" || d.type === "register_error" || d.type === "error") {
            done = true; reject(new Error(d.message || "Failed"));
          }
        }
      };

      ws.onerror = () => { if (!done) { done = true; reject(new Error("Connection failed — is the server running?")); } };
      ws.onclose = (e) => {
        setConn("disconnected");
        log(`Connection closed (code ${e.code})`, "warn");
        S.ws = null;
        updateDebug();
        if (!done) { done = true; reject(new Error("Connection closed")); }
      };

      S.ws = ws;
    });
  }

  function send(obj) {
    if (S.ws && S.ws.readyState === WebSocket.OPEN) {
      S.ws.send(JSON.stringify(obj));
      S.packetsSent++;
      S.lastSendTs = Date.now();
      updateDebug();
    }
  }

  // ── Message Router ────────────────────────────────────────
  function routeMessage(d) {
    switch (d.type) {
      case "auth_ok": case "register_ok":
        S.sessionId = d.session_id;
        S.connectedAt = Date.now();
        setConn("connected");
        log(`✅ Authenticated as ${d.username} (session ${d.session_id})`, "ok");
        break;
      case "auth_error": case "register_error":
        log(`❌ ${d.message}`, "err"); break;
      case "subscribe_ok":
        log(`📡 Subscribed to: ${d.users.join(", ")}`, "ok"); break;
      case "location_broadcast":
        handleBroadcast(d); break;
      case "admin_dump_ok":
        S.dbTables = d.tables;
        S.terminalWaitingForDump = false;
        renderTerminalSnapshot();
        break;
      case "error":
        if (S.terminalWaitingForDump && typeof d.message === "string" && d.message.includes("Admin dump failed")) {
          S.terminalWaitingForDump = false;
          terminalPrint(`<div class="term-line" style="color:#f87171">ERROR: ${d.message}</div>`);
        }
        log(`⚠️ ${d.message}`, "err"); break;
      default:
        log(`Unknown: ${d.type}`, "warn");
    }
    updateDebug();
  }

  // ── Location Broadcast Handler ────────────────────────────
  function handleBroadcast(d) {
    const { username, lat, lon, accuracy, server_ts, seq } = d;
    S.locations[username] = { lat, lon, accuracy, ts: server_ts, seq };

    // Latency measurement: if this is OUR broadcast echoed back, measure round-trip
    if (username === S.username && seq != null && S.sendTimestamps[seq]) {
      const rtt = Date.now() - S.sendTimestamps[seq];
      S.latencies.push(rtt);
      if (S.latencies.length > 50) S.latencies.shift(); // keep last 50
      delete S.sendTimestamps[seq];
    }

    if (username === S.username) updateSelfMarker(lat, lon, accuracy);
    else updateOtherMarker(username, lat, lon, accuracy, server_ts);

    refreshSubList();
    // Only log every 5th packet during high-frequency to reduce log spam
    if (S.packetsRecv % 5 === 0 || !S.streaming) {
      log(`📍 ${username}: ${lat.toFixed(5)}, ${lon.toFixed(5)} #${seq}`);
    }
  }

  // ── Map ───────────────────────────────────────────────────
  function initMap() {
    S.map = L.map("map", { center: [20.5, 78.9], zoom: 5, zoomControl: true });
    L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
      attribution: '© <a href="https://osm.org/copyright">OSM</a> © <a href="https://carto.com/">CARTO</a>',
      subdomains: "abcd", maxZoom: 19,
    }).addTo(S.map);

    // Map click handler
    S.map.on("click", (e) => {
      if (S.mode !== "click") return;
      const { lat, lng } = e.latlng;
      sendLocation(lat, lng, null);
      log(`🖱️ Clicked: ${lat.toFixed(5)}, ${lng.toFixed(5)}`);
    });
  }

  // Create marker icon ONCE per user, reuse it (prevents blinking)
  const iconCache = {};
  function getIcon(color, isSelf) {
    const key = `${color}_${isSelf}`;
    if (!iconCache[key]) {
      const s = isSelf ? 20 : 16;
      const cls = isSelf ? "pulse-marker" : "";
      iconCache[key] = L.divIcon({
        className: "",
        iconSize: [s, s],
        iconAnchor: [s / 2, s / 2],
        html: `<div class="${cls}" style="width:${s}px;height:${s}px;background:${color};border-radius:50%;border:3px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,0.4);"></div>`,
      });
    }
    return iconCache[key];
  }

  function updateSelfMarker(lat, lon, accuracy) {
    const icon = getIcon("#6366f1", true);
    if (!S.selfMarker) {
      S.selfMarker = L.marker([lat, lon], { icon, zIndexOffset: 1000 }).addTo(S.map);
      S.selfMarker.bindPopup(selfPopup(lat, lon, accuracy));
      S.map.setView([lat, lon], 15);
    } else {
      // Just move, don't recreate icon (prevents blink)
      S.selfMarker.setLatLng([lat, lon]);
      S.selfMarker.setPopupContent(selfPopup(lat, lon, accuracy));
    }
    if (accuracy && accuracy < 500) {
      if (!S.accuracyCircle) {
        S.accuracyCircle = L.circle([lat, lon], { radius: accuracy, color: "#6366f1", fillOpacity: 0.08, weight: 1 }).addTo(S.map);
      } else {
        S.accuracyCircle.setLatLng([lat, lon]).setRadius(accuracy);
      }
    }
  }

  function selfPopup(lat, lon, acc) {
    return `<div><div class="popup-user">📍 ${S.username} (You)</div><div class="popup-coords">${lat.toFixed(6)}, ${lon.toFixed(6)}</div>${acc != null ? `<div class="popup-meta">Accuracy: ±${acc.toFixed(1)}m</div>` : ""}</div>`;
  }

  function updateOtherMarker(user, lat, lon, accuracy, ts) {
    const color = colorFor(user);
    const icon = getIcon(color, false);
    if (!S.markers[user]) {
      S.markers[user] = L.marker([lat, lon], { icon }).addTo(S.map);
    } else {
      // Just move the existing marker, don't touch the icon
      S.markers[user].setLatLng([lat, lon]);
    }
    const t = ts ? new Date(ts).toLocaleTimeString() : "—";
    S.markers[user].bindPopup(`<div><div class="popup-user">👤 ${user}</div><div class="popup-coords">${lat.toFixed(6)}, ${lon.toFixed(6)}</div>${accuracy != null ? `<div class="popup-meta">±${accuracy.toFixed(1)}m</div>` : ""}<div class="popup-meta">Last: ${t}</div></div>`);
  }

  // ── Send Location Helper ──────────────────────────────────
  function sendLocation(lat, lon, accuracy) {
    S.seq++;
    S.sendTimestamps[S.seq] = Date.now(); // record send time for latency calc
    send({
      type: "location_update",
      lat, lon, accuracy,
      seq: S.seq,
      client_ts: new Date().toISOString(),
      username: S.username,
    });
  }

  // ── GPS Mode ──────────────────────────────────────────────
  function startGPS() {
    if (!("geolocation" in navigator)) {
      log("⚠️ Geolocation not available in this browser", "warn");
      return;
    }
    S.streaming = true;
    gpsBtn.classList.add("action-btn-active");
    gpsBtn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><rect x="6" y="6" width="12" height="12" rx="2"/></svg> Stop Live Sharing`;
    log("🛰️ GPS streaming started", "ok");

    S.gpsWatchId = navigator.geolocation.watchPosition(
      (pos) => sendLocation(pos.coords.latitude, pos.coords.longitude, pos.coords.accuracy),
      (err) => log(`GPS error: ${err.message}. Try Map Click or Simulation mode instead.`, "warn"),
      { enableHighAccuracy: true, maximumAge: 2000, timeout: 10000 }
    );
  }

  function stopGPS() {
    S.streaming = false;
    gpsBtn.classList.remove("action-btn-active");
    gpsBtn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><polygon points="3 11 22 2 13 21 11 13 3 11"/></svg> Start Live Sharing`;
    if (S.gpsWatchId != null) { navigator.geolocation.clearWatch(S.gpsWatchId); S.gpsWatchId = null; }
    log("🛰️ GPS streaming stopped", "warn");
  }

  // ── Simulation Mode ───────────────────────────────────────
  function startSim() {
    S.streaming = true;
    S.simStep = 0;
    simBtn.classList.add("action-btn-active");
    simBtn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><rect x="6" y="6" width="12" height="12" rx="2"/></svg> Stop Simulation`;

    const pathKey = simPathSel.value;
    const path = SIM_PATHS[pathKey];
    log(`🔁 Simulation started: ${path.label} @ ${S.freqMs}ms`, "ok");

    S.simInterval = setInterval(() => {
      S.simStep++;
      let lat, lon;

      if (pathKey === "circle") {
        const angle = (S.simStep * 0.05) % (2 * Math.PI);
        lat = path.center[0] + path.radius * Math.sin(angle);
        lon = path.center[1] + path.radius * Math.cos(angle);
      } else if (pathKey === "zigzag") {
        lat = path.center[0] + (S.simStep % 2 === 0 ? 1 : -1) * path.step * (S.simStep / 2);
        lon = path.center[1] + path.step * S.simStep;
      } else if (pathKey === "random") {
        const last = S.locations[S.username] || { lat: path.center[0], lon: path.center[1] };
        lat = last.lat + (Math.random() - 0.5) * path.step * 2;
        lon = last.lon + (Math.random() - 0.5) * path.step * 2;
      } else if (pathKey === "highway") {
        const t = (S.simStep * 0.005) % 1;
        lat = path.start[0] + (path.end[0] - path.start[0]) * t + (Math.random() - 0.5) * 0.001;
        lon = path.start[1] + (path.end[1] - path.start[1]) * t + (Math.random() - 0.5) * 0.001;
      }

      sendLocation(lat, lon, 5 + Math.random() * 15);
    }, S.freqMs);
  }

  function stopSim() {
    S.streaming = false;
    simBtn.classList.remove("action-btn-active");
    simBtn.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18"><polygon points="5 3 19 12 5 21 5 3"/></svg> Start Simulation`;
    if (S.simInterval) { clearInterval(S.simInterval); S.simInterval = null; }
    log("🔁 Simulation stopped", "warn");
  }

  // ── Stop All Streaming ────────────────────────────────────
  function stopAll() {
    if (S.gpsWatchId != null) stopGPS();
    if (S.simInterval) stopSim();
    S.streaming = false;
  }

  // ── Subscriber Management ────────────────────────────────
  function subscribeTo(u) {
    if (!u || u === S.username) { log("Can't subscribe to yourself", "warn"); return; }
    if (S.subscribers.includes(u)) { log(`Already tracking ${u}`, "warn"); return; }
    S.subscribers.push(u);
    send({ type: "subscribe", users: S.subscribers });
    refreshSubList();
  }

  function unsubscribeFrom(u) {
    S.subscribers = S.subscribers.filter(x => x !== u);
    if (S.markers[u]) { S.map.removeLayer(S.markers[u]); delete S.markers[u]; }
    delete S.locations[u];
    send({ type: "subscribe", users: S.subscribers });
    refreshSubList();
    log(`Unsubscribed from ${u}`);
  }

  function refreshSubList() {
    subList.innerHTML = "";
    if (!S.subscribers.length) {
      subList.innerHTML = `<li style="padding:16px;text-align:center;color:var(--text4);font-size:12px">No subscribers yet.<br>Add a username above to start tracking.</li>`;
      return;
    }
    S.subscribers.forEach(u => {
      const loc = S.locations[u];
      const c = colorFor(u);
      const li = document.createElement("li");
      li.className = "sub-item";
      li.innerHTML = `<div class="sub-info"><div class="sub-avatar" style="background:${c}">${u[0].toUpperCase()}</div><div><div class="sub-name">${u}</div><div class="sub-coords">${loc ? `${loc.lat.toFixed(4)}, ${loc.lon.toFixed(4)}` : "Waiting…"}</div></div></div><button class="sub-remove" data-u="${u}">✕</button>`;
      subList.appendChild(li);
    });
    subList.querySelectorAll(".sub-remove").forEach(b => b.addEventListener("click", () => unsubscribeFrom(b.dataset.u)));
  }

  // ── Debug Panel Update ────────────────────────────────────
  function updateDebug() {
    const loc = S.locations[S.username];
    $("#dbg-coords").textContent = loc ? `${loc.lat.toFixed(5)}, ${loc.lon.toFixed(5)}` : "—";
    $("#dbg-freq").textContent = S.streaming ? `${S.freqMs}ms` : "Idle";
    $("#dbg-sent").textContent = S.packetsSent;
    $("#dbg-recv").textContent = S.packetsRecv;

    // Latency = average round-trip time
    if (S.latencies.length > 0) {
      const avg = S.latencies.reduce((a, b) => a + b, 0) / S.latencies.length;
      $("#dbg-latency").textContent = avg.toFixed(1) + "ms";
    } else {
      $("#dbg-latency").textContent = "—";
    }

    $("#dbg-last-sent").textContent = S.lastSendTs ? new Date(S.lastSendTs).toLocaleTimeString() : "—";
    $("#dbg-ws-state").textContent = S.ws ? ["CONNECTING", "OPEN", "CLOSING", "CLOSED"][S.ws.readyState] : "CLOSED";
    if (S.connectedAt) {
      const sec = Math.floor((Date.now() - S.connectedAt) / 1000);
      const m = Math.floor(sec / 60), s = sec % 60;
      $("#dbg-uptime").textContent = `${m}m ${s}s`;
    }
  }
  setInterval(updateDebug, 1000);

  // ── Page Navigation ───────────────────────────────────────
  function showDash() {
    authPage.classList.remove("active");
    dashPage.classList.add("active");
    topUser.textContent = S.username;
    topAvatar.textContent = S.username[0].toUpperCase();
    sessionDisp.textContent = S.sessionId;
    setTimeout(() => { initMap(); refreshSubList(); }, 80);
  }

  function logout() {
    stopAll();
    if (S.ws) { S.ws.close(1000, "logout"); S.ws = null; }
    if (S.map) { S.map.remove(); S.map = null; }
    Object.assign(S, {
      selfMarker: null, accuracyCircle: null, markers: {}, subscribers: [], locations: {},
      sessionId: null, username: null, seq: 0, packetsSent: 0, packetsRecv: 0,
      connectedAt: null, latencies: [], lastSendTs: null, streaming: false, sendTimestamps: {},
    });
    // Clear icon cache
    Object.keys(iconCache).forEach(k => delete iconCache[k]);
    setConn("disconnected");
    dashPage.classList.remove("active");
    authPage.classList.add("active");
    log("Logged out", "warn");
  }

  // ── Frontend Password Validation ──────────────────────────
  function validateRegFields() {
    const u = $("#reg-username").value.trim();
    const p = $("#reg-password").value;
    const p2 = $("#reg-password2").value;
    const errEl = $("#register-error");

    if (u.length < 8) {
      errEl.textContent = "Username must be at least 8 characters";
      errEl.style.display = "block";
      return false;
    }
    if (p.length < 8) {
      errEl.textContent = "Password must be at least 8 characters";
      errEl.style.display = "block";
      return false;
    }
    if (!/[A-Z]/.test(p)) {
      errEl.textContent = "Password must contain at least one uppercase letter (A-Z)";
      errEl.style.display = "block";
      return false;
    }
    if (!/[@#]/.test(p)) {
      errEl.textContent = "Password must contain at least one special character (@ or #)";
      errEl.style.display = "block";
      return false;
    }
    if (p !== p2) {
      errEl.textContent = "Passwords do not match";
      errEl.style.display = "block";
      return false;
    }
    return true;
  }

  // ── Auth Form Handlers ────────────────────────────────────
  async function handleAuth(formType) {
    const isLogin = formType === "login";
    const btn = isLogin ? loginBtn : regBtn;
    const errEl = isLogin ? $("#login-error") : $("#register-error");

    const uField = isLogin ? "#login-username" : "#reg-username";
    const pField = isLogin ? "#login-password" : "#reg-password";
    const sField = isLogin ? "#login-server" : "#reg-server";

    const username = $(uField).value.trim();
    const password = $(pField).value;
    const url = $(sField).value.trim();

    if (!username || !password || !url) return;

    // Extra frontend validation for registration
    if (!isLogin && !validateRegFields()) return;

    btn.querySelector(".btn-text").style.display = "none";
    btn.querySelector(".btn-loader").style.display = "inline-flex";
    btn.disabled = true;
    errEl.style.display = "none";

    try {
      S.username = username;
      const msgType = isLogin ? "auth" : "register";
      await wsConnect(url, msgType, username, password);
      showDash();
    } catch (err) {
      errEl.textContent = err.message;
      errEl.style.display = "block";
      S.username = null;
    } finally {
      btn.querySelector(".btn-text").style.display = "inline";
      btn.querySelector(".btn-loader").style.display = "none";
      btn.disabled = false;
    }
  }

  // ── Event Wiring ──────────────────────────────────────────

  // Auth tabs
  $$(".auth-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      $$(".auth-tab").forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      const target = tab.dataset.tab;
      loginForm.classList.toggle("active", target === "login");
      regForm.classList.toggle("active", target === "register");
    });
  });

  // Login/Register submit
  loginForm.addEventListener("submit", (e) => { e.preventDefault(); handleAuth("login"); });
  regForm.addEventListener("submit", (e) => { e.preventDefault(); handleAuth("register"); });

  // Server URL toggle (hidden by default)
  $("#show-server-login").addEventListener("click", () => {
    const g = $("#login-server-group");
    g.style.display = g.style.display === "none" ? "block" : "none";
  });
  $("#show-server-reg").addEventListener("click", () => {
    const g = $("#reg-server-group");
    g.style.display = g.style.display === "none" ? "block" : "none";
  });

  // Mode tabs
  $$(".mode-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      stopAll();
      $$(".mode-tab").forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      S.mode = tab.dataset.mode;
      $$(".ctrl-mode-panel").forEach(p => p.classList.remove("active"));
      $(`#mode-${S.mode}`).classList.add("active");
    });
  });

  // GPS button
  gpsBtn.addEventListener("click", () => S.streaming ? stopGPS() : startGPS());

  // Sim button
  simBtn.addEventListener("click", () => S.streaming ? stopSim() : startSim());

  // Frequency slider
  freqSlider.addEventListener("input", () => {
    const idx = parseInt(freqSlider.value);
    S.freqMs = FREQ_MAP[idx];
    freqDisplay.innerHTML = `Current: <strong>${S.freqMs}ms</strong> · <span id="freq-desc">${FREQ_DESC[idx]}</span>`;
    // If simulating, restart with new frequency
    if (S.simInterval) { stopSim(); startSim(); }
  });

  // Control panel collapse
  $("#ctrl-collapse").addEventListener("click", () => {
    S.ctrlCollapsed = !S.ctrlCollapsed;
    ctrlPanel.classList.toggle("collapsed", S.ctrlCollapsed);
    $("#ctrl-collapse").textContent = S.ctrlCollapsed ? "+" : "—";
  });

  // Debug panel
  $("#debug-toggle-btn").addEventListener("click", () => {
    S.debugOpen = !S.debugOpen;
    debugPanel.classList.toggle("open", S.debugOpen);
  });
  $("#debug-close").addEventListener("click", () => {
    S.debugOpen = false;
    debugPanel.classList.remove("open");
  });

  // Sidebar
  $("#sidebar-toggle-btn").addEventListener("click", () => {
    S.sidebarOpen = !S.sidebarOpen;
    sidebar.classList.toggle("open", S.sidebarOpen);
    setTimeout(() => S.map && S.map.invalidateSize(), 350);
  });
  $("#sidebar-close-btn").addEventListener("click", () => {
    S.sidebarOpen = false;
    sidebar.classList.remove("open");
    setTimeout(() => S.map && S.map.invalidateSize(), 350);
  });

  // Subscribe
  $("#sub-add-btn").addEventListener("click", () => {
    const u = subInput.value.trim();
    if (u) { subscribeTo(u); subInput.value = ""; }
  });
  subInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      const u = subInput.value.trim();
      if (u) { subscribeTo(u); subInput.value = ""; }
    }
  });

  // Logout
  $("#logout-btn").addEventListener("click", logout);

  // Activity log collapse (click on the toggle arrow, not the header)
  $("#act-log-btn").addEventListener("click", (e) => {
    e.stopPropagation();
    S.logCollapsed = !S.logCollapsed;
    $("#activity-log").classList.toggle("collapsed", S.logCollapsed);
    $("#act-log-btn").textContent = S.logCollapsed ? "▸" : "▾";
  });

  // Activity log maximize/resize
  $("#act-max-btn").style.display = "none"; // Hide maximize button, replaced by robust resizer below

  // ── Draggable/Resizable Panels ────────────────────────────
  function makeResizable8Way(panel) {
    const dirs = ['n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw'];
    dirs.forEach(d => {
      const h = document.createElement('div');
      h.className = `resizer resizer-${d}`;
      panel.appendChild(h);
      h.addEventListener('mousedown', (e) => initResize(e, d));
    });

    let resDir = null, startX, startY, startW, startH, startL, startT;

    function initResize(e, dir) {
      e.stopPropagation();
      e.preventDefault();
      resDir = dir;
      startX = e.clientX; startY = e.clientY;
      const rect = panel.getBoundingClientRect();
      startW = rect.width; startH = rect.height;
      startL = rect.left; startT = rect.top;

      panel.style.transition = 'none';
      panel.style.width = startW + 'px';
      panel.style.height = startH + 'px';
      panel.style.left = startL + 'px';
      panel.style.top = startT + 'px';
      panel.style.bottom = 'auto';
      panel.style.right = 'auto';

      document.addEventListener('mousemove', doResize);
      document.addEventListener('mouseup', stopResize);
      document.body.style.cursor = window.getComputedStyle(e.target).cursor;
    }

    function doResize(e) {
      if (!resDir) return;
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;

      if (resDir.includes('e')) panel.style.width = Math.max(200, startW + dx) + 'px';
      if (resDir.includes('s')) panel.style.height = Math.max(100, startH + dy) + 'px';
      if (resDir.includes('w')) {
        const w = Math.max(200, startW - dx);
        panel.style.width = w + 'px';
        panel.style.left = (startL + (startW - w)) + 'px';
      }
      if (resDir.includes('n')) {
        const h = Math.max(100, startH - dy);
        panel.style.height = h + 'px';
        panel.style.top = (startT + (startH - h)) + 'px';
      }

      // Expand internal log list relative size
      const logList = panel.querySelector('.log-list');
      if (logList) {
        logList.style.maxHeight = 'calc(100% - 40px)';
      }
    }

    function stopResize() {
      resDir = null;
      document.removeEventListener('mousemove', doResize);
      document.removeEventListener('mouseup', stopResize);
      document.body.style.cursor = '';
    }
  }
  function makeDraggable(panel, handle) {
    let isDragging = false, startX, startY, origLeft, origTop;

    handle.addEventListener("mousedown", (e) => {
      // Don't drag if clicking a button inside the header
      if (e.target.closest(".icon-btn-xs")) return;
      isDragging = true;
      const rect = panel.getBoundingClientRect();
      startX = e.clientX;
      startY = e.clientY;
      origLeft = rect.left;
      origTop = rect.top;
      panel.style.transition = "none";
      document.body.style.cursor = "move";
      e.preventDefault();
    });

    document.addEventListener("mousemove", (e) => {
      if (!isDragging) return;
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;
      panel.style.left = (origLeft + dx) + "px";
      panel.style.top = (origTop + dy) + "px";
      // Override right/bottom positioning
      panel.style.right = "auto";
      panel.style.bottom = "auto";
    });

    document.addEventListener("mouseup", () => {
      if (isDragging) {
        isDragging = false;
        document.body.style.cursor = "";
      }
    });
  }

  // Make control panel and activity log draggable
  makeDraggable(ctrlPanel, $(".ctrl-header"));
  makeDraggable($("#activity-log"), $("#act-log-toggle"));
  makeResizable8Way($("#activity-log"));

  // ── DB Terminal Logic ─────────────────────────────────────
  const termOverlay = $("#db-terminal-page");
  const termBody = $("#term-body");
  const termTyped = $("#term-typed");
  const termOut = $("#term-output");

  function openTerminal() {
    termOverlay.classList.add("active");
    termOut.innerHTML = "";
    termTyped.focus();
    if (S.ws && S.ws.readyState === WebSocket.OPEN) {
      requestAdminDump();
    } else {
      terminalPrint(`<div class="term-line" style="color:#f87171">ERROR: WebSocket is not connected.</div>`);
    }
    // Set scroll to bottom
    setTimeout(() => termBody.scrollTop = termBody.scrollHeight, 10);
  }

  function closeTerminal() {
    termOverlay.classList.remove("active");
  }

  function terminalPrint(html) {
    const div = document.createElement("div");
    div.innerHTML = html;
    termOut.appendChild(div);
    termBody.scrollTop = termBody.scrollHeight;
  }
  function requestAdminDump() {
    S.terminalWaitingForDump = true;
    terminalPrint(`<div class="term-line">Loading live database snapshot...</div>`);
    send({ type: "admin_dump" });
  }

  function renderTerminalSnapshot() {
    if (!termOverlay.classList.contains("active")) return;
    terminalPrint(`<div class="term-line">Snapshot refreshed.</div>`);
    const tables = S.dbTables ? Object.keys(S.dbTables).map(t => ({ "Tables_in_webrtc_app": t })) : [];
    terminalPrint(formatAsciiTable(tables));
    ["Users", "Sessions", "Locations", "Logs"].forEach((table) => {
      terminalPrint(`<div class="term-line"><span class="term-prompt">mysql></span> select * from ${table};</div>`);
      terminalPrint(formatAsciiTable((S.dbTables && S.dbTables[table]) || []));
    });
  }

  function formatAsciiTable(data) {
    if (!data || !data.length) return "Empty set (0.00 sec)";
    const keys = Object.keys(data[0]);

    // Find max width for each column
    const colWidths = {};
    keys.forEach(k => {
      let max = k.length;
      data.forEach(row => {
        const val = row[k] === null ? "NULL" : String(row[k]);
        if (val.length > max) max = val.length;
      });
      colWidths[k] = max;
    });

    let separator = "+";
    keys.forEach(k => separator += "-".repeat(colWidths[k] + 2) + "+");

    let header = "|";
    keys.forEach(k => header += " " + k.padEnd(colWidths[k], " ") + " |");

    let rowsStr = "";
    data.forEach(row => {
      let r = "|";
      keys.forEach(k => {
        const val = row[k] === null ? "NULL" : String(row[k]);
        r += " " + val.padEnd(colWidths[k], " ") + " |";
      });
      rowsStr += r + "\\n";
    });

    return `<div class="ascii-table">${separator}\\n${header}\\n${separator}\\n${rowsStr}${separator}\\n${data.length} rows in set (0.00 sec)</div>`;
  }

  function handleTerminalCommand(cmd) {
    const raw = cmd.trim();
    const c = raw.toLowerCase().replace(/;$/, ""); // remove trailing semicolon

    terminalPrint(`<div class="term-line"><span class="term-prompt">mysql></span> ${raw}</div>`);

    if (!c) return;
    if (c === "clear") {
      termOut.innerHTML = "";
      return;
    }

    if (c === "use webrtc_app" || c === "use georelay") {
      terminalPrint(`<div class="term-line">Database changed</div>`);
      return;
    }

    if (c === "show tables") {
      if (!S.dbTables) {
        requestAdminDump();
        return;
      }
      const tables = S.dbTables ? Object.keys(S.dbTables).map(t => ({ "Tables_in_webrtc_app": t })) : [];
      terminalPrint(formatAsciiTable(tables));
      return;
    }

    if (c.startsWith("select * from ")) {
      const table = c.split("select * from ")[1].trim().toLowerCase();
      if (!S.dbTables) {
        terminalPrint(`<div class="term-line" style="color:#f87171">ERROR: Database not loaded. Ensure you're connected.</div>`);
        return;
      }
      // Match case-insensitive
      const key = Object.keys(S.dbTables).find(k => k.toLowerCase() === table);
      if (key) {
        terminalPrint(formatAsciiTable(S.dbTables[key]));
      } else {
        terminalPrint(`<div class="term-line" style="color:#f87171">ERROR 1146 (42S02): Table 'webrtc_app.${table}' doesn't exist</div>`);
      }
      return;
    }

    if (c === "help" || c === "\\h") {
      terminalPrint(`<div class="term-line">Supported commands for viva: use webrtc_app, show tables, select * from Users, select * from Sessions, select * from Locations, select * from Logs, clear.</div>`);
      return;
    }

    terminalPrint(`<div class="term-line" style="color:#f87171">ERROR 1064 (42000): You have an error in your SQL syntax near '${raw}'</div>`);
  }

  termTyped.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      const val = termTyped.value.trim();
      handleTerminalCommand(val);
      termTyped.value = "";
    }
  });

  // Ensure clicking in terminal keeps focus on input
  termBody.addEventListener("click", () => termTyped.focus());

  $("#db-term-btn").addEventListener("click", openTerminal);
  $("#close-term-btn").addEventListener("click", closeTerminal);

})();
