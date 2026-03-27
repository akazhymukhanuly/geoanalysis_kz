(function(){
  const pad2 = (n)=> (n<10?("0"+n):(""+n));
  const fmtHour = (h)=> pad2(h)+":00";

  const el = {
    topbar: document.getElementById("topbar"),
    topbarToggle: document.getElementById("topbarToggle"),
    controls: document.getElementById("controls"),
    slider: document.getElementById("time-slider"),
    timeVal: document.getElementById("time-val"),
    modeEvents: document.getElementById("modeEvents"),
    modeUnique: document.getElementById("modeUnique"),
    sidebar: document.getElementById("sidebar"),
    sbClose: document.getElementById("sb-close"),
    sbTitle: document.getElementById("sb-title"),
    sbSub: document.getElementById("sb-sub"),
    kpiEvents: document.getElementById("kpiEvents"),
    kpiUsers: document.getElementById("kpiUsers"),
    kpiTopHour: document.getElementById("kpiTopHour"),
    kpiMode: document.getElementById("kpiMode"),
    kpiOblastEvents: document.getElementById("kpiOblastEvents"),
    kpiOblastUsers: document.getElementById("kpiOblastUsers"),
    miniChart: document.getElementById("miniChart"),
    hsList: document.getElementById("hsList"),
    topRayonsList: document.getElementById("topRayonsList"),
    dataQuality: document.getElementById("dataQuality"),
    chartTip: document.getElementById("chartTip"),
    oblastSelect: document.getElementById("oblastSelect"),
    showDetails: document.getElementById("showDetails"),
    showClusters: document.getElementById("showClusters"),
    periodSelect: document.getElementById("periodSelect"),
    anchorDate: document.getElementById("anchorDate"),
    customPeriodWrap: document.getElementById("customPeriodWrap"),
    periodStart: document.getElementById("periodStart"),
    periodEnd: document.getElementById("periodEnd"),
    hourPlayBtn: document.getElementById("hourPlayBtn"),
    dbEvents: document.getElementById("dbEvents"),
    dbUsers: document.getElementById("dbUsers"),
    dbRayons: document.getElementById("dbRayons"),
    dbRayonPct: document.getElementById("dbRayonPct"),
    dbPeriod: document.getElementById("dbPeriod"),
    dbEventsDelta: document.getElementById("dbEventsDelta"),
    dbUsersDelta: document.getElementById("dbUsersDelta"),
    dowHourChart: document.getElementById("dowHourChart"),
    chartToggleBtn: document.getElementById("chartToggleBtn"),
    devicesTitle: document.getElementById("devicesTitle"),
    devicesList: document.getElementById("devicesList"),
    showChoropleth: document.getElementById("showChoropleth"),
    darkModeBtn: document.getElementById("darkModeBtn"),
    dbCoverage: document.getElementById("dbCoverage"),
    rayonSearch: document.getElementById("rayonSearch"),
    choroplethLegend: document.getElementById("choroplethLegend"),
  };

  const state = {
    oblastsFc: null,
    rayonsFc: null,
    rayonCache: new Map(),
    topRegionsLayer: null,
    detailLayer: null,
    heatLayer: null,
    clusterLayer: null,
    lastFiltered: [],
    selectedRayon: null,
    currentFilter: {
      minH: 9, maxH: 18, mode: "events", oblast: "ALL", period: "week", anchorDate: "", startDate: "", endDate: "",
    },
    lastOblast: "ALL",
    detailsShown: true,
    lastRequestId: 0,
    lastDashboardRequestId: 0,
    lastRayonRequestId: 0,
    selectedRayonOblast: null,
    playTimer: null,
    playHour: null,
    topbarCollapsed: false,
    chartMode: "hour",
    hotspotHighlightLayer: null,
    activeHotspotKey: null,
    choroplethData: {},
    choroplethEnabled: false,
    anomalyRayons: new Set(),
    darkMode: false,
  };

  const map = L.map("map", {
    preferCanvas: true,
    zoomSnap: 1,
    zoomDelta: 1,
    attributionControl: false,
  }).setView([48.0, 67.0], 5);
  let resizeTimer = null;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      try { map.invalidateSize({ pan: false, animate: false }); } catch(e) {}
      syncLayoutOffsets();
    }, 120);
  });
  map.createPane("oblastPane");
  map.getPane("oblastPane").style.zIndex = 410;
  map.createPane("rayonPane");
  map.getPane("rayonPane").style.zIndex = 420;

  L.tileLayer("/tiles/{z}/{x}/{y}.png", {
    minZoom: 5,
    maxZoom: 18,
    maxNativeZoom: 18,
    attribution: "OSM via local proxy",
  }).addTo(map);

  function syncLayoutOffsets(){
    const topbar = document.getElementById("topbar");
    if(!topbar) return;
    const rect = topbar.getBoundingClientRect();
    const top = Math.round(rect.bottom + 10);
    document.documentElement.style.setProperty("--dashbar-top", `${top}px`);
  }

  function applyTopbarCollapsed(collapsed){
    state.topbarCollapsed = !!collapsed;
    if(el.topbar){
      el.topbar.classList.toggle("collapsed", state.topbarCollapsed);
      // Fallback for environments where updated CSS didn't load yet.
      el.topbar.style.width = state.topbarCollapsed
        ? "auto"
        : "min(1280px, calc(100vw - 36px))";
    }
    if(el.controls){
      // Same fallback: collapse works even without .collapsed CSS selector.
      el.controls.style.display = state.topbarCollapsed ? "none" : "flex";
    }
    if(el.topbarToggle){
      if(state.topbarCollapsed){
        el.topbarToggle.textContent = "▾ Развернуть";
        el.topbarToggle.title = "Развернуть верхнюю панель";
      } else {
        el.topbarToggle.textContent = "▴ Свернуть";
        el.topbarToggle.title = "Свернуть верхнюю панель";
      }
    }
    syncLayoutOffsets();
    setTimeout(() => {
      try { map.invalidateSize({ pan: false, animate: false }); } catch(e) {}
    }, 120);
  }

  function killFocus(lyr){
    setTimeout(()=>{
      try{
        if(lyr && lyr._path){
          lyr._path.setAttribute("tabindex", "-1");
          lyr._path.style.outline = "none";
          lyr._path.style.boxShadow = "none";
          lyr._path.addEventListener("focus", (e)=> e.target.blur(), true);
        }
      }catch(e){}
    }, 0);
  }

  async function fetchJson(url){
    const r = await fetch(url);
    if(!r.ok) throw new Error(url + " -> " + r.status);
    return await r.json();
  }

  function getMode(){ return el.modeUnique.checked ? "unique" : "events"; }
  function fmtNum(n){ return Number(n || 0).toLocaleString(); }
  function modeLabel(mode){ return mode === "unique" ? "Уникальные ИИН" : "События"; }
  function isPlaying(){ return state.playTimer !== null; }
  function stopPlayback(){
    if(state.playTimer){
      clearInterval(state.playTimer);
      state.playTimer = null;
    }
    state.playHour = null;
    if(el.hourPlayBtn) el.hourPlayBtn.textContent = "▶ По часам";
  }

  function startPlayback(){
    if(isPlaying()) return;
    const vals = el.slider.noUiSlider.get();
    const startHour = parseInt(vals[0], 10);
    state.playHour = Number.isFinite(startHour) ? startHour : 0;
    if(el.hourPlayBtn) el.hourPlayBtn.textContent = "⏸ Пауза";
    state.playTimer = setInterval(() => {
      if(state.playHour === null) state.playHour = 0;
      const h = state.playHour % 24;
      el.slider.noUiSlider.set([h, h]); // сработает on("set") и перерисует карту
      state.playHour = (h + 1) % 24;
    }, 1200);
  }

  function todayIso(){
    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, "0");
    const d = String(now.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  function shiftIso(isoDate, days){
    const d = new Date(isoDate + "T00:00:00");
    d.setDate(d.getDate() + days);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  }

  function syncCustomPeriodVisibility(){
    const isCustom = (el.periodSelect?.value || "week") === "custom";
    if(el.customPeriodWrap){
      el.customPeriodWrap.style.display = isCustom ? "flex" : "none";
    }
  }

  function showTip(x, y, html){
    el.chartTip.innerHTML = html;
    el.chartTip.style.display = "block";
    const pad = 12;
    const rectW = el.chartTip.offsetWidth || 180;
    const rectH = el.chartTip.offsetHeight || 60;
    let left = x + pad;
    let top = y + pad;
    if (left + rectW > window.innerWidth - 8) left = x - rectW - pad;
    if (top + rectH > window.innerHeight - 8) top = y - rectH - pad;
    el.chartTip.style.left = left + "px";
    el.chartTip.style.top = top + "px";
  }

  function hideTip(){ el.chartTip.style.display = "none"; }

  // ---- Dark Mode ----
  function applyDarkMode(on) {
    state.darkMode = !!on;
    document.documentElement.setAttribute("data-theme", on ? "dark" : "light");
    if (el.darkModeBtn) el.darkModeBtn.textContent = on ? "☀️" : "🌙";
    localStorage.setItem("geo_dark_mode", on ? "1" : "0");
  }

  // ---- CountUp animation ----
  function animateNum(domEl, toVal, duration) {
    if (!domEl) return;
    duration = duration || 500;
    const fromVal = parseInt((domEl.textContent || "0").replace(/\D/g, "")) || 0;
    if (fromVal === toVal) { domEl.textContent = toVal.toLocaleString(); return; }
    const start = performance.now();
    const step = (now) => {
      const p = Math.min((now - start) / duration, 1);
      const eased = 1 - Math.pow(1 - p, 3);
      domEl.textContent = Math.round(fromVal + (toVal - fromVal) * eased).toLocaleString();
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }

  // ---- Choropleth ----
  function choroplethColor(pct) {
    if (pct <= 0) return "#e2e8f0";
    const hue = Math.round(220 - pct * 220);
    const sat = Math.round(50 + pct * 40);
    const lig = Math.round(70 - pct * 30);
    return `hsl(${hue},${sat}%,${lig}%)`;
  }

  function detectAnomalies(data) {
    const vals = Object.values(data).map(d => d.events).filter(v => v > 0);
    if (vals.length < 4) return new Set();
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const variance = vals.reduce((a, b) => a + (b - mean) ** 2, 0) / vals.length;
    const std = Math.sqrt(variance);
    const threshold = mean + 1.8 * std;
    return new Set(Object.entries(data).filter(([, d]) => d.events >= threshold).map(([id]) => id));
  }

  function renderChoroplethLegend(maxVal) {
    if (!el.choroplethLegend) return;
    el.choroplethLegend.style.display = "";
    el.choroplethLegend.innerHTML = `
      <div class="ch-title">Хороплет — плотность событий</div>
      <div class="ch-bar"></div>
      <div class="ch-labels"><span>0</span><span>${Math.round(maxVal / 2).toLocaleString()}</span><span>${maxVal.toLocaleString()}</span></div>
    `;
  }

  async function loadChoropleth() {
    if (!state.choroplethEnabled || !state.detailLayer) {
      if (el.choroplethLegend) el.choroplethLegend.style.display = "none";
      return;
    }
    const { oblast, period, anchorDate, startDate, endDate, minH, maxH } = state.currentFilter;
    if (!oblast || oblast === "ALL") {
      if (el.choroplethLegend) el.choroplethLegend.style.display = "none";
      return;
    }
    try {
      const q = new URLSearchParams({ oblast, period, min_h: String(minH), max_h: String(maxH) });
      if (anchorDate) q.set("anchor_date", anchorDate);
      if (period === "custom") {
        if (startDate) q.set("start_date", startDate);
        if (endDate) q.set("end_date", endDate);
      }
      state.choroplethData = await fetchJson(`/api/stats/choropleth?${q}`);
      const vals = Object.values(state.choroplethData).map(d => d.events);
      const maxVal = Math.max(1, ...vals);
      state.anomalyRayons = detectAnomalies(state.choroplethData);

      state.detailLayer.eachLayer(lyr => {
        const fid = lyr.feature?.properties?.full_id;
        const d = state.choroplethData[fid] || { events: 0, users: 0 };
        const pct = d.events / maxVal;
        const isAnomaly = state.anomalyRayons.has(fid);
        lyr.setStyle({
          fillColor: choroplethColor(pct),
          fillOpacity: d.events > 0 ? 0.55 : 0.06,
          color: isAnomaly ? "#ef4444" : "#0f172a",
          weight: isAnomaly ? 2.5 : 1,
        });
        const tip = `<b>${lyr.feature?.properties?.name_kk || "Район"}</b><br>${d.events.toLocaleString()} событий • ${d.users.toLocaleString()} польз.${isAnomaly ? "<br><span style='color:#ef4444;font-weight:700;'>⚠ Аномальная активность</span>" : ""}`;
        lyr.bindTooltip(tip, { sticky: true });
      });
      renderChoroplethLegend(maxVal);
    } catch (e) {
      console.warn("[choropleth]", e);
    }
  }

  function resetChoropleth() {
    if (!state.detailLayer) return;
    if (el.choroplethLegend) el.choroplethLegend.style.display = "none";
    state.detailLayer.eachLayer(lyr => {
      lyr.setStyle({ color: "#0f172a", weight: 1, fillOpacity: 0.03, fillColor: "#ffffff" });
      const name = lyr.feature?.properties?.name_kk || "район";
      lyr.bindTooltip(name, { sticky: true });
    });
  }

  // ---- District Search ----
  function applyRayonSearch(query) {
    if (!state.detailLayer) return;
    const q = query.trim().toLowerCase();
    if (!q) {
      state.detailLayer.eachLayer(lyr => lyr.setStyle({ opacity: 1, fillOpacity: state.choroplethEnabled ? undefined : 0.03 }));
      return;
    }
    let firstMatch = null;
    state.detailLayer.eachLayer(lyr => {
      const name = (lyr.feature?.properties?.name_kk || "").toLowerCase();
      const matches = name.includes(q);
      if (matches && !firstMatch) firstMatch = lyr;
      lyr.setStyle({ opacity: matches ? 1 : 0.25, fillOpacity: matches ? (state.choroplethEnabled ? 0.6 : 0.25) : 0.01 });
    });
    if (firstMatch) {
      try { map.fitBounds(firstMatch.getBounds(), { padding: [40, 40], maxZoom: 12 }); } catch (e) {}
    }
  }

  function renderDelta(el_delta, pct){
    if(!el_delta) return;
    if(pct === null || pct === undefined){
      el_delta.textContent = "";
      el_delta.className = "";
      return;
    }
    const sign = pct > 0 ? "▲" : (pct < 0 ? "▼" : "");
    el_delta.textContent = sign + Math.abs(pct) + "%";
    el_delta.className = "delta " + (pct > 0 ? "delta-up" : (pct < 0 ? "delta-dn" : "delta-flat"));
  }

  function drawDowHour(canvas, grid, dowLabels){
    if(!canvas || !grid) return;
    const ctx = canvas.getContext("2d");
    const w = canvas.clientWidth || 280;
    const h = canvas.clientHeight || 120;
    canvas.width = Math.floor(w * devicePixelRatio);
    canvas.height = Math.floor(h * devicePixelRatio);
    ctx.setTransform(devicePixelRatio,0,0,devicePixelRatio,0,0);
    ctx.clearRect(0,0,w,h);

    const padL = 28, padT = 6, padB = 14, padR = 4;
    const cols = 24;
    const rows = 7;
    const cellW = (w - padL - padR) / cols;
    const cellH = (h - padT - padB) / rows;

    // find max
    let maxV = 1;
    for(let d=0;d<rows;d++) for(let hh=0;hh<cols;hh++) if(grid[d][hh] > maxV) maxV = grid[d][hh];

    for(let d=0;d<rows;d++){
      // day label
      ctx.fillStyle = "#697386";
      ctx.font = `${Math.max(8, Math.floor(cellH * 0.55))}px Segoe UI,Arial,sans-serif`;
      ctx.textAlign = "right";
      ctx.fillText((dowLabels||["Вс","Пн","Вт","Ср","Чт","Пт","Сб"])[d], padL - 3, padT + d*cellH + cellH*0.72);
      for(let hh=0;hh<cols;hh++){
        const v = grid[d][hh] / maxV;
        const alpha = Math.max(0.04, v);
        ctx.fillStyle = `rgba(211,47,47,${alpha.toFixed(3)})`;
        const x = padL + hh*cellW + 1;
        const y = padT + d*cellH + 1;
        ctx.fillRect(x, y, Math.max(1, cellW-1.5), Math.max(1, cellH-1.5));
      }
    }
    // hour labels at bottom
    ctx.fillStyle = "#697386";
    ctx.font = `${Math.max(7, Math.floor(cellW * 0.7))}px Segoe UI,Arial,sans-serif`;
    ctx.textAlign = "center";
    for(let hh=0;hh<cols;hh+=4){
      ctx.fillText(String(hh).padStart(2,"0"), padL + hh*cellW + cellW/2, h - 2);
    }
  }

  function hourLabel(hr){
    const hh = (hr<10?("0"+hr):(""+hr));
    return hh + ":00 — " + hh + ":59";
  }

  function drawBars(canvas, values, opts){
    opts = opts || {};
    const topHour = opts.topHour ?? null;
    const minH = opts.minH ?? 0;
    const maxH = opts.maxH ?? 23;

    const ctx = canvas.getContext("2d");
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    canvas.width = Math.floor(w * devicePixelRatio);
    canvas.height = Math.floor(h * devicePixelRatio);
    ctx.setTransform(devicePixelRatio,0,0,devicePixelRatio,0,0);
    ctx.clearRect(0,0,w,h);

    const padL = 26, padR = 10, padT = 10, padB = 22;
    const plotW = w - padL - padR;
    const plotH = h - padT - padB;
    const maxV = Math.max(1, ...values);
    const n = values.length;
    const bw = plotW / n;

    canvas.__barsMeta = { padL, padT, plotW, plotH, bw, values, maxV, minH, maxH, topHour };

    for(let i=0;i<n;i++){
      const v = values[i] / maxV;
      const bh = Math.round(plotH * v);
      const x = padL + i*bw + 1;
      const y = padT + (plotH - bh);
      const isTop = (topHour !== null && i === topHour && values[i] > 0);
      ctx.fillStyle = isTop ? "rgba(211,47,47,0.98)" : "rgba(211,47,47,0.72)";
      ctx.fillRect(x, y, Math.max(2, bw-2), bh);
    }
  }

  el.miniChart.addEventListener("mousemove", (ev)=>{
    const meta = el.miniChart.__barsMeta;
    if(!meta) return;
    const r = el.miniChart.getBoundingClientRect();
    const x = ev.clientX - r.left;
    const y = ev.clientY - r.top;
    if (x < meta.padL || x > meta.padL + meta.plotW || y < meta.padT || y > meta.padT + meta.plotH){
      hideTip();
      return;
    }
    const idx = Math.max(0, Math.min(23, Math.floor((x - meta.padL) / meta.bw)));
    const v = meta.values[idx] || 0;
    const pct = Math.round((v / meta.maxV) * 100);
    const isInRange = (idx >= meta.minH && idx <= meta.maxH);
    const badge = isInRange
      ? '<span style="color:#22c55e;font-weight:900;">В ИНТЕРВАЛЕ</span>'
      : '<span style="color:#f59e0b;font-weight:900;">ВНЕ ИНТЕРВАЛА</span>';

    showTip(
      ev.clientX,
      ev.clientY,
      `<div><b>${idx}:00</b> <span style="opacity:.75;">(${hourLabel(idx)})</span></div>
       <div class="muted">${badge} • события</div>
       <div style="margin-top:6px;"><b>${v}</b> событий</div>
       <div class="muted">Интенсивность: ${pct}% от максимума</div>`
    );
  });
  el.miniChart.addEventListener("mouseleave", hideTip);

  function computeHotspots(rows){
    const grid = new Map();
    // Finer cells for oblast/city views, coarser for country-level.
    const cell = (state.currentFilter?.oblast && state.currentFilter.oblast !== "ALL") ? 0.03 : 0.12;
    for(const p of rows){
      const glat = Math.round(p.lat / cell) * cell;
      const glon = Math.round(p.lon / cell) * cell;
      const key = glat.toFixed(3)+","+glon.toFixed(3);
      if(!grid.has(key)) {
        grid.set(key, {
          lat: glat, lon: glon,
          count: 0,
          users: new Set(),
          uniqApprox: 0,
          hasApprox: false,
          sumLat: 0,
          sumLon: 0,
          w: 0,
        });
      }
      const o = grid.get(key);
      const w = (p.count !== undefined ? p.count : 1);
      o.count += w;
      o.sumLat += p.lat * w;
      o.sumLon += p.lon * w;
      o.w += w;

      // Raw points mode: exact unique users by iin set.
      if(p.iin) o.users.add(p.iin);
      // Aggregated mode: API returns approx uniq per point.
      if(p.uniq !== undefined && p.uniq !== null){
        o.hasApprox = true;
        o.uniqApprox += Number(p.uniq) || 0;
      }
    }
    const arr = Array.from(grid.values()).map(o => ({
      lat: o.w > 0 ? (o.sumLat / o.w) : o.lat,
      lon: o.w > 0 ? (o.sumLon / o.w) : o.lon,
      count: o.count,
      users: o.hasApprox ? Math.max(o.users.size, Math.round(o.uniqApprox)) : o.users.size,
      usersApprox: o.hasApprox,
    }));
    arr.sort((a,b)=> b.count - a.count);
    return arr.slice(0,5);
  }

  function renderHotspots(list){
    el.hsList.innerHTML = "";
    list.forEach((it, idx) => {
      const d = document.createElement("div");
      d.className = "hs";
      const hotspotKey = `${idx}_${Number(it.lat).toFixed(4)}_${Number(it.lon).toFixed(4)}`;
      d.dataset.hotspotKey = hotspotKey;
      const usersLabel = it.usersApprox ? `~${it.users}` : `${it.users}`;
      d.innerHTML = `
        <div>
          <div class="a">#${idx+1} горячая зона</div>
          <div class="b">${it.count} событий • ${usersLabel} пользователей</div>
        </div>
        <div class="c">${it.count}</div>
      `;
      d.onclick = ()=> {
        highlightHotspot(it, hotspotKey);
        map.setView([it.lat, it.lon], Math.max(map.getZoom(), 13), {animate:true});
      };
      el.hsList.appendChild(d);
    });
  }

  function highlightHotspot(it, hotspotKey){
    state.activeHotspotKey = hotspotKey;
    document.querySelectorAll("#hsList .hs").forEach((node) => {
      node.classList.toggle("active", node.dataset.hotspotKey === hotspotKey);
    });

    if(state.hotspotHighlightLayer){
      try{ map.removeLayer(state.hotspotHighlightLayer); }catch(e){}
      state.hotspotHighlightLayer = null;
    }

    const eventRadius = Math.max(450, Math.min(1600, Math.round(Math.sqrt(Number(it.count || 1)) * 170)));
    const glow = L.circle([it.lat, it.lon], {
      radius: eventRadius,
      color: "#f59e0b",
      weight: 2,
      opacity: 0.95,
      fillColor: "#fde68a",
      fillOpacity: 0.28,
    });
    const core = L.circleMarker([it.lat, it.lon], {
      radius: 10,
      color: "#ffffff",
      weight: 2,
      fillColor: "#f59e0b",
      fillOpacity: 0.95,
    });
    state.hotspotHighlightLayer = L.layerGroup([glow, core]).addTo(map);
  }

  function buildHeatData(rows, mode){
    if(mode === "unique"){
      // Aggregated points (low zoom): have uniq field but no iin — use uniq as weight
      if(rows.length > 0 && rows[0].uniq !== undefined){
        return rows.map(p => [p.lat, p.lon, Math.min(5, 0.2 + Math.log((p.uniq || 1) + 1))]);
      }
      // Raw points (high zoom): deduplicate by iin
      const seen = new Map();
      for(const p of rows){
        if(!p.iin) continue;
        if(!seen.has(p.iin)) seen.set(p.iin, p);
      }
      const result = Array.from(seen.values()).map(p => [p.lat, p.lon, 1.0]);
      return result.length > 0 ? result : rows.map(p => [p.lat, p.lon, 0.7]);
    }
    return rows.map(p => [p.lat, p.lon, p.count !== undefined ? Math.min(5, 0.2 + Math.log(p.count + 1)) : 0.7]);
  }

  function setClustersVisible(rows){
    const want = !!el.showClusters.checked;
    const ok = (typeof L !== "undefined" && typeof L.markerClusterGroup === "function");
    if(!ok) return;

    if(!state.clusterLayer){
      state.clusterLayer = L.markerClusterGroup({ showCoverageOnHover:false, chunkedLoading:true });
    }
    state.clusterLayer.clearLayers();
    if(!want){
      if(map.hasLayer(state.clusterLayer)) map.removeLayer(state.clusterLayer);
      return;
    }
    if(!map.hasLayer(state.clusterLayer)) map.addLayer(state.clusterLayer);

    const maxN = Math.min(rows.length, 12000);
    for(let i=0;i<maxN;i++){
      const p = rows[i];
      const marker = L.marker([p.lat, p.lon]);
      marker.bindPopup(
        `<b>Авторизация</b><br>ИИН: ${p.iin || "-"}<br>Час: ${p.hour ?? "-"}<br>Событий: ${p.count ?? 1}`
      );
      state.clusterLayer.addLayer(marker);
    }
  }

  function renderTopRegions(selectedOblast){
    if(state.topRegionsLayer){
      map.removeLayer(state.topRegionsLayer);
      state.topRegionsLayer = null;
    }
    if(!state.oblastsFc?.features?.length) return;
    if(selectedOblast && selectedOblast !== "ALL") return;

    const feats = state.oblastsFc.features;

    const baseStyle = { color:"#0f172a", weight:1, fillOpacity:0.02 };

    state.topRegionsLayer = L.geoJSON({type:"FeatureCollection", features: feats}, {
      pane: "oblastPane",
      renderer: L.canvas(),
      style: () => baseStyle,
      onEachFeature: (f, lyr) => {
        killFocus(lyr);
        const hoverStyle = { color:"#d32f2f", weight:2, fillOpacity:0.05 };
        lyr.on("mouseover", ()=> lyr.setStyle(hoverStyle));
        lyr.on("mouseout", ()=> lyr.setStyle(baseStyle));
        lyr.on("click", (e)=> {
          L.DomEvent.stopPropagation(e);
          const ob = f?.properties?.oblast_kk;
          if(ob){
            el.oblastSelect.value = ob;
            el.oblastSelect.dispatchEvent(new Event("change"));
          }
        });
        lyr.bindTooltip(f?.properties?.name_kk || f?.properties?.oblast_kk || "область", {sticky:true});
      }
    }).addTo(map);
  }

  async function renderDetailForOblast(ob, fitToBounds){
    if(state.detailLayer){
      map.removeLayer(state.detailLayer);
      state.detailLayer = null;
    }
    state.rayonsFc = null;

    if(ob === "ALL") return;
    if(!el.showDetails.checked) return;

    let fc = state.rayonCache.get(ob);
    if(!fc){
      fc = await fetchJson(`/api/rayons?oblast=${encodeURIComponent(ob)}`);
      state.rayonCache.set(ob, fc);
    }
    state.rayonsFc = fc;

    state.detailLayer = L.geoJSON(fc, {
      pane: "rayonPane",
      renderer: L.canvas(),
      style: { color:"#0f172a", weight:1, fillOpacity:0.03 },
      onEachFeature: (f, lyr) => {
        killFocus(lyr);
        lyr.bindTooltip(f?.properties?.name_kk || "район", {sticky:true});
        lyr.on("click", async (e)=>{
          L.DomEvent.stopPropagation(e);
          await onRayonClick(f);
        });
      }
    }).addTo(map);

    if(fitToBounds){
      try{ map.fitBounds(state.detailLayer.getBounds(), {padding:[20,20]}); }catch(e){}
    }
    if (state.choroplethEnabled) setTimeout(() => loadChoropleth(), 0);
  }

  async function onRayonClick(feature){
    const props = feature?.properties || {};
    const fullId = props.full_id;
    if(!fullId) return;
    state.selectedRayon = fullId;
    state.selectedRayonOblast = props.oblast_kk || null;
    await refreshSelectedRayonStats(props.name_kk || "Район");
  }

  function applyRayonStatsToSidebar(data, fallbackTitle){
    const hrs = data?.rayon?.hours || new Array(24).fill(0);
    let topH = 0;
    let topV = -1;
    for(let i=0;i<24;i++){
      if(hrs[i] > topV){ topV = hrs[i]; topH = i; }
    }

    el.sidebar.style.display = "block";
    el.sbSub.innerText = "Клик по району • KPI по фильтру";
    el.sbTitle.innerText = data?.rayon?.name_kk || fallbackTitle || "Район";
    el.kpiEvents.innerText = String(data?.rayon?.events ?? 0);
    el.kpiUsers.innerText = String(data?.rayon?.users ?? 0);
    el.kpiTopHour.innerText = topV > 0 ? fmtHour(topH) : "—";
    el.kpiMode.innerText = modeLabel(state.currentFilter.mode);
    el.kpiOblastEvents.innerText = String(data?.oblast?.events ?? 0);
    el.kpiOblastUsers.innerText = String(data?.oblast?.users ?? 0);
    if((data?.rayon?.events ?? 0) === 0){
      el.sbSub.innerText = "По выбранному району нет данных в текущем фильтре";
    }

    drawBars(el.miniChart, hrs, {
      topHour: topH,
      minH: state.currentFilter.minH,
      maxH: state.currentFilter.maxH,
    });
  }

  async function refreshSelectedRayonStats(fallbackTitle){
    if(!state.selectedRayon) return;
    const fullId = state.selectedRayon;
    const reqId = ++state.lastRayonRequestId;
    const q = new URLSearchParams({
      full_id: fullId,
      min_h: String(state.currentFilter.minH),
      max_h: String(state.currentFilter.maxH),
      period: state.currentFilter.period,
    });
    if(state.currentFilter.anchorDate) q.set("anchor_date", state.currentFilter.anchorDate);
    if(state.currentFilter.period === "custom"){
      if(state.currentFilter.startDate) q.set("start_date", state.currentFilter.startDate);
      if(state.currentFilter.endDate) q.set("end_date", state.currentFilter.endDate);
    }
    const data = await fetchJson(`/api/stats/rayon?${q.toString()}`);
    if(reqId !== state.lastRayonRequestId || state.selectedRayon !== fullId) return;
    applyRayonStatsToSidebar(data, fallbackTitle);
    await reloadPointsOnly();
    await reloadDashboard();
  }

  function syncFilterFromUi(){
    const vals = el.slider.noUiSlider.get();
    const minH = parseInt(vals[0], 10);
    const maxH = parseInt(vals[1], 10);
    const mode = getMode();
    const oblast = el.oblastSelect.value || "ALL";
    const period = el.periodSelect.value || "week";
    const anchorDate = (el.anchorDate?.value || "").trim();
    let startDate = (el.periodStart?.value || "").trim();
    let endDate = (el.periodEnd?.value || "").trim();
    if(period === "custom"){
      const t = todayIso();
      if(!endDate) endDate = t;
      if(!startDate) startDate = shiftIso(endDate, -6);
      if(el.periodStart) el.periodStart.value = startDate;
      if(el.periodEnd) el.periodEnd.value = endDate;
    }
    state.currentFilter = {minH, maxH, mode, oblast, period, anchorDate, startDate, endDate};
    state.detailsShown = !!el.showDetails.checked;
    el.timeVal.innerText = fmtHour(minH) + " — " + fmtHour(maxH);
  }

  function updateHeatLayer(rows){
    if(el.showClusters.checked){
      if(state.heatLayer && map.hasLayer(state.heatLayer)){
        map.removeLayer(state.heatLayer);
      }
      return;
    }
    const heatData = buildHeatData(rows, state.currentFilter.mode);
    const opts = {
      radius: 34,
      blur: 26,
      maxZoom: 17,
      gradient: {0.2:"#2563eb", 0.55:"#22c55e", 0.78:"#facc15", 1.0:"#ef4444"},
    };
    const mustCreate = !state.heatLayer || !state.heatLayer._map;
    if(mustCreate){
      state.heatLayer = L.heatLayer(heatData, opts).addTo(map);
      return;
    }
    if(!map.hasLayer(state.heatLayer)){
      // Layer object exists but is detached after cluster toggles.
      state.heatLayer.addTo(map);
    }
    try{
      if(typeof state.heatLayer.setLatLngs === "function"){
        state.heatLayer.setLatLngs(heatData);
      } else {
        throw new Error("setLatLngs not supported");
      }
    }catch(e){
      try{ if(map.hasLayer(state.heatLayer)) map.removeLayer(state.heatLayer); }catch(_){}
      state.heatLayer = L.heatLayer(heatData, opts).addTo(map);
    }
  }

  async function reloadPointsOnly(){
    const requestId = ++state.lastRequestId;
    const { minH, maxH, oblast, period, anchorDate, startDate, endDate } = state.currentFilter;

    const b = map.getBounds();
    const q = new URLSearchParams({
      min_lat: String(b.getSouth()),
      max_lat: String(b.getNorth()),
      min_lon: String(b.getWest()),
      max_lon: String(b.getEast()),
      zoom: String(map.getZoom()),
      min_h: String(minH),
      max_h: String(maxH),
      oblast,
      period,
    });
    if(anchorDate) q.set("anchor_date", anchorDate);
    if(period === "custom"){
      if(startDate) q.set("start_date", startDate);
      if(endDate) q.set("end_date", endDate);
    }
    if(state.selectedRayon && (oblast === "ALL" || state.selectedRayonOblast === oblast)){
      q.set("rayon_id", state.selectedRayon);
    }

    const rows = await fetchJson(`/api/points?${q.toString()}`);
    if(requestId !== state.lastRequestId) return;

    state.lastFiltered = rows;
    updateHeatLayer(rows);
    renderHotspots(computeHotspots(rows));
    setClustersVisible(rows);
  }

  function renderDashboard(payload){
    const k = payload?.kpi || {};
    const hrs = Array.isArray(payload?.hours) ? payload.hours : new Array(24).fill(0);
    const q = payload?.quality || {};
    const top = payload?.top_rayons || [];

    if(el.dbEvents) animateNum(el.dbEvents, k.events || 0);
    if(el.dbUsers) animateNum(el.dbUsers, k.users || 0);
    if(el.dbRayons) animateNum(el.dbRayons, k.active_rayons || 0);
    if(el.dbRayonPct) el.dbRayonPct.innerText = `${q.rayon_tagged_pct ?? 0}%`;
    if(el.dbPeriod) el.dbPeriod.innerText = payload?.period_used || "-";
    if(el.dbCoverage){
      const totalR = k.total_rayons || 0;
      const activeR = k.active_rayons || 0;
      if(totalR > 0) el.dbCoverage.textContent = Math.round(activeR / totalR * 100) + "%";
      else el.dbCoverage.textContent = "—";
    }
    if(el.dbEventsDelta) el.dbEventsDelta.textContent = "";
    if(el.dbUsersDelta) el.dbUsersDelta.textContent = "";
    el.sidebar.style.display = "block";

    if(!state.selectedRayon){
      const scope = state.currentFilter.oblast === "ALL" ? "Весь Казахстан" : state.currentFilter.oblast;
      el.sbTitle.innerText = scope;
      el.sbSub.innerText = "Общая аналитика по текущему фильтру";
      el.kpiEvents.innerText = fmtNum(k.events);
      el.kpiUsers.innerText = fmtNum(k.users);
      const topHour = (k.top_hour === null || k.top_hour === undefined) ? null : Number(k.top_hour);
      el.kpiTopHour.innerText = Number.isFinite(topHour) ? fmtHour(topHour) : "—";
      el.kpiMode.innerText = modeLabel(state.currentFilter.mode);
      el.kpiOblastEvents.innerText = fmtNum(k.events);
      el.kpiOblastUsers.innerText = fmtNum(k.users);
      drawBars(el.miniChart, hrs, {
        topHour: Number.isFinite(topHour) ? topHour : null,
        minH: state.currentFilter.minH,
        maxH: state.currentFilter.maxH,
      });
    }

    el.topRayonsList.innerHTML = "";
    top.slice(0, 5).forEach((r, idx) => {
      const d = document.createElement("div");
      d.className = "row-lite";
      d.innerHTML = `
        <div>
          <div><b>#${idx+1} ${r.rayon_name || r.rayon_id}</b></div>
          <div class="m">${r.oblast_kk || "-"} • ${fmtNum(r.users)} пользователей</div>
        </div>
        <div><b>${fmtNum(r.events)}</b></div>
      `;
      d.onclick = () => {
        if(r.oblast_kk){
          el.oblastSelect.value = r.oblast_kk;
          el.oblastSelect.dispatchEvent(new Event("change"));
        }
      };
      el.topRayonsList.appendChild(d);
    });

    el.dataQuality.innerHTML = `
      <div class="row-lite"><span>Район размечен</span><b>${q.rayon_tagged_pct ?? 0}%</b></div>
      <div class="row-lite"><span>Область размечена</span><b>${q.oblast_tagged_pct ?? 0}%</b></div>
      <div class="row-lite"><span>ИИН заполнен</span><b>${q.iin_filled_pct ?? 0}%</b></div>
    `;
  }

  async function reloadDashboard(){
    const requestId = ++state.lastDashboardRequestId;
    const { minH, maxH, oblast, period, anchorDate, startDate, endDate } = state.currentFilter;
    const q = new URLSearchParams({
      min_h: String(minH),
      max_h: String(maxH),
      oblast,
      period,
    });
    if(anchorDate) q.set("anchor_date", anchorDate);
    if(period === "custom"){
      if(startDate) q.set("start_date", startDate);
      if(endDate) q.set("end_date", endDate);
    }
    if(state.selectedRayon && (oblast === "ALL" || state.selectedRayonOblast === oblast)){
      q.set("rayon_id", state.selectedRayon);
    }
    const payload = await fetchJson(`/api/dashboard?${q.toString()}`);
    if(requestId !== state.lastDashboardRequestId) return;
    const k = payload?.kpi || {};
    if(state.selectedRayon && Number(k.events || 0) === 0){
      state.selectedRayon = null;
      state.selectedRayonOblast = null;
      state.lastRayonRequestId++;
      await reloadPointsOnly();
      await reloadDashboard();
      return;
    }
    renderDashboard(payload);

    // Fetch trend (WoW delta) in parallel
    try {
      const trendQ = new URLSearchParams({ oblast, period });
      if(anchorDate) trendQ.set("anchor_date", anchorDate);
      if(period === "custom"){
        if(startDate) trendQ.set("start_date", startDate);
        if(endDate) trendQ.set("end_date", endDate);
      }
      if(state.selectedRayon && (oblast === "ALL" || state.selectedRayonOblast === oblast)){
        trendQ.set("rayon_id", state.selectedRayon);
      }
      const trend = await fetchJson(`/api/stats/trend?${trendQ.toString()}`);
      if(trend?.available){
        renderDelta(el.dbEventsDelta, trend?.delta?.events_pct ?? null);
        renderDelta(el.dbUsersDelta, trend?.delta?.users_pct ?? null);
      }
    } catch(e){ /* trend is optional */ }

    // Fetch devices breakdown
    try {
      const devQ = new URLSearchParams({ oblast, period });
      if(anchorDate) devQ.set("anchor_date", anchorDate);
      if(period === "custom"){
        if(startDate) devQ.set("start_date", startDate);
        if(endDate) devQ.set("end_date", endDate);
      }
      if(state.selectedRayon && (oblast === "ALL" || state.selectedRayonOblast === oblast)){
        devQ.set("rayon_id", state.selectedRayon);
      }
      const devData = await fetchJson(`/api/stats/devices?${devQ.toString()}`);
      if(devData?.available && devData.items?.length > 0){
        if(el.devicesTitle) el.devicesTitle.style.display = "";
        if(el.devicesList){
          el.devicesList.innerHTML = devData.items.slice(0,8).map(d =>
            `<div class="row-lite"><div><div><b>${d.device_type}</b></div><div class="m">${d.users.toLocaleString()} польз.</div></div><div><b>${d.pct}%</b></div></div>`
          ).join("");
        }
      } else {
        if(el.devicesTitle) el.devicesTitle.style.display = "none";
        if(el.devicesList) el.devicesList.innerHTML = "";
      }
    } catch(e){ /* devices optional */ }

    if (state.choroplethEnabled) await loadChoropleth();
  }

  async function refreshLayers(forceFitBounds){
    const ob = state.currentFilter.oblast;
    renderTopRegions(ob);
    await renderDetailForOblast(ob, forceFitBounds);
  }

  function debounce(fn, ms){
    let t = null;
    return function(){
      clearTimeout(t);
      t = setTimeout(fn, ms);
    };
  }

  async function init(){
    el.sbClose.onclick = ()=> { el.sidebar.style.display = "none"; };
    if(el.periodStart && !el.periodStart.value){
      const t = todayIso();
      el.periodEnd.value = t;
      el.periodStart.value = shiftIso(t, -6);
    }
    syncCustomPeriodVisibility();
    applyTopbarCollapsed(localStorage.getItem("geo_topbar_collapsed") === "1");
    applyDarkMode(localStorage.getItem("geo_dark_mode") === "1");
    el.topbarToggle?.addEventListener("click", ()=>{
      const next = !state.topbarCollapsed;
      applyTopbarCollapsed(next);
      localStorage.setItem("geo_topbar_collapsed", next ? "1" : "0");
    });

    noUiSlider.create(el.slider, {
      start: [9, 18],
      connect: true,
      step: 1,
      range: { min: 0, max: 23 },
    });

    state.oblastsFc = await fetchJson("/api/oblasts");
    const names = Array.from(
      new Set(
        state.oblastsFc.features
          .map(f => (f?.properties?.oblast_kk || "").trim())
          .filter(Boolean)
      )
    ).sort((a,b)=> String(a).localeCompare(String(b), "kk"));

    el.oblastSelect.innerHTML =
      `<option value="ALL">Весь Казахстан</option>` +
      names.map(n => `<option value="${String(n).replaceAll('"','&quot;')}">${n}</option>`).join("");

    el.slider.noUiSlider.on("set", async ()=>{
      syncFilterFromUi();
      if(state.selectedRayon){
        await refreshSelectedRayonStats();
        return;
      }
      await reloadPointsOnly();
      await reloadDashboard();
    });
    el.modeEvents.addEventListener("change", async ()=>{
      syncFilterFromUi();
      updateHeatLayer(state.lastFiltered);
    });
    el.modeUnique.addEventListener("change", async ()=>{
      syncFilterFromUi();
      updateHeatLayer(state.lastFiltered);
    });
    el.oblastSelect.addEventListener("change", async ()=>{
      stopPlayback();
      syncFilterFromUi();
      state.selectedRayon = null;
      state.selectedRayonOblast = null;
      state.lastRayonRequestId++;
      const changed = state.lastOblast !== state.currentFilter.oblast;
      state.lastOblast = state.currentFilter.oblast;
      await refreshLayers(changed);
      await reloadPointsOnly();
      await reloadDashboard();
    });
    el.showDetails.addEventListener("change", async ()=>{
      stopPlayback();
      syncFilterFromUi();
      if(!state.detailsShown){
        state.selectedRayon = null;
        state.selectedRayonOblast = null;
        state.lastRayonRequestId++;
      }
      await refreshLayers(false);
      await reloadPointsOnly();
      await reloadDashboard();
    });
    el.periodSelect.addEventListener("change", async ()=>{
      stopPlayback();
      syncCustomPeriodVisibility();
      syncFilterFromUi();
      if(state.selectedRayon){
        await refreshSelectedRayonStats();
        return;
      }
      await reloadPointsOnly();
      await reloadDashboard();
    });
    el.periodStart?.addEventListener("change", async ()=>{
      stopPlayback();
      syncFilterFromUi();
      if(state.selectedRayon){
        await refreshSelectedRayonStats();
        return;
      }
      await reloadPointsOnly();
      await reloadDashboard();
    });
    el.periodEnd?.addEventListener("change", async ()=>{
      stopPlayback();
      syncFilterFromUi();
      if(state.selectedRayon){
        await refreshSelectedRayonStats();
        return;
      }
      await reloadPointsOnly();
      await reloadDashboard();
    });
    el.anchorDate?.addEventListener("change", async ()=>{
      syncFilterFromUi();
      if(state.selectedRayon){
        await refreshSelectedRayonStats();
        return;
      }
      await reloadPointsOnly();
      await reloadDashboard();
    });
    el.showClusters.addEventListener("change", ()=>{
      updateHeatLayer(state.lastFiltered);
      setClustersVisible(state.lastFiltered);
    });

    if (el.showChoropleth) {
      el.showChoropleth.addEventListener("change", async () => {
        state.choroplethEnabled = !!el.showChoropleth.checked;
        if (state.choroplethEnabled) {
          await loadChoropleth();
        } else {
          resetChoropleth();
        }
      });
    }

    if (el.darkModeBtn) {
      el.darkModeBtn.addEventListener("click", () => {
        applyDarkMode(!state.darkMode);
      });
    }

    if (el.rayonSearch) {
      el.rayonSearch.addEventListener("input", () => {
        applyRayonSearch(el.rayonSearch.value);
      });
    }

    el.hourPlayBtn?.addEventListener("click", ()=>{
      if(isPlaying()) stopPlayback();
      else startPlayback();
    });

    const onMapChanged = debounce(async ()=>{
      syncFilterFromUi();
      await reloadPointsOnly();
    }, 280);
    map.on("moveend", onMapChanged);
    map.on("zoomend", onMapChanged);
    map.on("click", async ()=>{
      if(!state.selectedRayon) return;
      state.selectedRayon = null;
      state.selectedRayonOblast = null;
      state.lastRayonRequestId++;
      await reloadPointsOnly();
      await reloadDashboard();
    });

    syncLayoutOffsets();
    const topbar = document.getElementById("topbar");
    if(typeof ResizeObserver !== "undefined" && topbar){
      const ro = new ResizeObserver(() => syncLayoutOffsets());
      ro.observe(topbar);
    }

    syncFilterFromUi();
    // Chart mode toggle
    if(el.chartToggleBtn){
      el.chartToggleBtn.addEventListener("click", async ()=>{
        state.chartMode = (state.chartMode === "hour") ? "dowhour" : "hour";
        el.chartToggleBtn.textContent = state.chartMode === "hour" ? "7×24" : "24ч";
        if(el.miniChart) el.miniChart.style.display = state.chartMode === "hour" ? "" : "none";
        if(el.dowHourChart) el.dowHourChart.style.display = state.chartMode === "dowhour" ? "" : "none";
        if(state.chartMode === "dowhour"){
          try{
            const { minH, maxH, oblast, period, anchorDate, startDate, endDate } = state.currentFilter;
            const q = new URLSearchParams({ oblast, period });
            if(anchorDate) q.set("anchor_date", anchorDate);
            if(period === "custom"){
              if(startDate) q.set("start_date", startDate);
              if(endDate) q.set("end_date", endDate);
            }
            if(state.selectedRayon) q.set("rayon_id", state.selectedRayon);
            const dh = await fetchJson(`/api/stats/dow_hour?${q.toString()}`);
            if(dh?.grid && el.dowHourChart){
              setTimeout(()=> drawDowHour(el.dowHourChart, dh.grid, dh.dow_labels), 50);
            }
          }catch(e){}
        }
      });
    }
    state.lastOblast = state.currentFilter.oblast;
    await refreshLayers(false);
    await reloadPointsOnly();
    await reloadDashboard();
  }

  init().catch(err => {
    console.error(err);
    alert("Ошибка инициализации: " + err.message);
  });
})();






