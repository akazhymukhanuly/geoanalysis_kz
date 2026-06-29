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
    showChoropleth: document.getElementById("showChoropleth"),
    showBehavior: document.getElementById("showBehavior"),
    darkModeBtn: document.getElementById("darkModeBtn"),
    dbCoverage: document.getElementById("dbCoverage"),
    rayonSearch: document.getElementById("rayonSearch"),
    choroplethLegend: document.getElementById("choroplethLegend"),
    behaviorLegend: document.getElementById("behaviorLegend"),
    compareSection: document.getElementById("compareSection"),
    compareCanvas: document.getElementById("compareChart"),
    compareLegend: document.getElementById("compareLegend"),
    anomalySection: document.getElementById("anomalySection"),
    anomalyList: document.getElementById("anomalyList"),
    anomalyCount: document.getElementById("anomalyCount"),
    layerAuth: document.getElementById("layerAuth"),
    layerTransfer: document.getElementById("layerTransfer"),
    layerBoth: document.getElementById("layerBoth"),
    purposeFilterPill: document.getElementById("purposeFilterPill"),
    purposeSelect: document.getElementById("purposeSelect"),
    dbTrCountCard: document.getElementById("dbTrCountCard"),
    dbTrVolCard: document.getElementById("dbTrVolCard"),
    dbTrAvgCard: document.getElementById("dbTrAvgCard"),
    dbTrUsersCard: document.getElementById("dbTrUsersCard"),
    dbTrCount: document.getElementById("dbTrCount"),
    dbTrVol: document.getElementById("dbTrVol"),
    dbTrAvg: document.getElementById("dbTrAvg"),
    dbTrUsers: document.getElementById("dbTrUsers"),
    transferStatsSection: document.getElementById("transferStatsSection"),
    kpiTrCount: document.getElementById("kpiTrCount"),
    kpiTrVol: document.getElementById("kpiTrVol"),
    kpiTrAvg: document.getElementById("kpiTrAvg"),
    kpiTrConv: document.getElementById("kpiTrConv"),
    purposeBreakdown: document.getElementById("purposeBreakdown"),
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
    behaviorEnabled: false,
    behaviorData: {},
    anomalyLayer: null,
    behaviorGridLayer: null,
    transferLayer: null,
    layerMode: "both",
    transferHeatLayer: null,
    transferDash: null,
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
  map.getPane("rayonPane").style.pointerEvents = "auto";

  // Persistent renderers — created once, reused across oblast switches.
  // Creating a new L.canvas() each time leaves orphaned map event listeners
  // that conflict with the new renderer and break rayon hover/click detection.
  const _oblastRenderer = L.canvas({ pane: "oblastPane" });
  const _rayonRenderer  = L.canvas({ pane: "rayonPane" });

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
      refreshDetailTop();
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

  // ---- Period Comparison Chart ----
  function drawCompareChart(canvas, curDays, prevDays) {
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const w = canvas.clientWidth || 260;
    const h = canvas.clientHeight || 80;
    canvas.width = Math.floor(w * devicePixelRatio);
    canvas.height = Math.floor(h * devicePixelRatio);
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const n = Math.max(curDays.length, 1);
    const padL = 6, padR = 6, padT = 6, padB = 18;
    const plotW = w - padL - padR;
    const plotH = h - padT - padB;
    const slotW = plotW / n;
    const barW = Math.max(2, Math.floor((slotW - 3) / 2));
    const allVals = [...curDays.map(d => d.events), ...prevDays.map(d => d.events)];
    const maxV = Math.max(1, ...allVals);

    canvas.__cmpMeta = { padL, padT, plotW, plotH, slotW, barW, curDays, prevDays, maxV, n };

    for (let i = 0; i < n; i++) {
      const cur = curDays[i]?.events || 0;
      const prev = prevDays[i]?.events || 0;
      const x = padL + i * slotW;
      const bh_cur = Math.round((cur / maxV) * plotH);
      ctx.fillStyle = "rgba(211,47,47,0.85)";
      ctx.fillRect(x + 1, padT + plotH - bh_cur, barW, Math.max(1, bh_cur));
      const bh_prev = Math.round((prev / maxV) * plotH);
      ctx.fillStyle = "rgba(100,116,139,0.55)";
      ctx.fillRect(x + barW + 2, padT + plotH - bh_prev, barW, Math.max(1, bh_prev));
    }

    ctx.fillStyle = "#697386";
    ctx.font = `${Math.max(7, Math.floor(Math.min(slotW, 10)))}px Segoe UI,Arial,sans-serif`;
    ctx.textAlign = "center";
    const step = Math.max(1, Math.round(n / 7));
    for (let i = 0; i < n; i += step) {
      const d = curDays[i]?.date || "";
      ctx.fillText(d.slice(5), padL + i * slotW + slotW / 2, h - 3);
    }
  }

  if (el.compareCanvas) {
    el.compareCanvas.addEventListener("mousemove", (ev) => {
      const m = el.compareCanvas.__cmpMeta;
      if (!m) return;
      const r = el.compareCanvas.getBoundingClientRect();
      const x = ev.clientX - r.left - m.padL;
      if (x < 0 || x > m.plotW) { hideTip(); return; }
      const i = Math.max(0, Math.min(m.n - 1, Math.floor(x / m.slotW)));
      const cur = m.curDays[i] || {};
      const prev = m.prevDays[i] || {};
      showTip(ev.clientX, ev.clientY,
        `<div><b>${cur.date || ""}</b></div>
         <div><span style="color:#d32f2f;">▮ Текущий:</span> <b>${(cur.events || 0).toLocaleString()}</b></div>
         <div><span style="color:#64748b;">▮ Предыдущий: ${prev.date || ""}</span> <b>${(prev.events || 0).toLocaleString()}</b></div>`
      );
    });
    el.compareCanvas.addEventListener("mouseleave", hideTip);
  }

  async function loadPeriodCompare() {
    const { oblast, period, anchorDate, startDate, endDate } = state.currentFilter;
    try {
      const q = new URLSearchParams({ oblast, period });
      if (anchorDate) q.set("anchor_date", anchorDate);
      if (period === "custom") {
        if (startDate) q.set("start_date", startDate);
        if (endDate) q.set("end_date", endDate);
      }
      const data = await fetchJson(`/api/stats/period_compare?${q}`);
      if (!data.available) { if (el.compareSection) el.compareSection.style.display = "none"; return; }
      if (el.compareSection) el.compareSection.style.display = "";
      if (el.compareLegend) {
        el.compareLegend.innerHTML =
          `<span class="cmp-cur">▮ ${data.cur_range}</span><span class="cmp-prev">▮ ${data.prev_range}</span>`;
      }
      setTimeout(() => drawCompareChart(el.compareCanvas, data.current, data.previous), 30);
    } catch (e) {
      if (el.compareSection) el.compareSection.style.display = "none";
    }
  }

  // ---- Anomalies ----
  async function loadAnomalies() {
    if (!el.anomalySection || !el.anomalyList) return;
    const { oblast, period, anchorDate, startDate, endDate } = state.currentFilter;
    try {
      const q = new URLSearchParams({ oblast, period });
      if (anchorDate) q.set("anchor_date", anchorDate);
      if (period === "custom") {
        if (startDate) q.set("start_date", startDate);
        if (endDate) q.set("end_date", endDate);
      }
      const data = await fetchJson(`/api/stats/anomalies?${q}`);
      if (state.anomalyLayer) { try { map.removeLayer(state.anomalyLayer); } catch (_) {} state.anomalyLayer = null; }
      if (!data.available || !data.anomalies?.length) {
        el.anomalySection.style.display = "none";
        return;
      }
      el.anomalySection.style.display = "";
      if (el.anomalyCount) el.anomalyCount.textContent = `(${data.anomalies.length})`;
      el.anomalyList.innerHTML = "";

      const markerGroup = L.layerGroup().addTo(map);
      state.anomalyLayer = markerGroup;
      const anomalyIds = new Set(data.anomalies.map(a => a.rayon_id));

      if (state.detailLayer) {
        state.detailLayer.eachLayer(lyr => {
          const fid = lyr.feature?.properties?.full_id;
          if (!anomalyIds.has(fid)) return;
          const a = data.anomalies.find(x => x.rayon_id === fid);
          if (!a) return;
          const isHigh = a.severity === "high";
          if (!state.choroplethEnabled && !state.behaviorEnabled) {
            lyr.setStyle({ color: isHigh ? "#ef4444" : "#f59e0b", weight: isHigh ? 3 : 2, fillColor: isHigh ? "#fca5a5" : "#fde68a", fillOpacity: 0.3 });
          }
          try {
            const center = lyr.getBounds().getCenter();
            L.circleMarker([center.lat, center.lng], {
              radius: isHigh ? 11 : 8, color: isHigh ? "#ef4444" : "#f59e0b", weight: 2,
              fillColor: isHigh ? "#ef4444" : "#f59e0b", fillOpacity: 0.75,
            }).bindTooltip(`⚠ ${a.rayon_name}: ${a.reasons[0]}`, { sticky: true }).addTo(markerGroup);
          } catch (_) {}
        });
      }

      data.anomalies.forEach(a => {
        const isHigh = a.severity === "high";
        const d = document.createElement("div");
        d.className = "anomaly-item " + (isHigh ? "anomaly-high" : "anomaly-medium");
        d.innerHTML = `<div class="anomaly-row">
          <div style="flex:1;">
            <div><b>${isHigh ? "🔴" : "🟡"} ${a.rayon_name}</b></div>
            <div class="m">${a.oblast_kk} • ${a.events.toLocaleString()} событий • ${a.users.toLocaleString()} польз.</div>
            ${a.reasons.map(rs => `<div class="anomaly-reason">⚠ ${rs}</div>`).join("")}
          </div>
        </div>`;
        el.anomalyList.appendChild(d);
      });
      refreshDetailTop();
    } catch (e) {
      if (el.anomalySection) el.anomalySection.style.display = "none";
    }
  }

  // ---- Behavior Clusters ----
  function _behaviorColor(pattern) {
    if (pattern === "work") return "#16a34a";
    if (pattern === "home") return "#2563eb";
    if (pattern === "transit") return "#d97706";
    return "#64748b";
  }

  function applyBehaviorClusters() {
    if (!state.detailLayer) return;
    const labels = { work: "Рабочая зона", home: "Жилая зона", transit: "Транзит", mixed: "Смешанная" };
    state.detailLayer.eachLayer(lyr => {
      const fid = lyr.feature?.properties?.full_id;
      const d = state.behaviorData[fid];
      if (!d) return;
      const color = _behaviorColor(d.pattern);
      lyr.setStyle({ fillColor: color, fillOpacity: 0.45, color: "#0f172a", weight: 1 });
      lyr.bindTooltip(
        `<b>${lyr.feature?.properties?.name_kk || "Район"}</b><br>${labels[d.pattern] || d.pattern}<br>Рабочих: ${d.work_pct}% • Ночных: ${d.night_pct}% • Транзит: ${d.transit_pct}%<br>${d.events.toLocaleString()} событий`,
        { sticky: true }
      );
    });
    if (el.behaviorLegend) el.behaviorLegend.style.display = "";
  }

  function resetBehaviorClusters() {
    clearBehaviorGrid();
    if (el.behaviorLegend) el.behaviorLegend.style.display = "none";
    if (!state.detailLayer) return;
    state.detailLayer.eachLayer(lyr => {
      lyr.setStyle({ color: "#0f172a", weight: 1, fillOpacity: 0.03, fillColor: "#ffffff" });
      lyr.bindTooltip(lyr.feature?.properties?.name_kk || "район", { sticky: true });
    });
  }

  async function loadBehaviorClusters() {
    if (!state.behaviorEnabled) { resetBehaviorClusters(); return; }
    const { oblast, period, anchorDate, startDate, endDate } = state.currentFilter;
    if (oblast && oblast !== "ALL") {
      // Sub-rayon grid: reset polygon colors, show point grid instead
      if (state.detailLayer) {
        state.detailLayer.eachLayer(lyr => {
          lyr.setStyle({ color: "#0f172a", weight: 1, fillOpacity: 0.03, fillColor: "#ffffff" });
          lyr.bindTooltip(lyr.feature?.properties?.name_kk || "район", { sticky: true });
        });
      }
      await loadBehaviorGrid();
      return;
    }
    // ALL Kazakhstan: rayon-level coloring
    clearBehaviorGrid();
    if (!state.detailLayer) return;
    try {
      const q = new URLSearchParams({ oblast, period });
      if (anchorDate) q.set("anchor_date", anchorDate);
      if (period === "custom") {
        if (startDate) q.set("start_date", startDate);
        if (endDate) q.set("end_date", endDate);
      }
      state.behaviorData = await fetchJson(`/api/stats/behavior_clusters?${q}`);
      applyBehaviorClusters();
    } catch (e) { console.warn("[behavior]", e); }
  }

  function clearBehaviorGrid() {
    if (state.behaviorGridLayer) {
      try { map.removeLayer(state.behaviorGridLayer); } catch (_) {}
      state.behaviorGridLayer = null;
    }
  }

  async function loadBehaviorGrid() {
    clearBehaviorGrid();
    const { oblast, period, anchorDate, startDate, endDate } = state.currentFilter;
    try {
      const q = new URLSearchParams({ oblast, period });
      if (anchorDate) q.set("anchor_date", anchorDate);
      if (period === "custom") {
        if (startDate) q.set("start_date", startDate);
        if (endDate) q.set("end_date", endDate);
      }
      const data = await fetchJson(`/api/stats/behavior_grid?${q}`);
      if (!data.length) return;

      const maxEv = Math.max(...data.map(d => d.events));
      const byPattern = { work: [], home: [], transit: [], mixed: [] };
      for (const d of data) {
        const w = Math.min(1.0, 0.15 + Math.sqrt(d.events / maxEv) * 0.85);
        (byPattern[d.pattern] || byPattern.mixed).push([d.lat, d.lon, w]);
      }

      const gradients = {
        work:    { 0.1: "#86efac", 0.4: "#22c55e", 1.0: "#15803d" },
        home:    { 0.1: "#93c5fd", 0.4: "#3b82f6", 1.0: "#1d4ed8" },
        transit: { 0.1: "#fde68a", 0.4: "#f59e0b", 1.0: "#b45309" },
      };

      const zoom = map.getZoom();
      const radius = Math.max(25, Math.min(60, zoom * 4));
      const blur = Math.max(20, Math.min(50, zoom * 3));
      const heatLayers = [];
      for (const [pattern, pts] of Object.entries(byPattern)) {
        if (!pts.length || pattern === "mixed") continue;
        // Normalize each pattern independently so rare patterns are still visible
        const patMax = Math.max(...pts.map(p => p[2]));
        const normPts = pts.map(([lat, lon, w]) => [lat, lon, w / patMax]);
        heatLayers.push(L.heatLayer(normPts, { radius, blur, maxZoom: 17, max: 1.0, gradient: gradients[pattern] }));
      }

      state.behaviorGridLayer = L.layerGroup(heatLayers).addTo(map);
      if (el.behaviorLegend) el.behaviorLegend.style.display = "";
      // Prevent heat canvases from intercepting pointer events on rayons below
      setTimeout(() => {
        if (state.behaviorGridLayer) {
          state.behaviorGridLayer.eachLayer(l => {
            if (l._canvas) l._canvas.style.pointerEvents = "none";
          });
        }
        refreshDetailTop();
      }, 60);
    } catch (e) { console.warn("[behavior_grid]", e); }
  }

  function refreshDetailTop() {
    if (!state.detailLayer) return;
    try { state.detailLayer.bringToFront(); } catch (_) {}
  }

  function setClustersVisible(rows){
    const want = !!el.showClusters.checked;
    const ok = (typeof L !== "undefined" && typeof L.markerClusterGroup === "function");
    if(!ok) return;

    // Destroy and recreate each time to avoid residual event handlers
    if(state.clusterLayer){
      try { if(map.hasLayer(state.clusterLayer)) map.removeLayer(state.clusterLayer); } catch(_){}
      state.clusterLayer = null;
    }
    if(!want){
      refreshDetailTop();
      return;
    }

    state.clusterLayer = L.markerClusterGroup({ showCoverageOnHover:false, chunkedLoading:true });
    map.addLayer(state.clusterLayer);

    const maxN = Math.min(rows.length, 12000);
    for(let i=0;i<maxN;i++){
      const p = rows[i];
      const marker = L.marker([p.lat, p.lon]);
      marker.bindPopup(
        `<b>Авторизация</b><br>ИИН: ${p.iin || "-"}<br>Час: ${p.hour ?? "-"}<br>Событий: ${p.count ?? 1}`
      );
      state.clusterLayer.addLayer(marker);
    }
    refreshDetailTop();
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
      renderer: _oblastRenderer,
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
      renderer: _rayonRenderer,
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
    if (state.behaviorEnabled) setTimeout(() => loadBehaviorClusters(), 0);
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

    renderTransferSidebarSection(state.selectedRayon);
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

  // ---- Transfer dot overlay (raw points only, shown when zoom >= 13) ----
  function _removeTransferLayer(){
    if(state.transferLayer){
      try{ map.removeLayer(state.transferLayer); }catch(_){}
      state.transferLayer = null;
    }
  }

  function renderTransferDots(rows){
    _removeTransferLayer();
    const zoom = map.getZoom();
    const legend = document.getElementById("transferDotLegend");
    // Only render individual dots when we have raw points (no step aggregation)
    const isRaw = rows.length > 0 && rows[0].has_transfer !== undefined;
    if(!isRaw || zoom < 13){
      if(legend) legend.style.display = "none";
      return;
    }
    const group = L.featureGroup();
    const TR_PURPOSE_COLORS = {
      p2p_local:     "#27ae60",
      p2p_abroad:    "#e74c3c",
      invest:        "#e67e22",
      conversion:    "#f39c12",
      deposit:       "#8e44ad",
      iban_external: "#2980b9",
      budget:        "#e91e8c",
      transfer:      "#95a5a6",
    };
    rows.forEach(p => {
      if(!p.has_transfer) return;
      const color = TR_PURPOSE_COLORS[p.purpose_cat] || "#f2b705";
      const m = L.circleMarker([p.lat, p.lon], {
        radius: 7,
        color: "#fff",
        weight: 1.5,
        fillColor: color,
        fillOpacity: 0.9,
        pane: "markerPane",
      });
      m.bindTooltip(`Перевод в сессии${p.purpose_cat ? " · " + p.purpose_cat : ""}`, { sticky: true });
      group.addLayer(m);
    });
    state.transferLayer = group;
    group.addTo(map);
    if(legend) legend.style.display = "flex";
  }

  // ---- Transfer heat layer (separate from auth heatmap) ----
  const TR_HEAT_GRADIENT = {0.15:"#134e4a", 0.4:"#0d9488", 0.65:"#34d399", 0.85:"#fcd34d", 1.0:"#f97316"};
  const TR_PURPOSE_COLORS_MAP = {
    p2p_local:"#0d9488", p2p_abroad:"#ef4444", invest:"#f97316",
    conversion:"#eab308", deposit:"#8b5cf6", iban_external:"#3b82f6",
    budget:"#ec4899", transfer:"#95a5a6",
  };
  const TR_PURPOSE_LABELS = {
    p2p_local:"P2P локал", p2p_abroad:"P2P зарубеж", invest:"Инвестиции",
    conversion:"Конвертация", deposit:"Депозит", iban_external:"IBAN внешний",
    budget:"Бюджет", transfer:"Прочее",
  };

  function _removeTransferHeatLayer() {
    if (state.transferHeatLayer) {
      try { map.removeLayer(state.transferHeatLayer); } catch (_) {}
      state.transferHeatLayer = null;
    }
  }

  async function loadTransferLayer() {
    _removeTransferHeatLayer();
    const legend = document.getElementById("transferDotLegend");
    const { period, anchorDate, startDate, endDate, oblast } = state.currentFilter;
    const b = map.getBounds();
    const zoom = map.getZoom();
    const q = new URLSearchParams({
      min_lat: String(b.getSouth()), max_lat: String(b.getNorth()),
      min_lon: String(b.getWest()),  max_lon: String(b.getEast()),
      zoom: String(zoom), period, oblast,
    });
    if (anchorDate) q.set("anchor_date", anchorDate);
    if (period === "custom") {
      if (startDate) q.set("start_date", startDate);
      if (endDate) q.set("end_date", endDate);
    }
    const purp = el.purposeSelect?.value;
    if (purp) q.set("purpose_cats", purp);

    try {
      const rows = await fetchJson(`/api/transfers/points?${q}`);
      if (!rows || !rows.length) { if(legend) legend.style.display = "none"; return; }

      // Always heatmap — no individual dot markers in global view
      const heatData = rows.map(r => [
        r.lat, r.lon,
        r.count !== undefined
          ? Math.min(5, 0.2 + Math.log((r.count || 1) + 1))
          : 0.7,
      ]);
      state.transferHeatLayer = L.heatLayer(heatData, {
        radius: 34, blur: 26, maxZoom: 17, gradient: TR_HEAT_GRADIENT,
      }).addTo(map);

      if (legend) legend.style.display = "flex";
      refreshDetailTop();
    } catch (e) { console.warn("[transfer layer]", e); }
  }

  async function loadTransferDashboard() {
    const { period, anchorDate, startDate, endDate, oblast } = state.currentFilter;
    const q = new URLSearchParams({ period, oblast });
    if (anchorDate) q.set("anchor_date", anchorDate);
    if (period === "custom") {
      if (startDate) q.set("start_date", startDate);
      if (endDate) q.set("end_date", endDate);
    }
    const purp = el.purposeSelect?.value;
    if (purp) q.set("purpose_cats", purp);
    try {
      const data = await fetchJson(`/api/transfers/dashboard?${q}`);
      state.transferDash = data;
      const kpi = data.kpi || {};
      if (el.dbTrCount) animateNum(el.dbTrCount, kpi.count || 0);
      if (el.dbTrVol) el.dbTrVol.textContent = fmtMln(kpi.volume_kzt || 0);
      if (el.dbTrAvg) el.dbTrAvg.textContent = fmtMln(kpi.avg_kzt || 0);
      if (el.dbTrUsers) animateNum(el.dbTrUsers, kpi.users || 0);
      [el.dbTrCountCard, el.dbTrVolCard, el.dbTrAvgCard, el.dbTrUsersCard].forEach(c => {
        if (c) c.style.display = state.layerMode !== "auth" ? "" : "none";
      });
    } catch(e) { console.warn("[transfer dash]", e); }
  }

  function renderTransferSidebarSection(rayonId) {
    if (!el.transferStatsSection) return;
    const lm = state.layerMode;
    if (lm === "auth") { el.transferStatsSection.style.display = "none"; return; }
    if (!state.transferDash) { el.transferStatsSection.style.display = "none"; return; }

    // Find rayon in transfer dashboard top list
    const top = (state.transferDash.top_rayons || []).find(r => r.rayon_id === rayonId);
    if (!top) {
      // No data for this rayon specifically — show global transfer stats
      const kpi = state.transferDash.kpi || {};
      if (el.kpiTrCount) el.kpiTrCount.textContent = fmtNum(kpi.count || 0);
      if (el.kpiTrVol) el.kpiTrVol.textContent = fmtMln(kpi.volume_kzt || 0);
      if (el.kpiTrAvg) el.kpiTrAvg.textContent = fmtMln(kpi.avg_kzt || 0);
      if (el.kpiTrConv) el.kpiTrConv.textContent = "—";
    } else {
      if (el.kpiTrCount) el.kpiTrCount.textContent = fmtNum(top.count || 0);
      if (el.kpiTrVol) el.kpiTrVol.textContent = fmtMln(top.volume_kzt || 0);
      if (el.kpiTrAvg) el.kpiTrAvg.textContent = top.count ? fmtMln((top.volume_kzt || 0) / top.count) : "—";
      if (el.kpiTrConv) el.kpiTrConv.textContent = "—";
    }

    // Purpose breakdown bars
    if (el.purposeBreakdown) {
      const purposes = state.transferDash.by_purpose || [];
      const maxVol = Math.max(1, ...purposes.map(p => p.volume_kzt));
      el.purposeBreakdown.innerHTML = purposes.slice(0, 6).map(p => {
        const pct = Math.round((p.volume_kzt / maxVol) * 100);
        const color = TR_PURPOSE_COLORS_MAP[p.purpose_cat] || "#95a5a6";
        return `<div class="tr-purpose-bar">
          <span style="width:90px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${TR_PURPOSE_LABELS[p.purpose_cat] || p.purpose_cat}</span>
          <div class="bar-track"><div class="bar-fill" style="width:${pct}%;background:${color};"></div></div>
          <span style="width:55px;text-align:right;">${fmtMln(p.volume_kzt)}</span>
        </div>`;
      }).join("");
    }

    el.transferStatsSection.style.display = "";
  }

  function fmtMln(v) {
    if (v >= 1e9) return (v / 1e9).toFixed(1) + " млрд";
    if (v >= 1e6) return (v / 1e6).toFixed(1) + " млн";
    if (v >= 1e3) return (v / 1e3).toFixed(0) + "K";
    return Math.round(v).toLocaleString();
  }

  function applyLayerModeUi(mode) {
    [el.layerAuth, el.layerTransfer, el.layerBoth].forEach((btn, i) => {
      if (btn) btn.classList.toggle("active", ["auth","transfer","both"][i] === mode);
    });
    if (el.purposeFilterPill) el.purposeFilterPill.style.display = mode !== "auth" ? "" : "none";
  }

  function setLayerMode(mode) {
    state.layerMode = mode;
    applyLayerModeUi(mode);
    reloadPointsOnly();
    if (mode !== "auth") {
      loadTransferDashboard().catch(() => {});
    } else {
      [el.dbTrCountCard, el.dbTrVolCard, el.dbTrAvgCard, el.dbTrUsersCard].forEach(c => {
        if (c) c.style.display = "none";
      });
      if (el.transferStatsSection) el.transferStatsSection.style.display = "none";
    }
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
    const lm = state.layerMode;

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
      layer_mode: lm === "auth" ? "no_transfer" : "all",
    });
    if(anchorDate) q.set("anchor_date", anchorDate);
    if(period === "custom"){
      if(startDate) q.set("start_date", startDate);
      if(endDate) q.set("end_date", endDate);
    }
    if(state.selectedRayon && (oblast === "ALL" || state.selectedRayonOblast === oblast)){
      q.set("rayon_id", state.selectedRayon);
    }

    // In pure transfer mode skip auth fetch entirely
    let rows = [];
    if (lm !== "transfer") {
      rows = await fetchJson(`/api/points?${q.toString()}`);
      if(requestId !== state.lastRequestId) return;
    }

    state.lastFiltered = rows;

    // Auth heatmap
    if (lm === "transfer") {
      if (state.heatLayer && map.hasLayer(state.heatLayer)) map.removeLayer(state.heatLayer);
    } else {
      updateHeatLayer(rows);
    }

    // Transfer heatmap (both / transfer modes)
    if (lm !== "auth") {
      loadTransferLayer();
    } else {
      _removeTransferHeatLayer();
    }

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

    // Conversion KPI (session join data)
    const sess = payload?.session;
    const convCard = document.getElementById("dbConvCard");
    const convPct  = document.getElementById("dbConvPct");
    const convSess = document.getElementById("dbConvSessions");
    if(sess && payload?.has_session_data && convCard){
      convCard.style.display = "";
      if(convPct)  convPct.textContent  = (sess.conversion_pct ?? 0) + "%";
      if(convSess) convSess.textContent = `${(sess.transfer_sessions || 0).toLocaleString()} из ${(sess.total_sessions || 0).toLocaleString()}`;
    } else if(convCard){
      convCard.style.display = "none";
    }

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

    if (state.choroplethEnabled) await loadChoropleth();

    // Run compare and anomalies in parallel — don't block dashboard render
    Promise.all([
      loadPeriodCompare().catch(() => {}),
      loadAnomalies().catch(() => {}),
    ]);
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
      if (state.layerMode !== "auth") loadTransferDashboard().catch(() => {});
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
      if (state.layerMode !== "auth") loadTransferDashboard().catch(() => {});
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
      if (state.layerMode !== "auth") loadTransferDashboard().catch(() => {});
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
      if (state.layerMode !== "auth") loadTransferDashboard().catch(() => {});
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
      if (state.layerMode !== "auth") loadTransferDashboard().catch(() => {});
    });
    el.anchorDate?.addEventListener("change", async ()=>{
      syncFilterFromUi();
      if(state.selectedRayon){
        await refreshSelectedRayonStats();
        return;
      }
      await reloadPointsOnly();
      await reloadDashboard();
      if (state.layerMode !== "auth") loadTransferDashboard().catch(() => {});
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

    if (el.showBehavior) {
      el.showBehavior.addEventListener("change", async () => {
        state.behaviorEnabled = !!el.showBehavior.checked;
        if (state.behaviorEnabled) {
          await loadBehaviorClusters();
        } else {
          resetBehaviorClusters();
        }
      });
    }

    if (el.layerAuth) {
      el.layerAuth.addEventListener("click", () => setLayerMode("auth"));
    }
    if (el.layerTransfer) {
      el.layerTransfer.addEventListener("click", () => setLayerMode("transfer"));
    }
    if (el.layerBoth) {
      el.layerBoth.addEventListener("click", () => setLayerMode("both"));
    }
    if (el.purposeSelect) {
      el.purposeSelect.addEventListener("change", async () => {
        if (state.layerMode !== "auth") {
          await loadTransferLayer();
          await loadTransferDashboard();
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
            const { oblast, period, anchorDate, startDate, endDate } = state.currentFilter;
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
    // Sync layer mode button to initial state (UI only, no reload)
    applyLayerModeUi(state.layerMode);
    state.lastOblast = state.currentFilter.oblast;
    await refreshLayers(false);
    await reloadPointsOnly();
    await reloadDashboard();
    if (state.layerMode !== "auth") loadTransferDashboard().catch(() => {});
  }

  init().catch(err => {
    console.error(err);
    alert("Ошибка инициализации: " + err.message);
  });
})();






