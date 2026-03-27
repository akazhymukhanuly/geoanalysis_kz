(function(){
  // ============================================================
  // UI HTML (твоя разметка 1-в-1)
  // ============================================================
  const ui = `
<div id="topbar">
  <div id="brand">
    <div class="t1">Mobile Auth Geo Analytics</div>
    <div class="t2">Kazakhstan • Heat + Region drilldown</div>
  </div>
  <div id="controls">
    <label class="pill" title="Слой тепла: Events">
      <input type="radio" name="mode" id="modeEvents" checked> Events
    </label>
    <label class="pill" title="Слой тепла: Unique IIN">
      <input type="radio" name="mode" id="modeUnique"> Unique IIN
    </label>

    <label class="pill" title="Фильтр по областям / городам (верхний уровень)">
      <span style="opacity:.8;">Область</span>
      <select id="oblastSelect">
        <option value="ALL">Барлық Қазақстан</option>
      </select>
    </label>

    <label class="pill" title="Показывать детальные границы (районы/аулы) выбранной области">
      <input type="checkbox" id="showDetails" checked> Details
    </label>

    <label class="pill" title="Показать слой кластеризации (если подключен markercluster)">
      <input type="checkbox" id="showClusters"> Clusters
    </label>

    <div class="pill" id="slider-wrap">
      <div id="time-val">09:00 — 18:00</div>
      <div id="time-slider"></div>
    </div>
  </div>
</div>

<div id="sidebar">
  <div id="sb-head">
    <div>
      <div id="sb-title">Регион</div>
      <div id="sb-sub">Клик по району/границе • KPI по фильтру</div>
    </div>
    <button id="sb-close">×</button>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="l">Авторизаций (events)</div>
      <div class="v" id="kpiEvents">0</div>
      <div class="s">в выбранный интервал</div>
    </div>
    <div class="kpi">
      <div class="l">Уникальных IIN</div>
      <div class="v" id="kpiUsers">0</div>
      <div class="s">в выбранный интервал</div>
    </div>
  </div>

  <div class="kpis">
    <div class="kpi">
      <div class="l">Top hour</div>
      <div class="v" id="kpiTopHour">—</div>
      <div class="s">пик активности</div>
    </div>
    <div class="kpi">
      <div class="l">Area</div>
      <div class="v" id="kpiArea">—</div>
      <div class="s">район / область</div>
    </div>
  </div>

  <div id="miniChartWrap">
    <div id="miniChartTitle">Распределение по часам (events)</div>
    <canvas id="miniChart"></canvas>
  </div>

  <div id="hotspots">
    <h4>Top Hotspots</h4>
    <div id="hsList"></div>
  </div>
</div>

<div id="legend">
  <div class="t">Heat intensity</div>
  <div class="bar"></div>
  <div class="s"><span>low</span><span>high</span></div>
</div>

<div id="chartTip"></div>
`;
  document.getElementById("ui-root").innerHTML = ui;

  // ============================================================
  // helpers
  // ============================================================
  const pad2 = (n)=> (n<10?("0"+n):(""+n));
  const fmtHour = (h)=> pad2(h)+":00";

  async function fetchJson(url){
    const r = await fetch(url);
    if(!r.ok) throw new Error(`${url} -> ${r.status}`);
    return await r.json();
  }

  function getMode(){
    return el.modeUnique.checked ? "unique" : "events";
  }

  // hotspots отрисуем по тем данным, которые пришли (они ограничены bbox/grid)
  function computeHotspots(rows){
    const grid = new Map();
    const cell = 0.12;
    for(const p of rows){
      const glat = Math.round(p.lat / cell) * cell;
      const glon = Math.round(p.lon / cell) * cell;
      const key = glat.toFixed(3)+","+glon.toFixed(3);
      if(!grid.has(key)) grid.set(key, {lat: glat, lon: glon, count:0, users:new Set()});
      const o = grid.get(key);
      o.count += (p.count !== undefined ? p.count : 1);
      if(p.iin) o.users.add(p.iin);
    }
    const arr = Array.from(grid.values()).map(o => ({lat:o.lat, lon:o.lon, count:o.count, users:o.users.size}));
    arr.sort((a,b)=> b.count - a.count);
    return arr.slice(0,5);
  }

  function renderHotspots(list, map){
    el.hsList.innerHTML = "";
    list.forEach((it, idx) => {
      const d = document.createElement("div");
      d.className = "hs";
      d.innerHTML = `
        <div>
          <div class="a">#${idx+1} hotspot</div>
          <div class="b">${it.count} events • ~${it.users} users</div>
        </div>
        <div class="c">${it.count}</div>
      `;
      d.onclick = ()=> map.setView([it.lat, it.lon], 12, {animate:true});
      el.hsList.appendChild(d);
    });
  }

  function buildHeatData(rows, mode){
    // rows могут быть grid ({count}) или raw points
    if(mode === "unique"){
      // unique по iin работает только на raw points; на grid смысла нет
      const seen = new Map();
      for(const p of rows){
        if(!p.iin) continue;
        if(!seen.has(p.iin)) seen.set(p.iin, p);
      }
      return Array.from(seen.values()).map(p => [p.lat, p.lon, 1.0]);
    }
    // events: если grid -> интенсивность = count
    return rows.map(p => [p.lat, p.lon, p.count !== undefined ? Math.min(5, 0.2 + Math.log(p.count+1)) : 0.7]);
  }

  // mini chart
  function drawBars(canvas, values, opts){
    opts = opts || {};
    const topHour = opts.topHour ?? null;
    const minH = opts.minH ?? 0;
    const maxH = opts.maxH ?? 23;

    const ctx = canvas.getContext("2d");
    const w = canvas.clientWidth, h = canvas.clientHeight;
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

    ctx.globalAlpha = 0.18;
    ctx.strokeStyle = "#0f172a";
    ctx.lineWidth = 1;
    for(let i=1;i<=3;i++){
      const y = padT + plotH * (i/4);
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(padL+plotW, y);
      ctx.stroke();
    }
    ctx.globalAlpha = 1;

    for(let i=0;i<n;i++){
      const v = values[i] / maxV;
      const bh = Math.round(plotH * v);
      const x = padL + i*bw + 1;
      const y = padT + (plotH - bh);
      const isTop = (topHour !== null && i === topHour && values[i] > 0);
      ctx.fillStyle = isTop ? "rgba(211,47,47,0.98)" : "rgba(211,47,47,0.72)";
      ctx.fillRect(x, y, Math.max(2, bw-2), bh);
    }

    ctx.fillStyle = "rgba(15,23,42,0.65)";
    ctx.font = "11px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto";
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    const ticks = new Set([0, 6, 12, 18, 23, minH, maxH]);
    if (topHour !== null) ticks.add(topHour);
    ticks.forEach((hr) => {
      if (hr < 0 || hr > 23) return;
      const x = padL + hr*bw + bw/2;
      const y = padT + plotH + 6;
      if (topHour !== null && hr === topHour && values[hr] > 0){
        ctx.fillStyle = "rgba(211,47,47,0.95)";
        ctx.font = "700 11px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto";
        ctx.fillText(String(hr), x, y);
        ctx.fillStyle = "rgba(15,23,42,0.65)";
        ctx.font = "11px ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto";
      } else {
        ctx.fillText(String(hr), x, y);
      }
    });
  }

  // tooltip for chart
  const showTip = (x,y,html)=>{
    el.chartTip.innerHTML = html;
    el.chartTip.style.display="block";
    const pad = 12;
    const rectW = el.chartTip.offsetWidth || 180;
    const rectH = el.chartTip.offsetHeight || 60;
    let left = x + pad, top = y + pad;
    if (left + rectW > window.innerWidth - 8) left = x - rectW - pad;
    if (top + rectH > window.innerHeight - 8) top = y - rectH - pad;
    el.chartTip.style.left = left + "px";
    el.chartTip.style.top = top + "px";
  };
  const hideTip = ()=> el.chartTip.style.display="none";

  // ============================================================
  // DOM refs
  // ============================================================
  const el = {
    slider: document.getElementById("time-slider"),
    timeVal: document.getElementById("time-val"),
    modeEvents: document.getElementById("modeEvents"),
    modeUnique: document.getElementById("modeUnique"),
    sidebar: document.getElementById("sidebar"),
    sbClose: document.getElementById("sb-close"),
    sbTitle: document.getElementById("sb-title"),
    kpiEvents: document.getElementById("kpiEvents"),
    kpiUsers: document.getElementById("kpiUsers"),
    kpiTopHour: document.getElementById("kpiTopHour"),
    kpiArea: document.getElementById("kpiArea"),
    miniChart: document.getElementById("miniChart"),
    hsList: document.getElementById("hsList"),
    chartTip: document.getElementById("chartTip"),
    oblastSelect: document.getElementById("oblastSelect"),
    showDetails: document.getElementById("showDetails"),
    showClusters: document.getElementById("showClusters"),
  };
  el.sbClose.onclick = ()=> { el.sidebar.style.display = "none"; };

  if (!el.slider.noUiSlider) {
    noUiSlider.create(el.slider, {
      start: [9, 18],
      connect: true,
      step: 1,
      range: { min: 0, max: 23 }
    });
  }

  el.miniChart.addEventListener("mousemove", (ev)=>{
    const meta = el.miniChart.__barsMeta;
    if(!meta) return;
    const r = el.miniChart.getBoundingClientRect();
    const x = ev.clientX - r.left;
    const y = ev.clientY - r.top;
    if (x < meta.padL || x > meta.padL + meta.plotW || y < meta.padT || y > meta.padT + meta.plotH){
      hideTip(); return;
    }
    const idx = Math.max(0, Math.min(23, Math.floor((x - meta.padL) / meta.bw)));
    const v = meta.values[idx] || 0;
    const pct = Math.round((v / meta.maxV) * 100);
    const isInRange = (idx >= meta.minH && idx <= meta.maxH);
    const badge = isInRange
      ? '<span style="color:#22c55e;font-weight:900;">IN RANGE</span>'
      : '<span style="color:#f59e0b;font-weight:900;">OUT</span>';

    showTip(ev.clientX, ev.clientY, `
      <div><b>${idx}:00</b></div>
      <div class="muted">${badge} • events</div>
      <div style="margin-top:6px;"><b>${v}</b> событий</div>
      <div class="muted">Интенсивность: ${pct}%</div>
    `);
  });
  el.miniChart.addEventListener("mouseleave", hideTip);

  // ============================================================
  // MAP init (без folium)
  // ============================================================
  const map = L.map("map", {
    preferCanvas: true,
    zoomSnap: 2,
    z
