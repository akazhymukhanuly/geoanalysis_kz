(function () {
  const pad2 = (n) => (n < 10 ? `0${n}` : `${n}`);
  const fmtHour = (h) => `${pad2(Number(h))}:00`;
  const CLIENT_TOP_LABEL = "Профиль клиента";
  const CLIENT_PANEL_LABEL = "Клиент";

  const map = L.map("map", { preferCanvas: true, attributionControl: false }).setView([43.24, 76.9], 11);
  L.tileLayer("/tiles/{z}/{x}/{y}.png", {
    minZoom: 5,
    maxZoom: 18,
    attribution: "OSM via local proxy",
  }).addTo(map);

  const typeConfig = {
    supermarket: { icon: "🛒", color: "#f1c40f", label: "Маркет" },
    food: { icon: "🍴", color: "#e67e22", label: "Еда" },
    medical: { icon: "🏥", color: "#e74c3c", label: "Медицина" },
    spa: { icon: "🧖", color: "#9b59b6", label: "СПА" },
    kids: { icon: "🧸", color: "#ff9ff3", label: "Детям" },
    beauty: { icon: "💄", color: "#fd79a8", label: "Красота" },
    furniture: { icon: "🛋️", color: "#a0522d", label: "Мебель" },
    fashion: { icon: "👗", color: "#fab1a0", label: "Одежда" },
    travel: { icon: "✈️", color: "#0984e3", label: "Туризм" },
    fitness: { icon: "💪", color: "#00b894", label: "Фитнес" },
    education: { icon: "🎓", color: "#74b9ff", label: "Учеба" },
    gas: { icon: "⛽", color: "#636e72", label: "АЗС" },
    default: { icon: "📍", color: "#b2bec3", label: "Объект" },
  };

  const el = {
    clientIinTop: document.getElementById("clientIinTop"),
    periodSelect: document.getElementById("periodSelect"),
    customPeriodWrap: document.getElementById("customPeriodWrap"),
    periodStart: document.getElementById("periodStart"),
    periodEnd: document.getElementById("periodEnd"),
    slider: document.getElementById("time-slider"),
    timeVal: document.getElementById("time-val"),
    pointPlayBtn: document.getElementById("pointPlayBtn"),
    clientIin: document.getElementById("clientIin"),
    clientEvents: document.getElementById("clientEvents"),
    clientPlaces: document.getElementById("clientPlaces"),
    clientTopHour: document.getElementById("clientTopHour"),
    clientRayons: document.getElementById("clientRayons"),
    clientFirstSeen: document.getElementById("clientFirstSeen"),
    clientLastSeen: document.getElementById("clientLastSeen"),
    clientTopRayons: document.getElementById("clientTopRayons"),
    clientInferredPlaces: document.getElementById("clientInferredPlaces"),
    clientMsg: document.getElementById("clientMsg"),
    chart: document.getElementById("clientHoursChart"),
    chartTip: document.getElementById("chartTip"),
    selectedAuthBlock: document.getElementById("selectedAuthBlock"),
    selAuthTime: document.getElementById("selAuthTime"),
    selAuthHour: document.getElementById("selAuthHour"),
    selAuthRayon: document.getElementById("selAuthRayon"),
    selAuthNear: document.getElementById("selAuthNear"),
    routeToggleBtn: document.getElementById("routeToggleBtn"),
    clientPanel: document.getElementById("clientPanel"),
  };

  const state = {
    iin: "",
    points: [],
    places: [],
    nearbyCache: new Map(),
    activeMiniMaps: {},
    playTimer: null,
    playIdx: 0,
    isPlayback: false,
    routeEnabled: false,
    routeLayer: null,
    filters: {
      minH: 0,
      maxH: 23,
      period: "week",
      startDate: "",
      endDate: "",
    },
  };

  let pointsLayer = L.layerGroup().addTo(map);
  let heatLayer = null;
  let playLayer = L.layerGroup().addTo(map);
  let placesLayer = L.layerGroup().addTo(map);

  function setPlaybackMode(on) {
    if (on) {
      if (map.hasLayer(pointsLayer)) map.removeLayer(pointsLayer);
      if (heatLayer && map.hasLayer(heatLayer)) map.removeLayer(heatLayer);
      return;
    }
    if (!map.hasLayer(pointsLayer)) map.addLayer(pointsLayer);
    if (heatLayer && !map.hasLayer(heatLayer)) map.addLayer(heatLayer);
  }

  function fmtDateTime(v) {
    if (!v) return "-";
    return String(v).replace("T", " ").slice(0, 19);
  }

  function fmtNum(n) {
    return Number(n || 0).toLocaleString();
  }

  function placeLabel(key) {
    if (key === "home") return "Дом (вероятно)";
    if (key === "work") return "Работа (вероятно)";
    if (key === "hobby") return "Хобби (вероятно)";
    return "Частое место";
  }

  function placeColor(key) {
    if (key === "home") return "#2563eb";
    if (key === "work") return "#16a34a";
    if (key === "hobby") return "#d97706";
    return "#64748b";
  }

  function findTopHour(hours) {
    let topIdx = 0;
    let topVal = -1;
    for (let i = 0; i < hours.length; i += 1) {
      if (hours[i] > topVal) {
        topVal = hours[i];
        topIdx = i;
      }
    }
    return fmtHour(topIdx);
  }

  function todayIso() {
    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, "0");
    const d = String(now.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  function shiftIso(isoDate, days) {
    const d = new Date(`${isoDate}T00:00:00`);
    d.setDate(d.getDate() + days);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  }

  function syncCustomPeriodVisibility() {
    const isCustom = (el.periodSelect?.value || "week") === "custom";
    if (el.customPeriodWrap) {
      el.customPeriodWrap.style.display = isCustom ? "flex" : "none";
    }
  }

  function syncFiltersFromUi() {
    const vals = el.slider.noUiSlider.get();
    const minH = parseInt(vals[0], 10);
    const maxH = parseInt(vals[1], 10);
    const period = el.periodSelect.value || "week";
    let startDate = (el.periodStart.value || "").trim();
    let endDate = (el.periodEnd.value || "").trim();
    if (period === "custom") {
      const t = todayIso();
      if (!endDate) endDate = t;
      if (!startDate) startDate = shiftIso(endDate, -6);
      el.periodStart.value = startDate;
      el.periodEnd.value = endDate;
    }
    state.filters = { minH, maxH, period, startDate, endDate };
    el.timeVal.innerText = `${fmtHour(minH)} — ${fmtHour(maxH)}`;
  }

  function stopPointPlayback() {
    if (state.playTimer) {
      clearInterval(state.playTimer);
      state.playTimer = null;
    }
    state.isPlayback = false;
    setPlaybackMode(false);
    playLayer.clearLayers();
    if (el.pointPlayBtn) el.pointPlayBtn.textContent = "▶ Точки";
  }

  function buildClientParams(order = "desc") {
    const p = new URLSearchParams({
      iin: state.iin,
      min_h: String(state.filters.minH),
      max_h: String(state.filters.maxH),
      period: state.filters.period,
      order,
    });
    if (state.filters.period === "custom") {
      if (state.filters.startDate) p.set("start_date", state.filters.startDate);
      if (state.filters.endDate) p.set("end_date", state.filters.endDate);
    }
    return p;
  }

  async function fetchJson(url) {
    const r = await fetch(url);
    if (!r.ok) {
      const text = await r.text();
      throw new Error(`${r.status}: ${text}`);
    }
    return r.json();
  }

  function showTip(x, y, html) {
    el.chartTip.innerHTML = html;
    el.chartTip.style.display = "block";
    const pad = 12;
    const w = el.chartTip.offsetWidth || 180;
    const h = el.chartTip.offsetHeight || 60;
    let left = x + pad;
    let top = y + pad;
    if (left + w > window.innerWidth - 8) left = x - w - pad;
    if (top + h > window.innerHeight - 8) top = y - h - pad;
    el.chartTip.style.left = `${left}px`;
    el.chartTip.style.top = `${top}px`;
  }

  function hideTip() {
    el.chartTip.style.display = "none";
  }

  function drawHours(canvas, values) {
    const ctx = canvas.getContext("2d");
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    canvas.width = Math.floor(w * devicePixelRatio);
    canvas.height = Math.floor(h * devicePixelRatio);
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const data = Array.from({ length: 24 }, (_, i) => Number(values[i] || 0));
    const max = Math.max(1, ...data);
    const padL = 20;
    const padR = 8;
    const padT = 6;
    const padB = 18;
    const plotW = w - padL - padR;
    const plotH = h - padT - padB;
    const bw = plotW / data.length;
    canvas.__barsMeta = { padL, padT, plotW, plotH, bw, data, max };

    for (let i = 0; i < data.length; i += 1) {
      const bh = Math.round((data[i] / max) * plotH);
      const x = padL + i * bw;
      const y = padT + (plotH - bh);
      ctx.fillStyle = "rgba(242,183,5,0.92)";
      ctx.fillRect(x + 1, y, Math.max(2, bw - 2), bh);
    }
  }

  el.chart.addEventListener("mousemove", (ev) => {
    const m = el.chart.__barsMeta;
    if (!m) return;
    const r = el.chart.getBoundingClientRect();
    const x = ev.clientX - r.left;
    const y = ev.clientY - r.top;
    if (x < m.padL || x > m.padL + m.plotW || y < m.padT || y > m.padT + m.plotH) {
      hideTip();
      return;
    }
    const idx = Math.max(0, Math.min(23, Math.floor((x - m.padL) / m.bw)));
    const val = m.data[idx] || 0;
    showTip(ev.clientX, ev.clientY, `<div><b>${fmtHour(idx)}</b></div><div>${val} событий</div>`);
  });
  el.chart.addEventListener("mouseleave", hideTip);

  function renderTopRayons(rows) {
    el.clientTopRayons.innerHTML = "";
    (rows || []).forEach((r, idx) => {
      const d = document.createElement("div");
      d.className = "row-lite";
      d.innerHTML = `
        <div>
          <div><b>#${idx + 1} ${r.rayon_name || "-"}</b></div>
          <div class="m">${r.oblast_kk || "-"}</div>
        </div>
        <div><b>${fmtNum(r.events)}</b></div>
      `;
      el.clientTopRayons.appendChild(d);
    });
  }

  function renderPlaces(places) {
    if (!placesLayer) return;
    placesLayer.clearLayers();
    if (el.clientInferredPlaces) el.clientInferredPlaces.innerHTML = "";
    if (!Array.isArray(places) || !places.length) return;

    places.forEach((p, idx) => {
      const color = placeColor(p.label);
      const radiusM = Math.max(120, Math.min(520, 90 + Number(p.events || 0) * 8));
      const confidencePct = Math.round(Number(p.confidence || 0) * 100);
      const circle = L.circle([p.lat, p.lon], {
        radius: radiusM,
        color,
        weight: 2,
        opacity: 0.92,
        fillColor: color,
        fillOpacity: 0.18,
      }).addTo(placesLayer);
      const core = L.circleMarker([p.lat, p.lon], {
        radius: 6,
        color: "#ffffff",
        weight: 2,
        fillColor: color,
        fillOpacity: 1,
      }).addTo(placesLayer);
      const popupHtml = `
        <b>${placeLabel(p.label)}</b><br>
        confidence: ${confidencePct}%<br>
        событий: ${fmtNum(p.events)}<br>
        активных дней: ${fmtNum(p.active_days)}
      `;
      circle.bindPopup(popupHtml);
      core.bindPopup(popupHtml);

      if (el.clientInferredPlaces) {
        const d = document.createElement("div");
        d.className = "row-lite";
        d.innerHTML = `
          <div>
            <div><b>#${idx + 1} ${placeLabel(p.label)}</b></div>
            <div class="m">confidence: ${confidencePct}% • событий: ${fmtNum(p.events)}</div>
          </div>
          <div><b>${confidencePct}%</b></div>
        `;
        d.onclick = () => {
          map.setView([p.lat, p.lon], Math.max(map.getZoom(), 15), { animate: true });
          core.openPopup();
        };
        el.clientInferredPlaces.appendChild(d);
      }
    });
  }

  function computeStabilityScore(points) {
    if (!points || points.length < 2) return null;
    // Find most common district
    const districtCounts = {};
    for (const p of points) {
      const key = p.rayon_name || "?";
      districtCounts[key] = (districtCounts[key] || 0) + 1;
    }
    const topCount = Math.max(...Object.values(districtCounts));
    return Math.round((topCount / points.length) * 100);
  }

  function renderStabilityScore(score) {
    const existing = document.getElementById("stabilityBadge");
    if (existing) existing.remove();
    if (score === null) return;
    const level = score >= 70 ? "high" : score >= 40 ? "medium" : "low";
    const label = score >= 70 ? "Высокая" : score >= 40 ? "Средняя" : "Низкая";
    const badge = document.createElement("div");
    badge.id = "stabilityBadge";
    badge.className = "stability-badge";
    badge.innerHTML = `
      <div class="stability-ring ${level === "high" ? "" : level}">${score}%</div>
      <div>
        <div style="font-weight:800;font-size:12px;">Концентрация активности</div>
        <div style="font-size:11px;color:var(--muted);">${label} — ${score}% событий в одном районе</div>
      </div>
    `;
    if (el.clientTopRayons && el.clientTopRayons.parentNode) {
      el.clientTopRayons.parentNode.insertBefore(badge, el.clientTopRayons);
    }
  }

  function renderRoute(points) {
    if (state.routeLayer) {
      map.removeLayer(state.routeLayer);
      state.routeLayer = null;
    }
    if (!state.routeEnabled || !points || points.length < 2) return;
    const sorted = [...points].filter(p => p.event_ts).sort((a, b) => (a.event_ts > b.event_ts ? 1 : -1));
    if (sorted.length < 2) return;
    state.routeLayer = L.layerGroup().addTo(map);
    const n = sorted.length;
    for (let i = 0; i < n - 1; i++) {
      const a = sorted[i];
      const b = sorted[i + 1];
      const t = i / Math.max(1, n - 2);
      const r = Math.round(37 + t * (239 - 37));
      const g = Math.round(99 + t * (68 - 99));
      const bl = Math.round(235 + t * (68 - 235));
      L.polyline([[a.lat, a.lon], [b.lat, b.lon]], {
        color: `rgb(${r},${g},${bl})`,
        weight: 2.5,
        opacity: 0.75,
      }).addTo(state.routeLayer);
    }
  }

  function showSelectedAuth(point, nearbyCount) {
    if (!el.selectedAuthBlock) return;
    el.selectedAuthBlock.classList.remove("hidden");
    el.selAuthTime.textContent = fmtDateTime(point.event_ts);
    el.selAuthHour.textContent = String(point.hour ?? "-");
    el.selAuthRayon.textContent = point.rayon_name || "-";
    el.selAuthNear.textContent = fmtNum(nearbyCount);
  }

  function pointKey(_p, idx) {
    return `pt_${idx}`;
  }

  function jitterLatLon(lat, lon, meters, seed) {
    const dLat = (meters / 111320) * Math.cos(seed);
    const dLon = (meters / (111320 * Math.cos((lat * Math.PI) / 180))) * Math.sin(seed);
    return [lat + dLat, lon + dLon];
  }

  function buildPopupHtml(point, nearby, key) {
    const counts = nearby.reduce((acc, curr) => {
      const t = curr.type || "default";
      acc[t] = (acc[t] || 0) + 1;
      return acc;
    }, {});
    const rows = Object.keys(counts)
      .sort((a, b) => counts[b] - counts[a])
      .map((type) => {
        const cfg = typeConfig[type] || typeConfig.default;
        return `
          <tr>
            <td><label><input type="checkbox" checked class="f-check" data-type="${type}"> ${cfg.icon} ${cfg.label}</label></td>
            <td align="right"><b>${counts[type]}</b></td>
          </tr>
        `;
      })
      .join("");

    return `
      <div class="client-card" data-card-key="${key}">
        <div style="font-size:14px;border-bottom:1px solid #ccc;padding-bottom:6px;">
          <b>Карточка клиента</b>
        </div>
        <div id="analytics-default-${key}" class="analytics-box">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <div style="font-weight:700;">📊 Авторизации</div>
            <div style="font-size:12px;color:#555;">час <b>${point.hour ?? "-"}</b></div>
          </div>
          <div class="kv">Район: <b>${point.rayon_name || "-"}</b></div>
          <div class="kv">Последняя: <b>${fmtDateTime(point.event_ts)}</b></div>
          <div class="kv">Объектов рядом (≤500м): <b>${nearby.length}</b></div>
          <div class="hint">Клик по объекту на мини-карте покажет адрес/оценку.</div>
        </div>

        <div id="analytics-object-${key}" class="analytics-box hidden">
          <div data-k="name" style="font-size:13px;"></div>
          <div class="kv">Адрес: <b><span data-k="addr">—</span></b></div>
          <div class="kv">Оценка: <b><span data-k="rate">—</span></b> | Отзывов: <b><span data-k="rev">—</span></b></div>
          <div class="hint">Закрой popup объекта или кликни в пустое место мини-карты.</div>
        </div>

        <div id="mini-map-${key}" class="mini-map-box"></div>
        <div class="stats-wrapper">
          <table class="stats-table">${rows || "<tr><td>Нет объектов в радиусе 500 м</td><td></td></tr>"}</table>
        </div>
      </div>
    `;
  }

  function showObjectCard(key, obj) {
    const defEl = document.getElementById(`analytics-default-${key}`);
    const objEl = document.getElementById(`analytics-object-${key}`);
    if (!defEl || !objEl) return;
    const cfg = typeConfig[obj.type] || typeConfig.default;
    objEl.querySelector('[data-k="name"]').innerHTML = `${cfg.icon} <b>${obj.name || cfg.label}</b>`;
    objEl.querySelector('[data-k="addr"]').textContent = obj.address || "—";
    objEl.querySelector('[data-k="rate"]').textContent = obj.rating == null ? "—" : String(obj.rating);
    objEl.querySelector('[data-k="rev"]').textContent = obj.reviews == null ? "—" : String(obj.reviews);
    defEl.classList.add("hidden");
    objEl.classList.remove("hidden");
  }

  function showDefaultCard(key) {
    const defEl = document.getElementById(`analytics-default-${key}`);
    const objEl = document.getElementById(`analytics-object-${key}`);
    if (!defEl || !objEl) return;
    objEl.classList.add("hidden");
    defEl.classList.remove("hidden");
  }

  function renderPopupMiniMap(key, point, nearby, popupRoot) {
    const mapId = `mini-map-${key}`;
    const container = document.getElementById(mapId);
    if (!container) return;

    if (state.activeMiniMaps[mapId]) {
      state.activeMiniMaps[mapId].remove();
      delete state.activeMiniMaps[mapId];
    }

    const mini = L.map(mapId, {
      center: [point.lat, point.lon],
      zoom: 16,
      minZoom: 14,
      maxZoom: 18,
      zoomControl: false,
      attributionControl: false,
    });
    state.activeMiniMaps[mapId] = mini;
    L.tileLayer("/tiles/{z}/{x}/{y}.png").addTo(mini);

    const routeLayer = L.layerGroup().addTo(mini);
    const typeLayers = {};
    const seen = new Map();

    mini.on("click", () => {
      routeLayer.clearLayers();
      mini.closePopup();
      showDefaultCard(key);
    });

    L.circle([point.lat, point.lon], { radius: 500, color: "#f2b705", fillOpacity: 0.1, weight: 1 }).addTo(mini);
    L.circleMarker([point.lat, point.lon], { radius: 6, color: "#111827", fillColor: "white", fillOpacity: 1, weight: 2 }).addTo(mini);

    nearby.forEach((n, idx) => {
      const nLat = Number(n.lat);
      const nLon = Number(n.lon);
      if (!Number.isFinite(nLat) || !Number.isFinite(nLon)) return;
      const t = n.type || "default";
      if (!typeLayers[t]) typeLayers[t] = L.layerGroup().addTo(mini);
      const cfg = typeConfig[t] || typeConfig.default;

      const k = `${nLat.toFixed(5)}_${nLon.toFixed(5)}`;
      const c = (seen.get(k) || 0) + 1;
      seen.set(k, c);

      let drawLat = nLat;
      let drawLon = nLon;
      if (c > 1) {
        [drawLat, drawLon] = jitterLatLon(nLat, nLon, Math.min(20, 2 * c), (idx + c) * 1.7);
      }

      const dot = L.circleMarker([drawLat, drawLon], {
        radius: 5,
        color: "white",
        weight: 1,
        fillColor: cfg.color,
        fillOpacity: 1,
      }).addTo(typeLayers[t]);

      dot.bindPopup(
        `<b>${cfg.icon} ${n.name || cfg.label}</b><br>📍 ${n.dist_m || "-"} м<br>⭐ ${n.rating ?? "-"} (${n.reviews ?? "-"})`,
        { className: "compact-popup", closeButton: false, offset: [0, -5] }
      );

      dot.on("click", function (e) {
        routeLayer.clearLayers();
        L.polyline([[point.lat, point.lon], [nLat, nLon]], { color: "#e67e22", weight: 3, dashArray: "5,10" }).addTo(routeLayer);
        this.openPopup();
        showObjectCard(key, n);
        if (e && e.originalEvent) e.originalEvent.stopPropagation();
      });

      dot.on("popupclose", () => {
        routeLayer.clearLayers();
        showDefaultCard(key);
      });
    });

    const checks = popupRoot.querySelectorAll(".f-check");
    checks.forEach((box) => {
      box.onchange = function () {
        const t = this.getAttribute("data-type");
        if (!typeLayers[t]) return;
        if (this.checked) mini.addLayer(typeLayers[t]);
        else {
          mini.removeLayer(typeLayers[t]);
          routeLayer.clearLayers();
        }
      };
      box.ondblclick = function () {
        checks.forEach((other) => {
          const t = other.getAttribute("data-type");
          if (other !== box) {
            other.checked = false;
            if (typeLayers[t]) mini.removeLayer(typeLayers[t]);
          }
        });
        box.checked = true;
        const t = box.getAttribute("data-type");
        if (typeLayers[t]) mini.addLayer(typeLayers[t]);
        routeLayer.clearLayers();
      };
    });

    setTimeout(() => mini.invalidateSize(), 220);
  }

  async function getNearbyForPoint(point, key) {
    if (state.nearbyCache.has(key)) return state.nearbyCache.get(key);
    const nearby = await fetchJson(
      `/api/infrastructure/nearby?lat=${encodeURIComponent(point.lat)}&lon=${encodeURIComponent(point.lon)}&radius_m=500&limit=400`
    );
    state.nearbyCache.set(key, nearby);
    return nearby;
  }

  async function openPointPopup(marker, point, key) {
    const nearby = await getNearbyForPoint(point, key);
    showSelectedAuth(point, nearby.length);
    const html = buildPopupHtml(point, nearby, key);
    marker.once("popupopen", (e) => {
      const root = e.popup.getElement();
      renderPopupMiniMap(key, point, nearby, root);
    });
    marker.once("popupclose", () => {
      showDefaultCard(key);
      const mapId = `mini-map-${key}`;
      if (state.activeMiniMaps[mapId]) {
        state.activeMiniMaps[mapId].remove();
        delete state.activeMiniMaps[mapId];
      }
    });
    marker.bindPopup(html, { maxWidth: 420 }).openPopup();
  }

  function renderMainPoints(points) {
    pointsLayer.clearLayers();
    playLayer.clearLayers();
    if (heatLayer) {
      map.removeLayer(heatLayer);
      heatLayer = null;
    }

    const heat = [];
    let bounds = null;
    const maxInteractive = Math.min(points.length, 2000);
    for (let idx = 0; idx < maxInteractive; idx += 1) {
      const p = points[idx];
      const ll = [p.lat, p.lon];
      heat.push([p.lat, p.lon, 0.72]);
      const marker = L.marker(ll, {
        icon: L.divIcon({
          className: "c-pulse",
          html: '<div style="background:#2980b9;width:22px;height:22px;border-radius:50%;border:2px white solid;"></div>',
          iconSize: [22, 22],
        }),
      });
      marker.on("click", async () => {
        await openPointPopup(marker, p, pointKey(p, idx));
      });
      pointsLayer.addLayer(marker);
      if (!bounds) bounds = L.latLngBounds(ll, ll);
      else bounds.extend(ll);
    }

    for (let idx = maxInteractive; idx < points.length; idx += 1) {
      const p = points[idx];
      heat.push([p.lat, p.lon, 0.72]);
      if (!bounds) bounds = L.latLngBounds([p.lat, p.lon], [p.lat, p.lon]);
      else bounds.extend([p.lat, p.lon]);
    }

    if (heat.length > 0) {
      heatLayer = L.heatLayer(heat, { radius: 20, blur: 14, maxZoom: 16 }).addTo(map);
    }
    if (bounds && bounds.isValid()) {
      map.fitBounds(bounds, { padding: [25, 25] });
    }
  }

  function showPlayPoint(p) {
    playLayer.clearLayers();
    const marker = L.circleMarker([p.lat, p.lon], {
      radius: 9,
      color: "#111827",
      fillColor: "#f2b705",
      fillOpacity: 0.95,
      weight: 2,
    }).addTo(playLayer);
    marker.bindTooltip(`${fmtDateTime(p.event_ts)} • ${fmtHour(p.hour)}`, { sticky: true }).openTooltip();
    if (!map.getBounds().pad(-0.1).contains([p.lat, p.lon])) {
      map.panTo([p.lat, p.lon], { animate: true, duration: 0.45 });
    }
  }

  function startPointPlayback() {
    if (!state.points.length) return;
    stopPointPlayback();
    const timeline = [...state.points].reverse();
    state.isPlayback = true;
    setPlaybackMode(true);
    state.playIdx = 1;
    el.pointPlayBtn.textContent = "⏸ Пауза";
    showPlayPoint(timeline[0]);
    state.playTimer = setInterval(() => {
      if (state.playIdx >= timeline.length) {
        stopPointPlayback();
        return;
      }
      showPlayPoint(timeline[state.playIdx]);
      state.playIdx += 1;
    }, 700);
  }

  async function reloadClient() {
    if (el.clientPanel) el.clientPanel.classList.add("loading-shimmer");
    if (el.clientPanel) el.clientPanel.scrollTop = 0;
    stopPointPlayback();
    state.nearbyCache.clear();
    const summaryParams = buildClientParams("desc");
    const pointsParams = buildClientParams("desc");
    pointsParams.set("limit", "12000");
    const placesParams = new URLSearchParams({
      iin: state.iin,
      period: state.filters.period,
    });
    if (state.filters.period === "custom") {
      if (state.filters.startDate) placesParams.set("start_date", state.filters.startDate);
      if (state.filters.endDate) placesParams.set("end_date", state.filters.endDate);
    }

    const [summary, points, places] = await Promise.all([
      fetchJson(`/api/client/summary?${summaryParams.toString()}`),
      fetchJson(`/api/client/points?${pointsParams.toString()}`),
      fetchJson(`/api/client/places?${placesParams.toString()}`),
    ]);
    state.points = points || [];
    state.places = places || [];

    el.clientIin.textContent = CLIENT_PANEL_LABEL;
    el.clientEvents.textContent = fmtNum(summary.events);
    el.clientPlaces.textContent = fmtNum(summary.unique_places);
    el.clientTopHour.textContent = findTopHour(summary.hours || []);
    el.clientRayons.textContent = fmtNum(summary.unique_rayons);
    el.clientFirstSeen.textContent = fmtDateTime(summary.first_seen);
    el.clientLastSeen.textContent = fmtDateTime(summary.last_seen);
    el.clientMsg.textContent = `Загружено ${fmtNum(state.points.length)} точек авторизации. Нажмите на точку или используйте ▶ Точки.`;
    if (el.clientIinTop) el.clientIinTop.textContent = CLIENT_TOP_LABEL;

    drawHours(el.chart, summary.hours || []);
    renderTopRayons(summary.top_rayons || []);
    renderPlaces(state.places);
    renderMainPoints(state.points);
    if (el.clientPanel) el.clientPanel.classList.remove("loading-shimmer");
    if (el.selectedAuthBlock) el.selectedAuthBlock.classList.add("hidden");
    renderRoute(state.points);
    renderStabilityScore(computeStabilityScore(state.points));
  }

  function bindUi() {
    if (!el.slider || typeof noUiSlider === "undefined") {
      el.clientMsg.textContent = "Не удалось инициализировать ползунок времени.";
      return;
    }
    if (el.slider.noUiSlider) {
      el.slider.noUiSlider.destroy();
    }
    noUiSlider.create(el.slider, {
      start: [0, 23],
      connect: true,
      step: 1,
      behaviour: "tap-drag",
      range: { min: 0, max: 23 },
    });
    if (el.periodStart && !el.periodStart.value) {
      const t = todayIso();
      el.periodEnd.value = t;
      el.periodStart.value = shiftIso(t, -6);
    }
    syncCustomPeriodVisibility();
    syncFiltersFromUi();

    el.slider.noUiSlider.on("update", () => {
      syncFiltersFromUi();
    });
    el.slider.noUiSlider.on("change", async () => {
      syncFiltersFromUi();
      await reloadClient();
    });
    el.periodSelect.addEventListener("change", async () => {
      syncCustomPeriodVisibility();
      syncFiltersFromUi();
      await reloadClient();
    });
    el.periodStart.addEventListener("change", async () => {
      syncFiltersFromUi();
      await reloadClient();
    });
    el.periodEnd.addEventListener("change", async () => {
      syncFiltersFromUi();
      await reloadClient();
    });
    el.pointPlayBtn.addEventListener("click", () => {
      if (state.playTimer) stopPointPlayback();
      else startPointPlayback();
    });

    if (el.routeToggleBtn) {
      el.routeToggleBtn.addEventListener("click", () => {
        state.routeEnabled = !state.routeEnabled;
        el.routeToggleBtn.classList.toggle("active", state.routeEnabled);
        el.routeToggleBtn.textContent = state.routeEnabled ? "🗺 Скрыть" : "🗺 Маршрут";
        renderRoute(state.points);
      });
    }
  }

  const params = new URLSearchParams(window.location.search);
  const urlIin = (params.get("iin") || "").trim();
  if (urlIin) {
    sessionStorage.setItem("client_iin", urlIin);
  }
  state.iin = urlIin || (sessionStorage.getItem("client_iin") || "").trim();
  if (!state.iin) {
    el.clientMsg.textContent = "Используйте адрес: /api/client?iin=...";
    return;
  }

  if (el.clientIin) el.clientIin.textContent = CLIENT_PANEL_LABEL;
  if (el.clientIinTop) el.clientIinTop.textContent = CLIENT_TOP_LABEL;
  bindUi();
  reloadClient().catch((e) => {
    console.error(e);
    el.clientMsg.textContent = `Ошибка: ${e.message}`;
  });
})();
