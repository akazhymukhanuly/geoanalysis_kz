from __future__ import annotations

import asyncio
import json
import math
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
import duckdb
import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config import (
    BASE_DIR,
    CACHE_DIR,
    DATA_DIR,
    DB_PATH,
    DB_URL as CONFIG_DB_URL,
    INFRA_XLSX_PATH,
    INFRA_PARQUET_PATH,
    KZ_GEOJSON_PATH,
    MAX_CACHE_ZOOM,
    MAX_SERVE_ZOOM,
    PARQUET_PATH,
    TRANSFERS_PARQUET_PATH,
    PROXY_TILE_URL,
    STATIC_DIR,
    TEMPLATES_DIR,
    UPSTREAM_TILE_URL,
    ENABLE_INFRA as CONFIG_ENABLE_INFRA,
)
import build_map_parquet_from_db as _build_module

_DB_WRITE_LOCK = threading.Lock()
ADMIN_TOKEN: str = os.getenv("ADMIN_TOKEN", "").strip()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global INIT_ERROR
    try:
        init_all()
        _start_clients_reload_scheduler()
        INIT_ERROR = None
    except Exception as exc:
        INIT_ERROR = str(exc)
    yield


app = FastAPI(title="Geo Analytics", lifespan=_lifespan)

@app.middleware("http")
async def _add_charset(request, call_next):
    response = await call_next(request)
    ct = response.headers.get("content-type", "")
    if "javascript" in ct and "charset" not in ct:
        response.headers["content-type"] = ct + "; charset=utf-8"
    return response

app.mount("/staticgeo", StaticFiles(directory=str(STATIC_DIR)), name="staticgeo")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

try:
    con = duckdb.connect(database=str(DB_PATH))
except Exception as exc:
    print(f"[duckdb] DB file is locked ({exc}); fallback to in-memory DB", flush=True)
    con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4;")
con.execute("PRAGMA enable_object_cache=true;")

INIT_ERROR: Optional[str] = None
BOUNDARIES: dict = {"type": "FeatureCollection", "features": []}
RAYON_BY_ID: dict[str, dict] = {}
RAYONS_BY_OBLAST: dict[str, list] = {}
HAS_EVENT_DATE = False
INFRA_POINTS: list[dict] = []
ENABLE_INFRA = CONFIG_ENABLE_INFRA
INFRA_SOURCE = "none"
CLIENT_RELOAD_DAILY_TIME = os.getenv("CLIENT_RELOAD_DAILY_TIME", "10:30").strip()
CLIENT_RELOAD_ENABLED = True
CLIENT_RELOAD_LOCK = threading.Lock()
LAST_CLIENT_RELOAD_AT: Optional[str] = None
CLIENT_RELOAD_THREAD_STARTED = False
BUILD_BEFORE_RELOAD = os.getenv("BUILD_BEFORE_RELOAD", "1").strip().lower() in ("1", "true", "yes", "on")
SOURCE_QUERY = os.getenv("SOURCE_QUERY", "")
LAST_BUILD_AT: Optional[str] = None
LAST_BUILD_ERROR: Optional[str] = None
TRANSFERS_HAS_TRANS_DATE = False
HAS_SESSION_DATA = False  # True when clients parquet carries has_transfer columns


def _sql_path(p: Path) -> str:
    return str(p).replace("\\", "/")


def _pick_col(columns: list[tuple], *candidates: str) -> Optional[str]:
    lowered = {row[0].lower(): row[0] for row in columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def _infer_infra_type(rubric: Optional[str], name: Optional[str]) -> str:
    text = f"{rubric or ''} {name or ''}".lower()
    checks = [
        ("supermarket", ["супермар", "market", "гипермар", "продукт"]),
        ("food", ["кафе", "ресторан", "кофе", "столов", "бар", "pizza", "бургер"]),
        ("medical", ["мед", "клиник", "апте", "стомат", "hospital"]),
        ("beauty", ["салон", "beauty", "космет", "маник"]),
        ("kids", ["дет", "kids", "baby", "школ", "садик"]),
        ("fitness", ["fit", "спорт", "gym", "йога"]),
        ("gas", ["азс", "заправ", "fuel", "gas"]),
        ("travel", ["авиа", "тур", "hotel", "гостин", "отель"]),
        ("fashion", ["одежд", "бутик", "fashion", "shoes"]),
        ("furniture", ["мебел", "интерьер"]),
        ("education", ["универ", "колледж", "образов", "edu"]),
    ]
    for t, parts in checks:
        if any(p in text for p in parts):
            return t
    return "default"


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _in_kz_bbox(lat: float, lon: float) -> bool:
    return 40.0 <= lat <= 56.0 and 46.0 <= lon <= 88.0


def _normalize_infra_points(points: list[dict]) -> list[dict]:
    if not points:
        return points
    normal = 0
    swapped = 0
    for p in points:
        lat = float(p["lat"])
        lon = float(p["lon"])
        if _in_kz_bbox(lat, lon):
            normal += 1
        if _in_kz_bbox(lon, lat):
            swapped += 1
    if swapped > normal * 2:
        for p in points:
            p["lat"], p["lon"] = float(p["lon"]), float(p["lat"])
        print(f"[infra] detected swapped coordinates, applied auto-swap ({normal} -> {swapped})", flush=True)
    return points


def _ring_bbox(ring: list[list[float]]) -> tuple[float, float, float, float]:
    xs = [p[0] for p in ring]
    ys = [p[1] for p in ring]
    return min(xs), min(ys), max(xs), max(ys)


def _feature_bbox(feature: dict) -> Optional[tuple[float, float, float, float]]:
    geom = (feature or {}).get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if gtype == "Polygon":
        polys = [coords] if isinstance(coords, list) else []
    elif gtype == "MultiPolygon":
        polys = coords if isinstance(coords, list) else []
    else:
        polys = []
    if not polys:
        return None
    bboxes = []
    for poly in polys:
        if not poly:
            continue
        ring_boxes = [_ring_bbox(r) for r in poly if r]
        if not ring_boxes:
            continue
        minx = min(b[0] for b in ring_boxes)
        miny = min(b[1] for b in ring_boxes)
        maxx = max(b[2] for b in ring_boxes)
        maxy = max(b[3] for b in ring_boxes)
        bboxes.append((minx, miny, maxx, maxy))
    if not bboxes:
        return None
    return (
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    )


def _point_on_segment(px: float, py: float, x1: float, y1: float, x2: float, y2: float) -> bool:
    cross = (px - x1) * (y2 - y1) - (py - y1) * (x2 - x1)
    if abs(cross) > 1e-12:
        return False
    dot = (px - x1) * (x2 - x1) + (py - y1) * (y2 - y1)
    if dot < 0:
        return False
    sq_len = (x2 - x1) ** 2 + (y2 - y1) ** 2
    return dot <= sq_len


def _point_in_ring(x: float, y: float, ring: list[list[float]]) -> bool:
    inside = False
    n = len(ring)
    if n < 3:
        return False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if _point_on_segment(x, y, xi, yi, xj, yj):
            return True
        cond = ((yi > y) != (yj > y)) and (
            x < (xj - xi) * (y - yi) / ((yj - yi) if (yj - yi) != 0 else 1e-18) + xi
        )
        if cond:
            inside = not inside
        j = i
    return inside


def _point_in_polygon(x: float, y: float, poly: list[list[list[float]]]) -> bool:
    if not poly or not poly[0]:
        return False
    if not _point_in_ring(x, y, poly[0]):
        return False
    for hole in poly[1:]:
        if hole and _point_in_ring(x, y, hole):
            return False
    return True


def _point_in_feature(lon: float, lat: float, feature: dict) -> bool:
    geom = (feature or {}).get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if gtype == "Polygon":
        polys = [coords] if isinstance(coords, list) else []
    elif gtype == "MultiPolygon":
        polys = coords if isinstance(coords, list) else []
    else:
        return False
    for poly in polys:
        if _point_in_polygon(lon, lat, poly):
            return True
    return False


def _to_str_clean(v: object) -> str:
    return str(v).strip()


def _geom_to_multipolygon_coords(geometry: Optional[dict]) -> list:
    if not geometry:
        return []
    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    if gtype == "Polygon" and isinstance(coords, list):
        return [coords]
    if gtype == "MultiPolygon" and isinstance(coords, list):
        return coords
    return []


def _append_period_clause(
    where: list[str],
    params: list,
    period: str,
    anchor_date: Optional[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    if not HAS_EVENT_DATE:
        return
    start, end = _period_bounds(period, anchor_date, start_date, end_date)
    where.append("event_date BETWEEN ? AND ?")
    params.extend([start.isoformat(), end.isoformat()])


TRANSFERS_SOURCE_QUERY = os.getenv("TRANSFERS_QUERY", "")


def _build_clients_parquet() -> None:
    global LAST_BUILD_AT, LAST_BUILD_ERROR
    db_url = CONFIG_DB_URL or os.getenv("DB_URL", "")
    if not db_url:
        print("[build] BUILD_BEFORE_RELOAD=1 but DB_URL is not set — skipping build", flush=True)
        return

    auth_query = SOURCE_QUERY or _build_module.DEFAULT_QUERY
    transfers_query = TRANSFERS_SOURCE_QUERY or _build_module.DEFAULT_TRANSFERS_QUERY
    session_transfers_query = _build_module.DEFAULT_SESSION_TRANSFERS_QUERY
    window = _build_module.SESSION_WINDOW_MINUTES

    print(f"[build] run_enriched: auth + transfers (session window ±{window} min)", flush=True)
    _build_module.run_enriched(
        db_url=db_url,
        auth_query=auth_query,
        transfers_query=transfers_query,
        session_transfers_query=session_transfers_query,
        out_parquet=PARQUET_PATH,
        transfers_parquet=TRANSFERS_PARQUET_PATH,
        geojson_path=KZ_GEOJSON_PATH,
        window_minutes=window,
    )

    LAST_BUILD_AT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LAST_BUILD_ERROR = None
    print(f"[build] enriched parquet build done at {LAST_BUILD_AT}", flush=True)


def _reload_clients_table(reason: str = "manual") -> None:
    global LAST_CLIENT_RELOAD_AT, LAST_BUILD_ERROR
    with CLIENT_RELOAD_LOCK:
        if BUILD_BEFORE_RELOAD and reason in ("daily_scheduler", "admin_api"):
            try:
                _build_clients_parquet()
            except Exception as exc:
                LAST_BUILD_ERROR = str(exc)
                print(f"[build] parquet build failed: {exc}; will reload existing parquet", flush=True)
        with _DB_WRITE_LOCK:
            _init_clients()
            _init_transfers()
        LAST_CLIENT_RELOAD_AT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[clients+transfers] reloaded from parquet ({reason}) at {LAST_CLIENT_RELOAD_AT}", flush=True)


def _seconds_until_next_daily_run(hh: int, mm: int) -> float:
    now = datetime.now()
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


def _start_clients_reload_scheduler() -> None:
    global CLIENT_RELOAD_THREAD_STARTED
    if CLIENT_RELOAD_THREAD_STARTED or not CLIENT_RELOAD_ENABLED:
        return

    try:
        hh_s, mm_s = CLIENT_RELOAD_DAILY_TIME.split(":", 1)
        hh = int(hh_s)
        mm = int(mm_s)
    except Exception:
        print(f"[clients] bad CLIENT_RELOAD_DAILY_TIME={CLIENT_RELOAD_DAILY_TIME}; scheduler disabled", flush=True)
        return
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        print(f"[clients] invalid scheduler time {CLIENT_RELOAD_DAILY_TIME}; scheduler disabled", flush=True)
        return

    def _loop() -> None:
        print(f"[clients] daily reload scheduler started at {CLIENT_RELOAD_DAILY_TIME}", flush=True)
        while True:
            try:
                time.sleep(max(1.0, _seconds_until_next_daily_run(hh, mm)))
                _reload_clients_table(reason="daily_scheduler")
            except Exception as exc:
                print(f"[clients] daily reload failed: {exc}", flush=True)
                time.sleep(60)

    t = threading.Thread(target=_loop, name="clients-daily-reload", daemon=True)
    t.start()
    CLIENT_RELOAD_THREAD_STARTED = True


def _load_boundaries() -> None:
    global BOUNDARIES, RAYON_BY_ID, RAYONS_BY_OBLAST

    if not KZ_GEOJSON_PATH.exists():
        raise RuntimeError(f"GeoJSON not found: {KZ_GEOJSON_PATH}")

    raw = json.loads(KZ_GEOJSON_PATH.read_text(encoding="utf-8"))
    feats = raw.get("features") or []

    rayon_by_id: dict[str, dict] = {}
    rayons_by_oblast: dict[str, list] = {}
    oblast_multipolys_fallback: dict[str, list] = {}
    oblast_feature_by_name: dict[str, dict] = {}

    for f in feats:
        props = f.get("properties") or {}
        full_id = props.get("full_id")
        oblast = props.get("oblast_kk")
        name = props.get("name_kk")

        if not full_id or not oblast or not name:
            continue

        full_id_s = _to_str_clean(full_id)
        oblast_s = _to_str_clean(oblast)
        name_s = _to_str_clean(name)
        if not full_id_s or not oblast_s or not name_s:
            continue

        feat = {
            "type": "Feature",
            "properties": {
                "full_id": full_id_s,
                "oblast_kk": oblast_s,
                "name_kk": name_s,
            },
            "geometry": f.get("geometry"),
        }
        is_oblast_feature = (name_s == oblast_s)
        if is_oblast_feature and feat.get("geometry"):
            # Prefer top-level oblast geometry for ALL mode (no inner rayon borders).
            oblast_feature_by_name[oblast_s] = feat
            continue

        rayon_by_id[full_id_s] = feat
        rayons_by_oblast.setdefault(oblast_s, []).append(feat)
        oblast_multipolys_fallback.setdefault(oblast_s, []).extend(_geom_to_multipolygon_coords(feat.get("geometry")))

    if not rayon_by_id:
        raise RuntimeError("No usable rayon features (full_id/oblast_kk/name_kk) found in GeoJSON")

    oblast_features = []
    if oblast_feature_by_name:
        oblast_features = [
            oblast_feature_by_name[ob]
            for ob in sorted(oblast_feature_by_name.keys())
        ]
    else:
        # Fallback when source GeoJSON has no dedicated oblast features.
        # Prefer dissolved geometry (no inner rayon borders) if shapely is available.
        try:
            from shapely.geometry import shape, mapping
            from shapely.ops import unary_union

            for ob in sorted(rayons_by_oblast.keys()):
                feats = rayons_by_oblast.get(ob) or []
                geoms = []
                for rf in feats:
                    g = rf.get("geometry")
                    if g:
                        geoms.append(shape(g))
                if not geoms:
                    continue
                dissolved = unary_union(geoms)
                ff = {
                    "type": "Feature",
                    "properties": {
                        "oblast_kk": ob,
                        "name_kk": ob,
                        "full_id": f"oblast::{ob}",
                    },
                    "geometry": mapping(dissolved),
                }
                oblast_features.append(ff)
        except Exception:
            for ob in sorted(rayons_by_oblast.keys()):
                polys = oblast_multipolys_fallback.get(ob) or []
                if not polys:
                    continue
                ff = {
                    "type": "Feature",
                    "properties": {
                        "oblast_kk": ob,
                        "name_kk": ob,
                        "full_id": f"oblast::{ob}",
                    },
                    "geometry": {"type": "MultiPolygon", "coordinates": polys},
                }
                oblast_features.append(ff)

    for ob in rayons_by_oblast:
        rayons_by_oblast[ob].sort(key=lambda x: x["properties"]["name_kk"])

    BOUNDARIES = {"type": "FeatureCollection", "features": oblast_features}
    RAYON_BY_ID = rayon_by_id
    RAYONS_BY_OBLAST = rayons_by_oblast


def _init_clients() -> None:
    global HAS_EVENT_DATE, HAS_SESSION_DATA
    if not PARQUET_PATH.exists():
        raise RuntimeError(f"Parquet not found: {PARQUET_PATH}. Build it first with make_test_parquet.py")

    parquet = _sql_path(PARQUET_PATH)
    columns = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet}')").fetchall()

    lat_col = _pick_col(columns, "lat", "latitude")
    lon_col = _pick_col(columns, "lon", "lng", "longitude")
    iin_col = _pick_col(columns, "iin", "client_iin", "customer_iin", "clientid")
    hour_col = _pick_col(columns, "hour", "hh")
    dt_col = _pick_col(
        columns,
        "updated_at",
        "datetimestamp",
        "date_timestamp",
        "event_ts",
        "datetime",
        "dt",
        "timestamp",
        "ts",
        "event_time",
        "event_dt",
    )
    oblast_col = _pick_col(columns, "oblast_kk")
    rayon_id_col = _pick_col(columns, "rayon_id")
    rayon_name_col = _pick_col(columns, "rayon_name")
    device_type_col = _pick_col(columns, "device_type", "device_model", "platform", "os", "device_name")
    event_date_col = _pick_col(columns, "event_date", "date")
    # Session-join columns (present only in enriched build)
    has_tr_col = _pick_col(columns, "has_transfer")
    sess_count_col = _pick_col(columns, "session_transfer_count")
    sess_kzt_col = _pick_col(columns, "session_transfer_kzt")
    sess_purpose_col = _pick_col(columns, "session_purpose_cat")

    if not lat_col or not lon_col:
        raise RuntimeError("lat/lon columns are required in clients parquet")
    if not oblast_col or not rayon_id_col:
        raise RuntimeError(
            "Parquet must contain enriched columns oblast_kk and rayon_id. Run make_test_parquet.py first."
        )

    if hour_col:
        hour_expr = f"CAST({hour_col} AS INTEGER)"
    elif dt_col:
        hour_expr = f"CAST(EXTRACT('hour' FROM CAST({dt_col} AS TIMESTAMP)) AS INTEGER)"
    elif iin_col:
        hour_expr = (
            f"CAST(abs(hash(CAST({iin_col} AS VARCHAR) || '|' || CAST({lat_col} AS VARCHAR) || '|' || "
            f"CAST({lon_col} AS VARCHAR))) % 24 AS INTEGER)"
        )
    else:
        hour_expr = "0"

    iin_expr = f"CAST({iin_col} AS VARCHAR)" if iin_col else "NULL"
    rayon_name_expr = f"CAST({rayon_name_col} AS VARCHAR)" if rayon_name_col else "NULL"
    device_type_expr = f"CAST({device_type_col} AS VARCHAR)" if device_type_col else "NULL::VARCHAR"
    event_ts_expr = f"CAST({dt_col} AS TIMESTAMP)" if dt_col else "NULL::TIMESTAMP"

    if event_date_col:
        event_date_expr = f"CAST({event_date_col} AS DATE)"
    elif dt_col:
        event_date_expr = f"CAST(CAST({dt_col} AS TIMESTAMP) AS DATE)"
    else:
        event_date_expr = "NULL::DATE"

    # Session-enrichment expressions (fallback to neutral values when column absent)
    has_tr_expr = f"TRY_CAST({has_tr_col} AS BOOLEAN)" if has_tr_col else "FALSE"
    sess_count_expr = f"COALESCE(TRY_CAST({sess_count_col} AS INTEGER), 0)" if sess_count_col else "0"
    sess_kzt_expr = f"COALESCE(TRY_CAST({sess_kzt_col} AS DOUBLE), 0.0)" if sess_kzt_col else "0.0"
    sess_purpose_expr = f"CAST({sess_purpose_col} AS VARCHAR)" if sess_purpose_col else "NULL::VARCHAR"

    for stmt in ["DROP VIEW clients;", "DROP TABLE clients;"]:
        try:
            con.execute(stmt)
        except Exception:
            pass

    con.execute(f"""
    CREATE OR REPLACE TABLE clients AS
    SELECT
      CAST({lat_col} AS DOUBLE) AS lat,
      CAST({lon_col} AS DOUBLE) AS lon,
      {iin_expr} AS iin,
      {event_ts_expr} AS event_ts,
      {hour_expr} AS hour,
      {event_date_expr} AS event_date,
      CAST({oblast_col} AS VARCHAR) AS oblast_kk,
      CAST({rayon_id_col} AS VARCHAR) AS rayon_id,
      {rayon_name_expr} AS rayon_name,
      {device_type_expr} AS device_type,
      COALESCE({has_tr_expr}, FALSE) AS has_transfer,
      {sess_count_expr} AS session_transfer_count,
      {sess_kzt_expr} AS session_transfer_kzt,
      {sess_purpose_expr} AS session_purpose_cat
    FROM read_parquet('{parquet}')
    WHERE {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL;
    """)

    non_null_dates = con.execute("SELECT COUNT(*) FROM clients WHERE event_date IS NOT NULL").fetchone()[0]
    HAS_EVENT_DATE = int(non_null_dates or 0) > 0

    session_hits = con.execute("SELECT COUNT(*) FROM clients WHERE has_transfer").fetchone()[0]
    HAS_SESSION_DATA = int(session_hits or 0) > 0
    print(f"[clients] has_session_data={HAS_SESSION_DATA}  session_hits={session_hits:,}", flush=True)


def _load_infra_points() -> None:
    global INFRA_POINTS, INFRA_SOURCE
    INFRA_POINTS = []
    INFRA_SOURCE = "none"

    if INFRA_PARQUET_PATH.exists():
        try:
            p = _sql_path(INFRA_PARQUET_PATH)
            rows = con.execute(
                f"""
                SELECT
                  CAST(name AS VARCHAR) AS name,
                  CAST(address AS VARCHAR) AS address,
                  CAST(type AS VARCHAR) AS type,
                  TRY_CAST(rating AS DOUBLE) AS rating,
                  TRY_CAST(reviews AS BIGINT) AS reviews,
                  TRY_CAST(lat AS DOUBLE) AS lat,
                  TRY_CAST(lon AS DOUBLE) AS lon
                FROM read_parquet('{p}')
                WHERE TRY_CAST(lat AS DOUBLE) BETWEEN -90 AND 90
                  AND TRY_CAST(lon AS DOUBLE) BETWEEN -180 AND 180
                LIMIT 200000
                """
            ).fetchall()
            points = []
            for r in rows:
                name, address, t, rating, reviews, lat, lon = r
                points.append(
                    {
                        "name": str(name) if name else "Объект",
                        "address": str(address) if address else None,
                        "rubric": None,
                        "type": str(t) if t else "default",
                        "rating": float(rating) if rating is not None else None,
                        "reviews": int(reviews) if reviews is not None else None,
                        "lat": float(lat),
                        "lon": float(lon),
                    }
                )
            INFRA_POINTS = points
            INFRA_POINTS = _normalize_infra_points(INFRA_POINTS)
            INFRA_SOURCE = "parquet"
            print(f"[infra] loaded {len(INFRA_POINTS)} points from parquet: {INFRA_PARQUET_PATH}", flush=True)
            return
        except Exception as exc:
            print(f"[infra] parquet load failed: {exc}", flush=True)

    if not INFRA_XLSX_PATH.exists():
        print(f"[infra] no parquet/xlsx source found ({INFRA_PARQUET_PATH}, {INFRA_XLSX_PATH})", flush=True)
        return

    x = _sql_path(INFRA_XLSX_PATH)
    try:
        rows = con.execute(f"""
        SELECT
          COALESCE("составное наименование (name_ex.primary)", name, full_name) AS name,
          "полный адрес с городом" AS address,
          "категории (рубрики)" AS rubric,
          TRY_CAST("статистика отзывов (reviews.general_rating)" AS DOUBLE) AS rating,
          TRY_CAST("статистика отзывов (reviews.general_review_count)" AS BIGINT) AS reviews,
          TRY_CAST("координаты (lon, lat) (point.lat)" AS DOUBLE) AS lat,
          TRY_CAST("координаты (lon, lat) (point.lon)" AS DOUBLE) AS lon
        FROM read_xlsx('{x}', all_varchar=true)
        WHERE TRY_CAST("координаты (lon, lat) (point.lat)" AS DOUBLE) BETWEEN -90 AND 90
          AND TRY_CAST("координаты (lon, lat) (point.lon)" AS DOUBLE) BETWEEN -180 AND 180
        LIMIT 120000
        """).fetchall()
    except Exception as exc:
        print(f"[infra] xlsx load failed: {exc}", flush=True)
        return

    points = []
    for r in rows:
        name, address, rubric, rating, reviews, lat, lon = r
        if lat is None or lon is None:
            continue
        t = _infer_infra_type(rubric, name)
        points.append(
            {
                "name": str(name) if name else "Объект",
                "address": str(address) if address else None,
                "rubric": str(rubric) if rubric else None,
                "type": t,
                "rating": float(rating) if rating is not None else None,
                "reviews": int(reviews) if reviews is not None else None,
                "lat": float(lat),
                "lon": float(lon),
            }
        )
    INFRA_POINTS = points
    INFRA_POINTS = _normalize_infra_points(INFRA_POINTS)
    INFRA_SOURCE = "xlsx"
    print(f"[infra] loaded {len(INFRA_POINTS)} points from xlsx: {INFRA_XLSX_PATH}", flush=True)


_PURPOSE_CAT_SQL = """
    CASE
        WHEN lower(CAST({p} AS VARCHAR)) LIKE '%зарубеж%' THEN 'p2p_abroad'
        WHEN lower(CAST({p} AS VARCHAR)) LIKE '%брокерск%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%invest%'   THEN 'invest'
        WHEN lower(CAST({p} AS VARCHAR)) LIKE '%конвертац%' THEN 'conversion'
        WHEN lower(CAST({p} AS VARCHAR)) LIKE '%открыт%'
         AND lower(CAST({p} AS VARCHAR)) LIKE '%депозит%'  THEN 'deposit'
        WHEN lower(CAST({p} AS VARCHAR)) LIKE '%iban%'     THEN 'iban_external'
        WHEN lower(CAST({p} AS VARCHAR)) LIKE '%p2p%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%visa alias%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%номеру телефона%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%карты на карту%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%родительск%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%open api%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%cash by code%'
          OR lower(CAST({p} AS VARCHAR)) LIKE '%возврат%'  THEN 'p2p_local'
        WHEN lower(CAST({p} AS VARCHAR)) LIKE '%бюджет%'   THEN 'budget'
        ELSE 'transfer'
    END
"""


def _init_transfers() -> None:
    global TRANSFERS_HAS_TRANS_DATE
    if not TRANSFERS_PARQUET_PATH.exists():
        print(f"[transfers] parquet not found: {TRANSFERS_PARQUET_PATH} — table will be empty", flush=True)
        con.execute("DROP TABLE IF EXISTS transfers;")
        con.execute("""
            CREATE TABLE transfers (
                lat DOUBLE, lon DOUBLE, iin VARCHAR, amount_kzt DOUBLE,
                amount_cur DOUBLE, currency VARCHAR, direction VARCHAR,
                purpose VARCHAR, purpose_cat VARCHAR, bank_commission DOUBLE,
                partner_id VARCHAR, account VARCHAR, client_name VARCHAR,
                trans_ts TIMESTAMP, trans_date VARCHAR, hour BIGINT,
                oblast_kk VARCHAR, rayon_id VARCHAR, rayon_name VARCHAR
            )
        """)
        TRANSFERS_HAS_TRANS_DATE = False
        return

    p = _sql_path(TRANSFERS_PARQUET_PATH)
    cols_raw = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}') LIMIT 0").fetchall()
    col_map = {row[0].lower(): row[0] for row in cols_raw}

    def _pc(*candidates: str) -> str:
        for c in candidates:
            if c.lower() in col_map:
                return col_map[c.lower()]
        return candidates[0]

    lat_c        = _pc("lat")
    lon_c        = _pc("lon")
    iin_c        = _pc("iin")
    amtkzt_c     = _pc("amount_kzt")
    amtcur_c     = _pc("amount_cur")
    cur_c        = _pc("target_currency", "currency")
    dir_c        = _pc("direction")
    purp_c       = _pc("purpose")
    ts_c         = _pc("trans_ts", "trans_date")
    comm_c       = _pc("bank_commission", "commission")
    partner_c    = _pc("partner_id", "partner")
    account_c    = _pc("account")
    cname_c      = _pc("client_name", "name")
    oblast_c     = _pc("oblast_kk", "oblast")
    rayon_id_c   = _pc("rayon_id")
    rayon_name_c = _pc("rayon_name")

    purpose_cat_expr = _PURPOSE_CAT_SQL.format(p=purp_c)

    con.execute("DROP TABLE IF EXISTS transfers;")
    con.execute(f"""
    CREATE TABLE transfers AS
    SELECT
        TRY_CAST({lat_c}        AS DOUBLE)    AS lat,
        TRY_CAST({lon_c}        AS DOUBLE)    AS lon,
        CAST({iin_c}            AS VARCHAR)   AS iin,
        TRY_CAST({amtkzt_c}     AS DOUBLE)    AS amount_kzt,
        TRY_CAST({amtcur_c}     AS DOUBLE)    AS amount_cur,
        CAST({cur_c}            AS VARCHAR)   AS currency,
        CAST({dir_c}            AS VARCHAR)   AS direction,
        CAST({purp_c}           AS VARCHAR)   AS purpose,
        {purpose_cat_expr}                    AS purpose_cat,
        TRY_CAST({comm_c}       AS DOUBLE)    AS bank_commission,
        CAST({partner_c}        AS VARCHAR)   AS partner_id,
        CAST({account_c}        AS VARCHAR)   AS account,
        CAST({cname_c}          AS VARCHAR)   AS client_name,
        TRY_CAST({ts_c}         AS TIMESTAMP) AS trans_ts,
        CAST(TRY_CAST({ts_c}    AS DATE) AS VARCHAR) AS trans_date,
        CAST(EXTRACT('hour' FROM TRY_CAST({ts_c} AS TIMESTAMP)) AS BIGINT) AS hour,
        CAST({oblast_c}         AS VARCHAR)   AS oblast_kk,
        CAST({rayon_id_c}       AS VARCHAR)   AS rayon_id,
        CAST({rayon_name_c}     AS VARCHAR)   AS rayon_name
    FROM read_parquet('{p}')
    WHERE TRY_CAST({lat_c} AS DOUBLE) IS NOT NULL
      AND TRY_CAST({lon_c} AS DOUBLE) IS NOT NULL
    """)

    cnt = con.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
    non_null = con.execute("SELECT COUNT(*) FROM transfers WHERE trans_ts IS NOT NULL").fetchone()[0]
    TRANSFERS_HAS_TRANS_DATE = int(non_null or 0) > 0
    print(f"[transfers] loaded {cnt} rows, has_trans_date={TRANSFERS_HAS_TRANS_DATE}", flush=True)


def _reload_transfers_table() -> None:
    with _DB_WRITE_LOCK:
        _init_transfers()


def _append_transfers_period_clause(
    where: list[str],
    params: list,
    period: str,
    anchor_date: Optional[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    if not TRANSFERS_HAS_TRANS_DATE:
        return
    start, end = _period_bounds(period, anchor_date, start_date, end_date)
    where.append("trans_ts BETWEEN ? AND ?")
    params.extend([start.isoformat(), end.isoformat() + " 23:59:59"])


def init_all() -> None:
    _load_boundaries()
    _reload_clients_table(reason="startup")
    _reload_transfers_table()
    # Infra load is disabled by default to keep startup fully offline and fast.
    if ENABLE_INFRA:
        _load_infra_points()


def _ensure_ready() -> None:
    if INIT_ERROR:
        raise HTTPException(status_code=503, detail={"error": "service_not_ready", "message": INIT_ERROR})


def _period_bounds(
    period: str,
    anchor_date: Optional[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> tuple[date, date]:
    today = date.today()
    if anchor_date:
        try:
            base = datetime.strptime(anchor_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "bad_date", "message": str(exc)}) from exc
    else:
        base = today

    p = period.lower()
    if p == "today":
        end = base
        start = end
    elif p == "day":
        end = base - timedelta(days=1)
        start = end
    elif p == "week":
        end = base - timedelta(days=1)
        start = end - timedelta(days=6)
    elif p == "month":
        end = base - timedelta(days=1)
        start = end - timedelta(days=29)
    elif p == "custom":
        # Frontend should send both dates; keep a safe fallback to avoid 400 on stale UI.
        if not end_date:
            end_date = (base - timedelta(days=1)).isoformat()
        if not start_date:
            start_date = (datetime.strptime(end_date, "%Y-%m-%d").date() - timedelta(days=6)).isoformat()
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "bad_date", "message": str(exc)}) from exc
        if end < start:
            raise HTTPException(
                status_code=400,
                detail={"error": "bad_date_range", "message": "end_date must be greater or equal to start_date"},
            )
    else:
        raise HTTPException(
            status_code=400,
            detail={"error": "bad_period", "message": "use day|week|month|custom"},
        )
    return start, end


@app.get("/")
async def root() -> RedirectResponse:
    return RedirectResponse(url="/global")


@app.get("/global")
async def global_page(request: Request):
    return templates.TemplateResponse("global.html", {"request": request}, media_type="text/html; charset=utf-8")


@app.get("/api/client")
@app.get("/client")
async def client_page(request: Request, iin: Optional[str] = None):
    return templates.TemplateResponse("client.html", {"request": request, "iin": iin or ""}, media_type="text/html; charset=utf-8")



@app.get("/healthz")
async def healthz():
    if INIT_ERROR:
        return JSONResponse(
            {
                "status": "degraded",
                "init_error": INIT_ERROR,
                "parquet_path": str(PARQUET_PATH),
                "geojson_path": str(KZ_GEOJSON_PATH),
            },
            status_code=503,
        )
    return {
        "status": "ok",
        "parquet_path": str(PARQUET_PATH),
        "geojson_path": str(KZ_GEOJSON_PATH),
        "parquet_exists": PARQUET_PATH.exists(),
        "geojson_exists": KZ_GEOJSON_PATH.exists(),
        "infra_points": len(INFRA_POINTS),
        "infra_enabled": ENABLE_INFRA,
        "infra_source": INFRA_SOURCE,
        "clients_reload_enabled": CLIENT_RELOAD_ENABLED,
        "clients_reload_daily_time": CLIENT_RELOAD_DAILY_TIME,
        "clients_reload_last_at": LAST_CLIENT_RELOAD_AT,
        "build_before_reload": BUILD_BEFORE_RELOAD,
        "build_last_at": LAST_BUILD_AT,
        "build_last_error": LAST_BUILD_ERROR,
    }


@app.get("/api/admin/reload-clients")
@app.post("/api/admin/reload-clients")
async def api_admin_reload_clients(request: Request):
    _ensure_ready()
    if ADMIN_TOKEN:
        token = request.headers.get("X-Admin-Token", "")
        if token != ADMIN_TOKEN:
            raise HTTPException(status_code=403, detail="forbidden")
    try:
        _reload_clients_table(reason="admin_api")
    except Exception as exc:
        return JSONResponse(
            {
                "status": "error",
                "message": str(exc),
                "last_reload_at": LAST_CLIENT_RELOAD_AT,
            },
            status_code=500,
        )
    return {
        "status": "ok",
        "message": "clients table reloaded from parquet",
        "last_reload_at": LAST_CLIENT_RELOAD_AT,
    }


@app.get("/api/oblasts")
async def api_oblasts():
    _ensure_ready()
    return JSONResponse(BOUNDARIES)


@app.get("/api/rayons")
async def api_rayons(oblast: str):
    _ensure_ready()
    return JSONResponse({"type": "FeatureCollection", "features": RAYONS_BY_OBLAST.get(oblast, [])})


@app.get("/api/dashboard")
async def api_dashboard(
    min_h: int = 0,
    max_h: int = 23,
    oblast: str = "ALL",
    rayon_id: Optional[str] = None,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    min_h = max(0, min_h)
    max_h = min(23, max_h)
    if max_h < min_h:
        min_h, max_h = max_h, min_h

    where = ["hour BETWEEN ? AND ?"]
    params: list = [min_h, max_h]

    effective_rayon_id: Optional[str] = None
    if rayon_id:
        meta = RAYON_BY_ID.get(rayon_id)
        if meta:
            feature_oblast = meta.get("properties", {}).get("oblast_kk")
            if not oblast or oblast == "ALL" or feature_oblast == oblast:
                effective_rayon_id = rayon_id

    if effective_rayon_id:
        where.append("rayon_id = ?")
        params.append(effective_rayon_id)
    elif oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)

    period_label = "все_доступные_данные"
    if HAS_EVENT_DATE:
        start, end = _period_bounds(period, anchor_date, start_date, end_date)
        where.append("event_date BETWEEN ? AND ?")
        params.extend([start.isoformat(), end.isoformat()])
        period_label = f"{start.isoformat()}..{end.isoformat()}"

    where_sql = " AND ".join(where)

    kpi = con.execute(
        f"""
        SELECT COUNT(*) AS events, APPROX_COUNT_DISTINCT(iin) AS users, COUNT(DISTINCT rayon_id) AS active_rayons
        FROM clients
        WHERE {where_sql}
        """,
        params,
    ).fetchone()

    hour_rows = con.execute(
        f"""
        SELECT hour, COUNT(*) AS events
        FROM clients
        WHERE {where_sql}
        GROUP BY hour
        ORDER BY hour
        """,
        params,
    ).fetchall()
    hours = [0] * 24
    for hr, cnt in hour_rows:
        h = int(hr)
        if 0 <= h <= 23:
            hours[h] = int(cnt or 0)
    top_hour = None
    top_hour_events = 0
    if any(hours):
        top_hour = max(range(24), key=lambda i: hours[i])
        top_hour_events = int(hours[top_hour])

    top_rows = con.execute(
        f"""
        SELECT rayon_id, COALESCE(MAX(rayon_name), rayon_id) AS rayon_name,
               COALESCE(MAX(oblast_kk), '-') AS oblast_kk,
               COUNT(*) AS events, APPROX_COUNT_DISTINCT(iin) AS users
        FROM clients
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id
        ORDER BY events DESC
        LIMIT 8
        """,
        params,
    ).fetchall()

    quality = con.execute(
        f"""
        SELECT COUNT(*) AS total, COUNT(rayon_id) AS tagged_rayon,
               COUNT(oblast_kk) AS tagged_oblast, COUNT(iin) AS with_iin
        FROM clients
        WHERE {where_sql}
        """,
        params,
    ).fetchone()

    total = int(quality[0] or 0)
    tagged_rayon = int(quality[1] or 0)
    tagged_oblast = int(quality[2] or 0)
    with_iin = int(quality[3] or 0)

    if oblast == "ALL":
        total_rayons = sum(len(v) for v in RAYONS_BY_OBLAST.values())
    else:
        total_rayons = len(RAYONS_BY_OBLAST.get(oblast, []))

    # Session conversion (only when enriched parquet available)
    session = None
    if HAS_SESSION_DATA:
        sess_row = con.execute(
            f"""
            SELECT
                SUM(CASE WHEN has_transfer THEN 1 ELSE 0 END) AS tr_sessions,
                COUNT(*) AS total_sessions,
                COALESCE(SUM(session_transfer_kzt), 0) AS total_kzt
            FROM clients WHERE {where_sql}
            """,
            params,
        ).fetchone()
        tr_sess = int(sess_row[0] or 0)
        tot_sess = int(sess_row[1] or 0)
        session = {
            "transfer_sessions": tr_sess,
            "total_sessions": tot_sess,
            "conversion_pct": round(tr_sess * 100 / tot_sess, 1) if tot_sess else 0.0,
            "total_transfer_kzt": float(sess_row[2] or 0),
        }

    return {
        "period_used": period_label,
        "has_event_date": HAS_EVENT_DATE,
        "has_session_data": HAS_SESSION_DATA,
        "session": session,
        "kpi": {
            "events": int(kpi[0] or 0),
            "users": int(kpi[1] or 0),
            "active_rayons": int(kpi[2] or 0),
            "top_hour": top_hour,
            "top_hour_events": top_hour_events,
            "total_rayons": total_rayons,
        },
        "hours": hours,
        "quality": {
            "total": total,
            "rayon_tagged_pct": round((tagged_rayon / total) * 100, 2) if total else 0.0,
            "oblast_tagged_pct": round((tagged_oblast / total) * 100, 2) if total else 0.0,
            "iin_filled_pct": round((with_iin / total) * 100, 2) if total else 0.0,
        },
        "top_rayons": [
            {
                "rayon_id": r[0],
                "rayon_name": r[1],
                "oblast_kk": r[2],
                "events": int(r[3]),
                "users": int(r[4]),
            }
            for r in top_rows
        ],
    }


@app.get("/api/points")
async def api_points(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    zoom: int,
    min_h: int = 0,
    max_h: int = 23,
    oblast: str = "ALL",
    rayon_id: Optional[str] = None,
    iin: Optional[str] = None,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    layer_mode: str = "all",
    end_date: Optional[str] = None,
):
    _ensure_ready()

    # normalize hours
    min_h = max(0, int(min_h))
    max_h = min(23, int(max_h))
    if max_h < min_h:
        min_h, max_h = max_h, min_h

    params: list = [min_lat, max_lat, min_lon, max_lon, min_h, max_h]
    where_clause = """
        lat BETWEEN ? AND ?
        AND lon BETWEEN ? AND ?
        AND hour BETWEEN ? AND ?
    """

    # --- Rayon / Oblast filter ---
    # Guard against stale rayon_id when user switches oblast in UI.
    effective_rayon_id: Optional[str] = None
    if rayon_id:
        feature = RAYON_BY_ID.get(rayon_id)
        if feature:
            feature_oblast = feature.get("properties", {}).get("oblast_kk")
            if not oblast or oblast == "ALL" or feature_oblast == oblast:
                effective_rayon_id = rayon_id

    if effective_rayon_id:
        where_clause += " AND rayon_id = ?"
        params.append(effective_rayon_id)

        # Optional bbox accelerator
        feature = RAYON_BY_ID.get(effective_rayon_id)
        if feature:
            fb = _feature_bbox(feature)  # (min_lon, min_lat, max_lon, max_lat)
            if fb:
                where_clause += " AND lon BETWEEN ? AND ? AND lat BETWEEN ? AND ?"
                params.extend([fb[0], fb[2], fb[1], fb[3]])
    elif oblast and oblast != "ALL":
        where_clause += " AND oblast_kk = ?"
        params.append(oblast)

    # --- IIN filter ---
    if iin:
        where_clause += " AND iin = ?"
        params.append(iin)

    # --- Layer mode filter (auth-only vs all) ---
    if layer_mode == "no_transfer":
        where_clause += " AND (has_transfer IS NULL OR has_transfer = FALSE)"

    # --- Period filter ---
    period_where: list[str] = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    if period_where:
        where_clause += " AND " + " AND ".join(period_where)
        params.extend(period_params)

    # --- Aggregation step selection ---
    # Avoid ROUND(lat/step)*step grid centroids at mid zoom — they appear as visible squares.
    # Switch to raw points early (zoom >= 11) so heatmap kernel blurs them naturally.
    if effective_rayon_id:
        step = None
    elif oblast and oblast != "ALL":
        step = None if zoom >= 11 else 0.04
    elif zoom < 7:
        step = 0.4
    elif zoom < 9:
        step = 0.12
    elif zoom < 11:
        step = 0.04
    else:
        step = None

    # --- Grid / clusters ---
    if step is not None:
        query = f"""
            SELECT
                ROUND(lat/{step})*{step} AS lat,
                ROUND(lon/{step})*{step} AS lon,
                COUNT(*) AS count,
                APPROX_COUNT_DISTINCT(iin) AS uniq,
                SUM(CASE WHEN has_transfer THEN 1 ELSE 0 END) AS tr
            FROM clients
            WHERE {where_clause}
            GROUP BY lat, lon
            LIMIT 12000
        """
        rows = con.execute(query, params).fetchall()
        return [
            {"lat": float(r[0]), "lon": float(r[1]), "count": int(r[2]), "uniq": int(r[3]), "tr": int(r[4])}
            for r in rows
        ]

    # --- Raw points ---
    query = f"""
        SELECT lat, lon, iin, hour,
               has_transfer,
               session_purpose_cat
        FROM clients
        WHERE {where_clause}
        LIMIT 30000
    """
    rows = con.execute(query, params).fetchall()
    return [
        {
            "lat": float(r[0]), "lon": float(r[1]),
            "iin": r[2], "hour": int(r[3]),
            "has_transfer": bool(r[4]),
            "purpose_cat": r[5],
        }
        for r in rows
    ]


@app.get("/api/stats/rayon")
async def api_stats_rayon(
    full_id: str,
    min_h: int = 0,
    max_h: int = 23,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    min_h = max(0, min_h)
    max_h = min(23, max_h)
    if max_h < min_h:
        min_h, max_h = max_h, min_h

    meta = RAYON_BY_ID.get(full_id)
    if not meta:
        return JSONResponse({"error": "rayon not found"}, status_code=404)

    props = meta["properties"]
    oblast_kk = props["oblast_kk"]
    name_kk = props["name_kk"]

    period_where: list = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    period_sql = (" AND " + " AND ".join(period_where)) if period_where else ""

    summary_row = con.execute(
        f"""
        SELECT COUNT(*) AS events, APPROX_COUNT_DISTINCT(iin) AS users
        FROM clients
        WHERE rayon_id = ? AND hour BETWEEN ? AND ?
          {period_sql}
        """,
        [full_id, min_h, max_h, *period_params],
    ).fetchone()

    hour_rows = con.execute(
        f"""
        SELECT hour, COUNT(*) AS events
        FROM clients
        WHERE rayon_id = ? AND hour BETWEEN ? AND ?
          {period_sql}
        GROUP BY hour ORDER BY hour
        """,
        [full_id, min_h, max_h, *period_params],
    ).fetchall()

    hours = [0] * 24
    for h, cnt in hour_rows:
        hi = int(h)
        if 0 <= hi <= 23:
            hours[hi] = int(cnt or 0)

    oblast_kpi = con.execute(
        f"""
        SELECT COUNT(*) AS events, APPROX_COUNT_DISTINCT(iin) AS users
        FROM clients
        WHERE oblast_kk = ? AND hour BETWEEN ? AND ? {period_sql}
        """,
        [oblast_kk, min_h, max_h, *period_params],
    ).fetchone()

    return JSONResponse(
        {
            "rayon": {
                "full_id": full_id,
                "name_kk": name_kk,
                "oblast_kk": oblast_kk,
                "events": int(summary_row[0] or 0),
                "users": int(summary_row[1] or 0),
                "hours": hours,
            },
            "oblast": {
                "oblast_kk": oblast_kk,
                "events": int(oblast_kpi[0] or 0),
                "users": int(oblast_kpi[1] or 0),
            },
        }
    )


@app.get("/api/client/summary")
async def api_client_summary(
    iin: str = Query(..., min_length=4),
    min_h: int = 0,
    max_h: int = 23,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    min_h = max(0, min_h)
    max_h = min(23, max_h)
    if max_h < min_h:
        min_h, max_h = max_h, min_h

    where = ["iin = ?", "hour BETWEEN ? AND ?"]
    params: list = [iin, min_h, max_h]
    _append_period_clause(where, params, period, anchor_date, start_date, end_date)
    where_sql = " AND ".join(where)

    totals = con.execute(
        f"""
        SELECT COUNT(*) AS events,
               COUNT(DISTINCT CAST(ROUND(lat, 4) AS VARCHAR) || '|' || CAST(ROUND(lon, 4) AS VARCHAR)) AS unique_places,
               MIN(event_ts) AS first_seen,
               MAX(event_ts) AS last_seen,
               COUNT(DISTINCT rayon_id) AS unique_rayons
        FROM clients
        WHERE {where_sql}
        """,
        params,
    ).fetchone()

    if not totals or int(totals[0] or 0) == 0:
        return JSONResponse({"error": "client not found"}, status_code=404)

    hour_rows = con.execute(
        f"""
        SELECT hour, COUNT(*)
        FROM clients
        WHERE {where_sql}
        GROUP BY hour
        ORDER BY hour
        """,
        params,
    ).fetchall()

    top_rayons_rows = con.execute(
        f"""
        SELECT COALESCE(rayon_name, rayon_id) AS rayon_name,
               COALESCE(oblast_kk, '-') AS oblast_kk,
               COUNT(*) AS events
        FROM clients
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id, rayon_name, oblast_kk
        ORDER BY events DESC
        LIMIT 5
        """,
        params,
    ).fetchall()

    hours = [0] * 24
    for hour, count in hour_rows:
        h = int(hour)
        if 0 <= h <= 23:
            hours[h] = int(count)

    tr = con.execute(
        f"""
        SELECT
            SUM(CASE WHEN has_transfer THEN 1 ELSE 0 END) AS tr_count,
            COALESCE(SUM(session_transfer_kzt), 0) AS tr_kzt,
            ROUND(SUM(CASE WHEN has_transfer THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS conv_pct
        FROM clients WHERE {where_sql}
        """,
        params,
    ).fetchone()

    tr_purpose = con.execute(
        f"""
        SELECT session_purpose_cat, COUNT(*) AS cnt,
               COALESCE(SUM(session_transfer_kzt), 0) AS vol
        FROM clients
        WHERE {where_sql} AND has_transfer AND session_purpose_cat IS NOT NULL
        GROUP BY session_purpose_cat ORDER BY cnt DESC LIMIT 6
        """,
        params,
    ).fetchall()

    return {
        "iin": iin,
        "events": int(totals[0] or 0),
        "unique_places": int(totals[1] or 0),
        "first_seen": str(totals[2]) if totals[2] is not None else None,
        "last_seen": str(totals[3]) if totals[3] is not None else None,
        "unique_rayons": int(totals[4] or 0),
        "hours": hours,
        "top_rayons": [{"rayon_name": r[0], "oblast_kk": r[1], "events": int(r[2])} for r in top_rayons_rows],
        "transfers": {
            "count": int(tr[0] or 0),
            "total_kzt": float(tr[1] or 0),
            "conversion_pct": float(tr[2] or 0),
            "by_purpose": [
                {"purpose_cat": r[0], "count": int(r[1]), "volume_kzt": float(r[2])}
                for r in tr_purpose
            ],
        },
    }


@app.get("/api/client/points")
async def api_client_points(
    iin: str = Query(..., min_length=4),
    limit: int = 5000,
    min_h: int = 0,
    max_h: int = 23,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    order: str = "desc",
):
    _ensure_ready()
    limit = max(100, min(30000, int(limit)))
    min_h = max(0, min_h)
    max_h = min(23, max_h)
    if max_h < min_h:
        min_h, max_h = max_h, min_h

    where = ["iin = ?", "hour BETWEEN ? AND ?"]
    params: list = [iin, min_h, max_h]
    _append_period_clause(where, params, period, anchor_date, start_date, end_date)
    where_sql = " AND ".join(where)
    order_sql = "ASC" if str(order).lower() == "asc" else "DESC"

    rows = con.execute(
        f"""
        SELECT lat, lon, hour, event_ts, rayon_name, oblast_kk,
               has_transfer, session_transfer_kzt, session_purpose_cat
        FROM clients
        WHERE {where_sql}
        ORDER BY event_ts {order_sql} NULLS LAST
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()
    return [
        {
            "lat": float(r[0]),
            "lon": float(r[1]),
            "hour": int(r[2]),
            "event_ts": str(r[3]) if r[3] is not None else None,
            "rayon_name": r[4],
            "oblast_kk": r[5],
            "has_transfer": bool(r[6]) if r[6] is not None else False,
            "transfer_kzt": float(r[7]) if r[7] else 0.0,
            "purpose_cat": r[8],
        }
        for r in rows
    ]


@app.get("/api/client/places")
async def api_client_places(
    iin: str = Query(..., min_length=4),
):
    _ensure_ready()

    # Always use full dataset for place detection — period filter is intentionally ignored.
    rows = con.execute(
        """
        SELECT
          ROUND(lat, 3) AS lat_bin,
          ROUND(lon, 3) AS lon_bin,
          AVG(lat) AS lat,
          AVG(lon) AS lon,
          COUNT(*) AS events,
          SUM(CASE
                WHEN EXTRACT('dow' FROM COALESCE(event_ts, CAST(event_date AS TIMESTAMP))) BETWEEN 1 AND 5
                 AND hour BETWEEN 9 AND 18
                THEN 1 ELSE 0 END
          ) AS work_events,
          SUM(CASE
                WHEN hour >= 22 OR hour < 6
                THEN 1 ELSE 0 END
          ) AS night_events,
          SUM(CASE
                WHEN EXTRACT('dow' FROM COALESCE(event_ts, CAST(event_date AS TIMESTAMP))) IN (0, 6)
                THEN 1 ELSE 0 END
          ) AS weekend_events,
          COUNT(DISTINCT COALESCE(event_date, CAST(event_ts AS DATE))) AS active_days
        FROM clients
        WHERE iin = ?
        GROUP BY lat_bin, lon_bin
        HAVING COUNT(*) >= 2
        ORDER BY events DESC
        LIMIT 120
        """,
        [iin],
    ).fetchall()

    if not rows:
        return []

    candidates = []
    for r in rows:
        events = int(r[4] or 0)
        if events <= 0:
            continue
        work_events = int(r[5] or 0)
        night_events = int(r[6] or 0)
        weekend_events = int(r[7] or 0)
        active_days = int(r[8] or 0)

        work_ratio = work_events / events
        night_ratio = night_events / events
        weekend_ratio = weekend_events / events
        regularity = min(1.0, active_days / 20.0) if active_days > 0 else 0.0

        home_score = 0.50 * night_ratio + 0.30 * regularity + 0.12 * weekend_ratio + 0.08 * (1.0 - work_ratio)
        work_score = 0.62 * work_ratio + 0.28 * regularity + 0.10 * (1.0 - weekend_ratio)
        hobby_score = 0.55 * weekend_ratio + 0.35 * (1.0 - regularity) + 0.10 * (1.0 - night_ratio)

        candidates.append(
            {
                "lat": float(r[2]),
                "lon": float(r[3]),
                "events": events,
                "active_days": active_days,
                "work_ratio": round(work_ratio, 4),
                "night_ratio": round(night_ratio, 4),
                "weekend_ratio": round(weekend_ratio, 4),
                "home_score": home_score,
                "work_score": work_score,
                "hobby_score": hobby_score,
            }
        )

    _DEDUP_M = 400

    def _near_any(lat: float, lon: float, placed: list) -> bool:
        return any(_haversine_m(lat, lon, p["lat"], p["lon"]) < _DEDUP_M for p in placed)

    ordered: list[dict] = []

    # Step 1: HOME — best home_score across all candidates.
    for c in sorted(candidates, key=lambda x: x["home_score"], reverse=True):
        best = dict(c)
        best["label"] = "home"
        best["confidence"] = round(max(0.05, min(0.99, c["home_score"])), 4)
        ordered.append(best)
        break

    # Step 2: WORK — best work_score, must not be near HOME.
    for c in sorted(candidates, key=lambda x: x["work_score"], reverse=True):
        if _near_any(c["lat"], c["lon"], ordered):
            continue
        best = dict(c)
        best["label"] = "work"
        best["confidence"] = round(max(0.05, min(0.99, c["work_score"])), 4)
        ordered.append(best)
        break

    # Step 3: HOBBY — best hobby_score, must not be near HOME or WORK.
    for c in sorted(candidates, key=lambda x: x["hobby_score"], reverse=True):
        if _near_any(c["lat"], c["lon"], ordered):
            continue
        best = dict(c)
        best["label"] = "hobby"
        best["confidence"] = round(max(0.05, min(0.99, c["hobby_score"])), 4)
        ordered.append(best)
        break

    # Step 4: FREQUENT — remaining locations not near any already placed.
    for c in sorted(candidates, key=lambda x: x["events"], reverse=True):
        if _near_any(c["lat"], c["lon"], ordered):
            continue
        item = dict(c)
        item["label"] = "frequent"
        item["confidence"] = round(max(0.05, min(0.99, max(c["home_score"], c["work_score"], c["hobby_score"]))), 4)
        ordered.append(item)
        if len(ordered) >= 6:
            break

    for item in ordered:
        item.pop("home_score", None)
        item.pop("work_score", None)
        item.pop("hobby_score", None)

    return ordered[:6]


@app.get("/api/transfers/points")
async def api_transfers_points(
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    zoom: int,
    purpose_cats: Optional[str] = None,
    metric: str = "count",
    period: str = "month",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    params: list = [min_lat, max_lat, min_lon, max_lon]
    where = "lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"

    if purpose_cats:
        cats = [c.strip() for c in purpose_cats.split(",") if c.strip()]
        if cats:
            placeholders = ",".join("?" * len(cats))
            where += f" AND purpose_cat IN ({placeholders})"
            params.extend(cats)

    period_where: list[str] = []
    period_params: list = []
    _append_transfers_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    if period_where:
        where += " AND " + " AND ".join(period_where)
        params.extend(period_params)

    weight_expr = "COALESCE(SUM(amount_kzt), COUNT(*))" if metric == "amount" else "COUNT(*)"

    if zoom < 7:
        step = 0.4
    elif zoom < 9:
        step = 0.12
    elif zoom < 11:
        step = 0.04
    else:
        step = None

    if step is not None:
        rows = con.execute(f"""
            SELECT ROUND(lat/{step})*{step} AS lat,
                   ROUND(lon/{step})*{step} AS lon,
                   COUNT(*) AS cnt,
                   COALESCE(SUM(amount_kzt), 0) AS vol
            FROM transfers
            WHERE {where}
            GROUP BY 1, 2
            LIMIT 15000
        """, params).fetchall()
        return [{"lat": float(r[0]), "lon": float(r[1]), "count": int(r[2]), "amount": float(r[3])} for r in rows]

    rows = con.execute(f"""
        SELECT lat, lon, amount_kzt, purpose_cat, trans_ts
        FROM transfers
        WHERE {where}
        ORDER BY trans_ts DESC NULLS LAST
        LIMIT 20000
    """, params).fetchall()
    return [
        {"lat": float(r[0]), "lon": float(r[1]),
         "amount": float(r[2]) if r[2] is not None else 0.0,
         "purpose_cat": r[3],
         "ts": str(r[4]) if r[4] else None}
        for r in rows
    ]


@app.get("/api/transfers/dashboard")
async def api_transfers_dashboard(
    purpose_cats: Optional[str] = None,
    oblast: str = "ALL",
    period: str = "month",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    where: list[str] = []
    params: list = []

    if purpose_cats:
        cats = [c.strip() for c in purpose_cats.split(",") if c.strip()]
        if cats:
            placeholders = ",".join("?" * len(cats))
            where.append(f"purpose_cat IN ({placeholders})")
            params.extend(cats)

    if oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)

    _append_transfers_period_clause(where, params, period, anchor_date, start_date, end_date)
    where_sql = (" AND ".join(where)) if where else "1=1"

    kpi = con.execute(f"""
        SELECT COUNT(*) AS cnt,
               COALESCE(SUM(amount_kzt), 0) AS vol,
               COALESCE(AVG(amount_kzt), 0) AS avg_amt,
               APPROX_COUNT_DISTINCT(iin) AS users
        FROM transfers WHERE {where_sql}
    """, params).fetchone()

    purpose_rows = con.execute(f"""
        SELECT purpose_cat, COUNT(*) AS cnt, COALESCE(SUM(amount_kzt), 0) AS vol
        FROM transfers WHERE {where_sql}
        GROUP BY purpose_cat ORDER BY vol DESC
    """, params).fetchall()

    hour_rows = con.execute(f"""
        SELECT hour, COUNT(*) AS cnt
        FROM transfers WHERE {where_sql} AND hour IS NOT NULL
        GROUP BY hour ORDER BY hour
    """, params).fetchall()
    hours = [0] * 24
    for hr, cnt in hour_rows:
        h = int(hr or 0)
        if 0 <= h <= 23:
            hours[h] = int(cnt or 0)

    top_rayon_rows = con.execute(f"""
        SELECT rayon_id, COALESCE(MAX(rayon_name), rayon_id) AS rayon_name,
               COALESCE(MAX(oblast_kk), '-') AS oblast_kk,
               COUNT(*) AS cnt, COALESCE(SUM(amount_kzt), 0) AS vol
        FROM transfers
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id ORDER BY vol DESC LIMIT 8
    """, params).fetchall()

    return {
        "has_trans_date": TRANSFERS_HAS_TRANS_DATE,
        "kpi": {
            "count": int(kpi[0] or 0),
            "volume_kzt": float(kpi[1] or 0),
            "avg_kzt": float(kpi[2] or 0),
            "users": int(kpi[3] or 0),
        },
        "by_purpose": [
            {"purpose_cat": r[0], "count": int(r[1]), "volume_kzt": float(r[2])}
            for r in purpose_rows
        ],
        "hours": hours,
        "top_rayons": [
            {"rayon_id": r[0], "rayon_name": r[1], "oblast_kk": r[2],
             "count": int(r[3]), "volume_kzt": float(r[4])}
            for r in top_rayon_rows
        ],
    }




@app.get("/api/infrastructure/nearby")
async def api_infrastructure_nearby(
    lat: float,
    lon: float,
    radius_m: int = 500,
    limit: int = 300,
):
    _ensure_ready()
    if not ENABLE_INFRA:
        return []
    radius_m = max(50, min(5000, int(radius_m)))
    limit = max(10, min(2000, int(limit)))

    out = []
    for p in INFRA_POINTS:
        d = _haversine_m(lat, lon, p["lat"], p["lon"])
        if d <= radius_m:
            item = dict(p)
            item["dist_m"] = int(round(d))
            out.append(item)

    out.sort(key=lambda x: x["dist_m"])
    return out[:limit]


@app.get("/tiles/{z}/{x}/{y}.png")
async def get_tiles(z: int, x: int, y: int):
    if z > MAX_SERVE_ZOOM:
        return Response(status_code=404, content="Zoom too high", media_type="text/plain")

    tile_path = CACHE_DIR / str(z) / str(x) / f"{y}.png"

    if z <= MAX_CACHE_ZOOM and tile_path.exists():
        return FileResponse(
            str(tile_path),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    if not UPSTREAM_TILE_URL:
        return Response(status_code=404, content="Tile upstream is not configured", media_type="text/plain")
    url = UPSTREAM_TILE_URL.format(z=z, x=x, y=y)
    headers = {"User-Agent": "GeoAnalytics/1.0", "Accept": "image/png"}
    proxies = None
    if PROXY_TILE_URL:
        proxy_url = PROXY_TILE_URL if PROXY_TILE_URL.startswith("http") else f"http://{PROXY_TILE_URL}"
        proxies = {"http": proxy_url, "https": proxy_url}

    def _fetch_tile() -> requests.Response:
        return requests.get(url, headers=headers, proxies=proxies, timeout=20)

    try:
        resp = await asyncio.to_thread(_fetch_tile)

        if resp.status_code != 200:
            return Response(status_code=resp.status_code, content=f"OSM {resp.status_code}", media_type="text/plain")

        if z <= MAX_CACHE_ZOOM:
            tile_path.parent.mkdir(parents=True, exist_ok=True)
            tile_path.write_bytes(resp.content)
            cache_header = "public, max-age=86400"
        else:
            cache_header = "public, max-age=3600"

        return Response(content=resp.content, media_type="image/png", headers={"Cache-Control": cache_header})
    except Exception as exc:
        return Response(status_code=500, content=str(exc), media_type="text/plain")


@app.get("/api/stats/trend")
async def api_stats_trend(
    oblast: str = "ALL",
    rayon_id: Optional[str] = None,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    if not HAS_EVENT_DATE:
        return {"available": False}

    cur_start, cur_end = _period_bounds(period, anchor_date, start_date, end_date)
    duration = (cur_end - cur_start).days + 1
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=duration - 1)

    where_base: list = []
    params_base: list = []

    effective_rayon_id: Optional[str] = None
    if rayon_id:
        meta = RAYON_BY_ID.get(rayon_id)
        if meta:
            feature_oblast = meta.get("properties", {}).get("oblast_kk")
            if not oblast or oblast == "ALL" or feature_oblast == oblast:
                effective_rayon_id = rayon_id

    if effective_rayon_id:
        where_base.append("rayon_id = ?")
        params_base.append(effective_rayon_id)
    elif oblast and oblast != "ALL":
        where_base.append("oblast_kk = ?")
        params_base.append(oblast)

    def _query_period(s: date, e: date):
        w = list(where_base) + ["event_date BETWEEN ? AND ?"]
        p = list(params_base) + [s.isoformat(), e.isoformat()]
        where_sql = " AND ".join(w) if w else "1=1"
        row = con.execute(
            f"SELECT COUNT(*) AS events, APPROX_COUNT_DISTINCT(iin) AS users FROM clients WHERE {where_sql}",
            p,
        ).fetchone()
        return int(row[0] or 0), int(row[1] or 0)

    cur_events, cur_users = _query_period(cur_start, cur_end)
    prev_events, prev_users = _query_period(prev_start, prev_end)

    def _pct(cur: int, prev: int) -> Optional[float]:
        if prev == 0:
            return None
        return round((cur - prev) / prev * 100, 1)

    return {
        "available": True,
        "current": {
            "events": cur_events,
            "users": cur_users,
            "start": cur_start.isoformat(),
            "end": cur_end.isoformat(),
        },
        "previous": {
            "events": prev_events,
            "users": prev_users,
            "start": prev_start.isoformat(),
            "end": prev_end.isoformat(),
        },
        "delta": {
            "events_pct": _pct(cur_events, prev_events),
            "users_pct": _pct(cur_users, prev_users),
        },
    }


@app.get("/api/stats/dow_hour")
async def api_stats_dow_hour(
    oblast: str = "ALL",
    rayon_id: Optional[str] = None,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()

    where: list = []
    params: list = []

    effective_rayon_id: Optional[str] = None
    if rayon_id:
        meta = RAYON_BY_ID.get(rayon_id)
        if meta:
            feature_oblast = meta.get("properties", {}).get("oblast_kk")
            if not oblast or oblast == "ALL" or feature_oblast == oblast:
                effective_rayon_id = rayon_id

    if effective_rayon_id:
        where.append("rayon_id = ?")
        params.append(effective_rayon_id)
    elif oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)

    period_where: list = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    where.extend(period_where)
    params.extend(period_params)

    where_sql = (" AND ".join(where) + " AND event_date IS NOT NULL") if where else "event_date IS NOT NULL"

    rows = con.execute(
        f"""
        SELECT
            CAST(EXTRACT('dow' FROM event_date) AS INTEGER) AS dow,
            hour,
            COUNT(*) AS events
        FROM clients
        WHERE {where_sql}
        GROUP BY dow, hour
        """,
        params,
    ).fetchall()

    grid = [[0] * 24 for _ in range(7)]
    for dow, h, cnt in rows:
        d = int(dow)
        hh = int(h)
        if 0 <= d <= 6 and 0 <= hh <= 23:
            grid[d][hh] = int(cnt or 0)

    return {
        "grid": grid,
        "dow_labels": ["Вс", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"],
        "has_data": HAS_EVENT_DATE,
    }


@app.get("/api/stats/coverage")
async def api_stats_coverage(
    oblast: str,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    if not oblast or oblast == "ALL":
        return JSONResponse({"error": "oblast required"}, status_code=400)

    all_rayons = RAYONS_BY_OBLAST.get(oblast, [])
    if not all_rayons:
        return []

    where = ["oblast_kk = ?"]
    params: list = [oblast]
    period_where: list = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    where.extend(period_where)
    params.extend(period_params)
    where_sql = " AND ".join(where)

    active_rows = con.execute(
        f"""
        SELECT rayon_id, COUNT(*) AS events
        FROM clients
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id
        """,
        params,
    ).fetchall()
    active_map = {str(r[0]): int(r[1]) for r in active_rows}

    result = []
    for feat in all_rayons:
        rid = feat["properties"]["full_id"]
        events = active_map.get(rid, 0)
        result.append({
            "full_id": rid,
            "name_kk": feat["properties"]["name_kk"],
            "events": events,
            "active": events > 0,
        })

    return sorted(result, key=lambda x: x["events"])


@app.get("/api/stats/choropleth")
async def api_stats_choropleth(
    oblast: str = "ALL",
    min_h: int = 0,
    max_h: int = 23,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    min_h = max(0, min_h)
    max_h = min(23, max_h)
    if max_h < min_h:
        min_h, max_h = max_h, min_h
    where = ["hour BETWEEN ? AND ?"]
    params: list = [min_h, max_h]
    if oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)
    period_where: list = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    where.extend(period_where)
    params.extend(period_params)
    where_sql = " AND ".join(where)
    rows = con.execute(
        f"""
        SELECT rayon_id, COUNT(*) AS events, APPROX_COUNT_DISTINCT(iin) AS users
        FROM clients
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id
        """,
        params,
    ).fetchall()
    return {str(r[0]): {"events": int(r[1]), "users": int(r[2])} for r in rows}


@app.get("/api/stats/period_compare")
async def api_stats_period_compare(
    oblast: str = "ALL",
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    if not HAS_EVENT_DATE:
        return {"available": False}

    cur_start, cur_end = _period_bounds(period, anchor_date, start_date, end_date)
    duration = (cur_end - cur_start).days + 1
    prev_end = cur_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=duration - 1)

    where_base: list[str] = []
    params_base: list = []
    if oblast and oblast != "ALL":
        where_base.append("oblast_kk = ?")
        params_base.append(oblast)

    def _daily(s: date, e: date):
        w = list(where_base) + ["event_date BETWEEN ? AND ?"]
        p = list(params_base) + [s.isoformat(), e.isoformat()]
        where_sql = " AND ".join(w) if w else "1=1"
        rows = con.execute(
            f"SELECT event_date, COUNT(*) AS events, APPROX_COUNT_DISTINCT(iin) AS users FROM clients WHERE {where_sql} GROUP BY event_date ORDER BY event_date",
            p,
        ).fetchall()
        return {str(r[0]): {"events": int(r[1]), "users": int(r[2])} for r in rows}

    cur_data = _daily(cur_start, cur_end)
    prev_data = _daily(prev_start, prev_end)

    cur_days = []
    prev_days = []
    for i in range(duration):
        cd = (cur_start + timedelta(days=i)).isoformat()
        pd = (prev_start + timedelta(days=i)).isoformat()
        cur_days.append({"date": cd, **cur_data.get(cd, {"events": 0, "users": 0})})
        prev_days.append({"date": pd, **prev_data.get(pd, {"events": 0, "users": 0})})

    return {
        "available": True,
        "current": cur_days,
        "previous": prev_days,
        "cur_range": f"{cur_start.isoformat()}..{cur_end.isoformat()}",
        "prev_range": f"{prev_start.isoformat()}..{prev_end.isoformat()}",
    }


@app.get("/api/stats/anomalies")
async def api_stats_anomalies(
    oblast: str = "ALL",
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()
    if not HAS_EVENT_DATE:
        return {"available": False, "anomalies": []}

    where: list[str] = []
    params: list = []
    if oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)

    period_where: list[str] = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    where.extend(period_where)
    params.extend(period_params)
    where_sql = (" AND ".join(where)) if where else "1=1"

    rows = con.execute(
        f"""
        SELECT
            rayon_id,
            COALESCE(MAX(rayon_name), rayon_id) AS rayon_name,
            COALESCE(MAX(oblast_kk), '-') AS oblast_kk,
            COUNT(*) AS events,
            APPROX_COUNT_DISTINCT(iin) AS users,
            AVG(CAST(hour AS DOUBLE)) AS avg_hour,
            STDDEV(CAST(hour AS DOUBLE)) AS std_hour,
            SUM(CASE WHEN hour < 6 OR hour >= 22 THEN 1 ELSE 0 END) AS night_events
        FROM clients
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id
        HAVING COUNT(*) >= 5
        ORDER BY events DESC
        """,
        params,
    ).fetchall()

    if len(rows) < 3:
        return {"available": True, "anomalies": []}

    import statistics as _stats
    events_list = [int(r[3]) for r in rows]
    mean_ev = _stats.mean(events_list)
    std_ev = _stats.stdev(events_list) if len(events_list) > 1 else 0

    anomalies = []
    for r in rows:
        rayon_id, rayon_name, oblast_kk, events, users, avg_hour, _, night_events = r
        events = int(events)
        users = int(users)
        night_events = int(night_events or 0)
        night_pct = night_events / events if events > 0 else 0

        reasons = []
        severity = "normal"

        if std_ev > 0:
            z = (events - mean_ev) / std_ev
            if z > 2.0:
                reasons.append(f"Всплеск активности: +{round(z, 1)}σ выше среднего")
                severity = "high"
            elif z > 1.5:
                reasons.append(f"Повышенная активность: +{round(z, 1)}σ")
                severity = "medium"

        if night_pct > 0.30:
            reasons.append(f"Ночные авторизации: {round(night_pct * 100)}% событий с 22:00 до 06:00")
            if severity == "normal":
                severity = "medium"

        if avg_hour is not None:
            ah = float(avg_hour)
            if ah < 7.0:
                reasons.append(f"Аномально раннее время: ср. {round(ah, 1)}ч")
                if severity == "normal":
                    severity = "medium"
            elif ah > 21.0:
                reasons.append(f"Аномально позднее время: ср. {round(ah, 1)}ч")
                if severity == "normal":
                    severity = "medium"

        if reasons:
            anomalies.append({
                "rayon_id": str(rayon_id),
                "rayon_name": str(rayon_name),
                "oblast_kk": str(oblast_kk),
                "events": events,
                "users": users,
                "night_pct": round(night_pct * 100, 1),
                "avg_hour": round(float(avg_hour), 1) if avg_hour is not None else None,
                "severity": severity,
                "reasons": reasons,
            })

    anomalies.sort(key=lambda x: (0 if x["severity"] == "high" else 1, -x["events"]))
    return {"available": True, "anomalies": anomalies[:20]}


@app.get("/api/stats/behavior_clusters")
async def api_stats_behavior_clusters(
    oblast: str = "ALL",
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()

    where: list[str] = []
    params: list = []
    if oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)

    period_where: list[str] = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    where.extend(period_where)
    params.extend(period_params)
    where_sql = (" AND ".join(where)) if where else "1=1"

    rows = con.execute(
        f"""
        SELECT
            rayon_id,
            COALESCE(MAX(rayon_name), rayon_id) AS rayon_name,
            COALESCE(MAX(oblast_kk), '-') AS oblast_kk,
            COUNT(*) AS events,
            APPROX_COUNT_DISTINCT(iin) AS users,
            SUM(CASE WHEN hour BETWEEN 9 AND 18 THEN 1 ELSE 0 END) AS work_events,
            SUM(CASE WHEN hour >= 22 OR hour < 6 THEN 1 ELSE 0 END) AS night_events,
            SUM(CASE WHEN hour BETWEEN 6 AND 9 OR hour BETWEEN 17 AND 20 THEN 1 ELSE 0 END) AS transit_events
        FROM clients
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id
        HAVING COUNT(*) >= 3
        """,
        params,
    ).fetchall()

    result = {}
    for r in rows:
        rayon_id, rayon_name, oblast_kk, events, users, work_ev, night_ev, transit_ev = r
        events = int(events)
        work_r = int(work_ev or 0) / events
        night_r = int(night_ev or 0) / events
        transit_r = int(transit_ev or 0) / events

        if work_r >= 0.45:
            pattern = "work"
        elif night_r >= 0.25:
            pattern = "home"
        elif transit_r >= 0.30:
            pattern = "transit"
        else:
            pattern = "mixed"

        result[str(rayon_id)] = {
            "rayon_name": str(rayon_name),
            "oblast_kk": str(oblast_kk),
            "events": events,
            "users": int(users),
            "work_pct": round(work_r * 100, 1),
            "night_pct": round(night_r * 100, 1),
            "transit_pct": round(transit_r * 100, 1),
            "pattern": pattern,
        }

    return result


@app.get("/api/stats/behavior_grid")
async def api_stats_behavior_grid(
    oblast: str = "ALL",
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    step: float = 0.02,
):
    _ensure_ready()
    step = max(0.005, min(0.1, float(step)))

    where: list[str] = []
    params: list = []
    if oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)

    period_where: list[str] = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    where.extend(period_where)
    params.extend(period_params)
    where_sql = (" AND ".join(where)) if where else "1=1"

    rows = con.execute(
        f"""
        SELECT
            ROUND(lat/{step})*{step} AS lat_bin,
            ROUND(lon/{step})*{step} AS lon_bin,
            COUNT(*) AS events,
            SUM(CASE WHEN hour BETWEEN 9 AND 18 THEN 1 ELSE 0 END) AS work_ev,
            SUM(CASE WHEN hour >= 22 OR hour < 6 THEN 1 ELSE 0 END) AS night_ev,
            SUM(CASE WHEN hour BETWEEN 6 AND 9 OR hour BETWEEN 17 AND 20 THEN 1 ELSE 0 END) AS transit_ev
        FROM clients
        WHERE {where_sql}
        GROUP BY lat_bin, lon_bin
        HAVING COUNT(*) >= 3
        LIMIT 8000
        """,
        params,
    ).fetchall()

    if not rows:
        return []

    # Compute per-cell ratios first, then classify relative to the mean.
    # Fixed thresholds don't work for banking data where work hours dominate
    # everywhere — relative scoring finds cells that are distinctively
    # residential/transit compared to the oblast average.
    cells = []
    for r in rows:
        lat_bin, lon_bin, events, work_ev, night_ev, transit_ev = r
        events = int(events)
        cells.append({
            "lat": round(float(lat_bin), 4),
            "lon": round(float(lon_bin), 4),
            "events": events,
            "work_r": int(work_ev or 0) / events,
            "night_r": int(night_ev or 0) / events,
            "transit_r": int(transit_ev or 0) / events,
        })

    mean_work = sum(c["work_r"] for c in cells) / len(cells)
    mean_night = sum(c["night_r"] for c in cells) / len(cells)
    mean_transit = sum(c["transit_r"] for c in cells) / len(cells)

    result = []
    for c in cells:
        work_r, night_r, transit_r = c["work_r"], c["night_r"], c["transit_r"]
        work_score = work_r - mean_work
        night_score = night_r - mean_night
        transit_score = transit_r - mean_transit

        scores = {"work": work_score, "home": night_score, "transit": transit_score}
        best = max(scores, key=scores.get)
        pattern = best if scores[best] > 0.01 else "mixed"

        result.append({
            "lat": c["lat"],
            "lon": c["lon"],
            "events": c["events"],
            "work_pct": round(work_r * 100, 1),
            "night_pct": round(night_r * 100, 1),
            "transit_pct": round(transit_r * 100, 1),
            "pattern": pattern,
        })

    return result


@app.get("/api/client/journey")
async def api_client_journey(
    iin: str = Query(..., min_length=4),
    period: str = "month",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()

    where = ["iin = ?"]
    params: list = [iin]
    _append_period_clause(where, params, period, anchor_date, start_date, end_date)
    where_sql = " AND ".join(where)

    rows = con.execute(
        f"""
        SELECT event_date, event_ts, lat, lon, hour, rayon_name, oblast_kk
        FROM clients
        WHERE {where_sql} AND event_ts IS NOT NULL
        ORDER BY event_ts ASC
        LIMIT 10000
        """,
        params,
    ).fetchall()

    days: dict[str, list] = {}
    for r in rows:
        event_date, event_ts, lat, lon, hour, rayon_name, oblast_kk = r
        day_key = str(event_date) if event_date else (str(event_ts)[:10] if event_ts else None)
        if not day_key:
            continue
        days.setdefault(day_key, []).append({
            "lat": float(lat),
            "lon": float(lon),
            "hour": int(hour),
            "event_ts": str(event_ts) if event_ts else None,
            "rayon_name": str(rayon_name) if rayon_name else None,
            "oblast_kk": str(oblast_kk) if oblast_kk else None,
        })

    result = []
    for day_key in sorted(days.keys()):
        pts = days[day_key]
        result.append({
            "date": day_key,
            "events": len(pts),
            "rayons": list({p["rayon_name"] for p in pts if p["rayon_name"]}),
            "points": pts,
        })

    return {"iin": iin, "days": result, "total_days": len(result)}


@app.get("/api/stats/devices")
async def api_stats_devices(
    oblast: str = "ALL",
    rayon_id: Optional[str] = None,
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    _ensure_ready()

    # Check if device_type column exists
    try:
        cols = con.execute("DESCRIBE clients").fetchall()
        col_names = [c[0].lower() for c in cols]
        if "device_type" not in col_names:
            return {"available": False, "items": []}
    except Exception:
        return {"available": False, "items": []}

    where: list = []
    params: list = []

    effective_rayon_id: Optional[str] = None
    if rayon_id:
        meta = RAYON_BY_ID.get(rayon_id)
        if meta:
            feature_oblast = meta.get("properties", {}).get("oblast_kk")
            if not oblast or oblast == "ALL" or feature_oblast == oblast:
                effective_rayon_id = rayon_id

    if effective_rayon_id:
        where.append("rayon_id = ?")
        params.append(effective_rayon_id)
    elif oblast and oblast != "ALL":
        where.append("oblast_kk = ?")
        params.append(oblast)

    period_where: list = []
    period_params: list = []
    _append_period_clause(period_where, period_params, period, anchor_date, start_date, end_date)
    where.extend(period_where)
    params.extend(period_params)

    where_sql = (" AND ".join(where)) if where else "1=1"

    rows = con.execute(
        f"""
        SELECT
            COALESCE(device_type, 'Неизвестно') AS dtype,
            COUNT(*) AS events,
            APPROX_COUNT_DISTINCT(iin) AS users
        FROM clients
        WHERE {where_sql} AND device_type IS NOT NULL
        GROUP BY dtype
        ORDER BY events DESC
        LIMIT 20
        """,
        params,
    ).fetchall()

    total_events = sum(int(r[1] or 0) for r in rows)
    items = []
    for r in rows:
        ev = int(r[1] or 0)
        items.append({
            "device_type": str(r[0]),
            "events": ev,
            "users": int(r[2] or 0),
            "pct": round(ev / total_events * 100, 1) if total_events > 0 else 0.0,
        })

    return {"available": True, "items": items}


@app.get("/api/stats/conversion")
async def api_stats_conversion(
    oblast: str = "ALL",
    period: str = "week",
    anchor_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    top_n: int = 20,
):
    """
    Conversion funnel: for each rayon shows how many auth sessions led to a transfer.
    Only available when the parquet was built with run_enriched (has_transfer column).
    """
    _ensure_ready()
    if not HAS_SESSION_DATA:
        return {"available": False, "reason": "Parquet built without session join. Re-run build with run_enriched."}

    start, end = _period_bounds(period, anchor_date, start_date, end_date)
    where_parts = ["event_date BETWEEN ? AND ?"]
    params: list = [start.isoformat(), end.isoformat()]
    if oblast and oblast != "ALL":
        where_parts.append("oblast_kk = ?")
        params.append(oblast)
    where_sql = " AND ".join(where_parts)

    rows = con.execute(
        f"""
        SELECT
            rayon_id,
            ANY_VALUE(rayon_name) AS rayon_name,
            ANY_VALUE(oblast_kk) AS oblast_kk,
            COUNT(*) AS total_sessions,
            SUM(CASE WHEN has_transfer THEN 1 ELSE 0 END) AS transfer_sessions,
            ROUND(SUM(CASE WHEN has_transfer THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS conversion_pct,
            APPROX_COUNT_DISTINCT(iin) AS unique_users,
            COALESCE(SUM(session_transfer_kzt), 0) AS total_transfer_kzt,
            COALESCE(AVG(CASE WHEN has_transfer THEN session_transfer_kzt END), 0) AS avg_transfer_kzt
        FROM clients
        WHERE {where_sql} AND rayon_id IS NOT NULL
        GROUP BY rayon_id
        ORDER BY transfer_sessions DESC
        LIMIT ?
        """,
        params + [top_n],
    ).fetchall()

    total_sessions_all = con.execute(
        f"SELECT COUNT(*), SUM(CASE WHEN has_transfer THEN 1 ELSE 0 END) FROM clients WHERE {where_sql}",
        params,
    ).fetchone()
    total_all = int(total_sessions_all[0] or 0)
    transfer_all = int(total_sessions_all[1] or 0)

    return {
        "available": True,
        "period": f"{start} / {end}",
        "summary": {
            "total_sessions": total_all,
            "transfer_sessions": transfer_all,
            "conversion_pct": round(transfer_all * 100 / total_all, 1) if total_all else 0.0,
        },
        "by_rayon": [
            {
                "rayon_id": r[0],
                "rayon_name": r[1],
                "oblast_kk": r[2],
                "total_sessions": int(r[3] or 0),
                "transfer_sessions": int(r[4] or 0),
                "conversion_pct": float(r[5] or 0),
                "unique_users": int(r[6] or 0),
                "total_transfer_kzt": float(r[7] or 0),
                "avg_transfer_kzt": float(r[8] or 0),
            }
            for r in rows
        ],
    }


9
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.getenv("PORT", "8080")))