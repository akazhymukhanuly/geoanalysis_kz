from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

from config import DB_URL as CONFIG_DB_URL, TRANSFERS_PARQUET_PATH

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
GEOJSON_PATH = DATA_DIR / "Open_dataset_of_administrative_boundaries_of_Kazakhstan.geojson"
OUT_PARQUET = DATA_DIR / "clients_enriched.parquet"

# Time window for matching auth event to a transfer (minutes).
# Override via SESSION_WINDOW_MINUTES env var.
SESSION_WINDOW_MINUTES = int(os.getenv("SESSION_WINDOW_MINUTES", "30"))

DEFAULT_QUERY = """
SELECT
  updated_at,
  iin,
  phone,
  device_name,
  device_geo_latitude AS lat,
  device_geo_longitude AS lon
FROM ods.parsed_auth_entries pae
WHERE updated_at >= (CURRENT_DATE - INTERVAL '3 months')
  AND device_geo_latitude IS NOT NULL
  AND device_geo_longitude IS NOT NULL
  AND device_geo_latitude != '0.0'
GROUP BY 1,2,3,4,5,6
"""

DEFAULT_TRANSFERS_QUERY = """
SELECT
  iin,
  client_name,
  account,
  partner_id,
  amount_cur,
  amount_kzt,
  target_currency,
  direction,
  purpose,
  bank_commission,
  trans_date,
  lat,
  lon
FROM ods.dm_mobile_transfers dat
WHERE trans_date >= (CURRENT_DATE - INTERVAL '1 months')
  AND lat IS NOT NULL
  AND lon IS NOT NULL
  AND lat <> '0.0'
  AND lon <> '0.0'
  AND lat <> '0'
  AND lon <> '0'
"""

# For session join we need minute-level precision.
# If trans_date in your table is a DATE column, set SESSION_TRANSFERS_QUERY env var
# to select the proper TIMESTAMP column (e.g. trans_ts) from dm_mobile_transfers.
DEFAULT_SESSION_TRANSFERS_QUERY = os.getenv("SESSION_TRANSFERS_QUERY", """
SELECT
  iin,
  amount_kzt,
  direction,
  purpose,
  trans_date AS trans_ts,
  lat,
  lon
FROM ods.dm_mobile_transfers
WHERE trans_date >= (CURRENT_DATE - INTERVAL '3 months')
  AND lat IS NOT NULL AND lon IS NOT NULL
  AND lat NOT IN ('0.0', '0')
  AND lon NOT IN ('0.0', '0')
""")

# Purpose categorisation (mirrors _PURPOSE_CAT_SQL in main.py)
_PURPOSE_MAP = [
    ("зарубеж",         "p2p_abroad"),
    ("брокерск",        "invest"),
    ("invest",          "invest"),
    ("конвертац",       "conversion"),
    ("iban",            "iban_external"),
    ("бюджет",          "budget"),
    ("p2p",             "p2p_local"),
    ("visa alias",      "p2p_local"),
    ("номеру телефона", "p2p_local"),
    ("карты на карту",  "p2p_local"),
    ("родительск",      "p2p_local"),
    ("open api",        "p2p_local"),
    ("cash by code",    "p2p_local"),
    ("возврат",         "p2p_local"),
]


def _categorize_purpose(purpose: object) -> str:
    if purpose is None or (isinstance(purpose, float) and pd.isna(purpose)):
        return "transfer"
    p = str(purpose).lower()
    if "открыт" in p and "депозит" in p:
        return "deposit"
    for kw, cat in _PURPOSE_MAP:
        if kw in p:
            return cat
    return "transfer"


def pick_col(cols: list[str], *candidates: str) -> str | None:
    m = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in m:
            return m[c.lower()]
    return None


def log(stage: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{stage}] {msg}", flush=True)


def load_admin(geojson_path: Path) -> gpd.GeoDataFrame:
    admin = gpd.read_file(geojson_path)
    admin = admin[
        admin["full_id"].notna()
        & admin["name_kk"].notna()
        & admin["oblast_kk"].notna()
    ].copy()

    admin["rayon_id"] = admin["full_id"].astype("string")
    admin["rayon_name"] = admin["name_kk"].astype("string")
    admin["oblast_kk"] = admin["oblast_kk"].astype("string")

    admin = admin.set_geometry("geometry")
    if admin.crs is None:
        admin = admin.set_crs("EPSG:4326")
    else:
        admin = admin.to_crs(4326)

    # Fix invalid geometry from OSM-like sources.
    try:
        admin["geometry"] = admin["geometry"].make_valid()
    except Exception:
        admin["geometry"] = admin["geometry"].buffer(0)

    admin["area_m2"] = admin.to_crs(3857).area
    return admin[["rayon_id", "rayon_name", "oblast_kk", "area_m2", "geometry"]].copy()


def enrich_df(df: pd.DataFrame, admin_small: gpd.GeoDataFrame) -> pd.DataFrame:
    log("STAGE-2", f"Старт нормализации DataFrame, вход строк: {len(df)}")
    cols = list(df.columns)

    lat_col = pick_col(cols, "lat", "latitude", "device_geo_latitude", "geo_lat", "geo_latitude")
    lon_col = pick_col(cols, "lon", "lng", "longitude", "device_geo_longitude", "geo_lon", "geo_longitude")
    iin_col = pick_col(cols, "iin", "client_iin", "customer_iin")
    dt_col = pick_col(
        cols,
        "updated_at",
        "datetimestamp",
        "date_timestamp",
        "event_ts",
        "datetime",
        "event_time",
        "dt",
        "timestamp",
        "ts",
    )
    hour_col = pick_col(cols, "hour", "hh")
    ip_col = pick_col(cols, "ip", "ip_address", "ip_addr")
    device_id_col = pick_col(cols, "device_id", "device_uuid", "device_guid", "device")
    device_type_col = pick_col(cols, "device_type", "device_model", "platform", "os", "device_name")

    if not lat_col or not lon_col:
        raise RuntimeError("Source query must return lat/lon columns")

    work = df.copy()
    work[lat_col] = pd.to_numeric(work[lat_col], errors="coerce")
    work[lon_col] = pd.to_numeric(work[lon_col], errors="coerce")
    work = work.dropna(subset=[lat_col, lon_col]).copy()

    if hour_col:
        work["hour"] = pd.to_numeric(work[hour_col], errors="coerce").fillna(0).astype(int)
    elif dt_col:
        ts = pd.to_datetime(work[dt_col], errors="coerce")
        work["hour"] = ts.dt.hour.fillna(0).astype(int)
    elif iin_col:
        key = (
            work[iin_col].astype(str)
            + "|"
            + work[lat_col].astype(str)
            + "|"
            + work[lon_col].astype(str)
        )
        work["hour"] = (key.map(hash).abs() % 24).astype(int)
    else:
        work["hour"] = 0

    if dt_col:
        work["event_ts"] = pd.to_datetime(work[dt_col], errors="coerce")
        work["event_date"] = work["event_ts"].dt.date
    else:
        key = work[lat_col].astype(str) + "|" + work[lon_col].astype(str)
        days_back = (key.map(hash).abs() % 90).astype(int)
        base = pd.Timestamp.today().normalize() - pd.to_timedelta(days_back, unit="D")
        work["event_ts"] = base + pd.to_timedelta(work["hour"], unit="h")
        work["event_date"] = work["event_ts"].dt.date

    work["iin"] = work[iin_col].astype("string") if iin_col else pd.Series([pd.NA] * len(work), dtype="string")
    work["ip_addr"] = work[ip_col].astype("string") if ip_col else pd.Series([pd.NA] * len(work), dtype="string")
    work["device_id"] = work[device_id_col].astype("string") if device_id_col else pd.Series([pd.NA] * len(work), dtype="string")
    work["device_type"] = work[device_type_col].astype("string") if device_type_col else pd.Series([pd.NA] * len(work), dtype="string")

    is_degrees_like = work[lat_col].between(-90, 90).all() and work[lon_col].between(-180, 180).all()
    src_crs = "EPSG:4326" if is_degrees_like else "EPSG:3857"

    gdf = gpd.GeoDataFrame(
        work.reset_index(drop=True),
        geometry=gpd.points_from_xy(work[lon_col], work[lat_col]),
        crs=src_crs,
    )
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    join_left = gdf.reset_index(drop=False).rename(columns={"index": "pt_idx"})
    joined = gpd.sjoin(
        join_left,
        admin_small,
        how="left",
        predicate="intersects",
    )

    joined = (
        joined.sort_values(["pt_idx", "area_m2"], ascending=[True, True])
        .drop_duplicates(subset=["pt_idx"], keep="first")
        .set_index("pt_idx")
        .sort_index()
    )

    out = pd.DataFrame(joined.drop(columns=["geometry", "index_right", "area_m2"], errors="ignore"))
    out["lat"] = pd.to_numeric(out[lat_col], errors="coerce")
    out["lon"] = pd.to_numeric(out[lon_col], errors="coerce")

    out_cols = [
        "lat",
        "lon",
        "iin",
        "event_ts",
        "event_date",
        "hour",
        "ip_addr",
        "device_id",
        "device_type",
        "oblast_kk",
        "rayon_id",
        "rayon_name",
    ]
    for c in out_cols:
        if c not in out.columns:
            out[c] = pd.NA

    return out[out_cols].copy()


def write_parquet(out: pd.DataFrame, out_parquet: Path) -> None:
    try:
        out.to_parquet(out_parquet, index=False)
    except Exception as exc:
        # Fallback for environments without pyarrow/fastparquet.
        log("STAGE-4", f"Pandas parquet failed ({exc}); fallback to duckdb COPY")
        import duckdb

        con = duckdb.connect(database=":memory:")
        con.register("out_df", out)
        target = str(out_parquet).replace("\\", "/").replace("'", "''")
        con.execute(f"COPY out_df TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()


def enrich_transfers_df(df: pd.DataFrame, admin_small: gpd.GeoDataFrame) -> pd.DataFrame:
    log("STAGE-2T", f"Нормализация transfers DataFrame, вход строк: {len(df)}")
    cols = list(df.columns)

    lat_col  = pick_col(cols, "lat", "latitude")
    lon_col  = pick_col(cols, "lon", "longitude")
    iin_col  = pick_col(cols, "iin", "client_iin")
    ts_col   = pick_col(cols, "trans_ts", "trans_date", "datetime", "dt")
    amt_kzt  = pick_col(cols, "amount_kzt")
    amt_cur  = pick_col(cols, "amount_cur")
    currency = pick_col(cols, "target_currency", "currency")
    dir_col  = pick_col(cols, "direction")
    purp_col = pick_col(cols, "purpose")
    comm_col = pick_col(cols, "bank_commission", "commission")
    partner  = pick_col(cols, "partner_id", "partner")
    account  = pick_col(cols, "account")
    cname    = pick_col(cols, "client_name", "name")

    if not lat_col or not lon_col:
        raise RuntimeError("Transfers query must return lat/lon columns")

    work = df.copy()
    work[lat_col] = pd.to_numeric(work[lat_col], errors="coerce")
    work[lon_col] = pd.to_numeric(work[lon_col], errors="coerce")
    work = work.dropna(subset=[lat_col, lon_col]).copy()
    work = work[work[lat_col] != 0.0].copy()
    work = work[work[lon_col] != 0.0].copy()

    # Timestamp & hour
    if ts_col:
        work["trans_ts"] = pd.to_datetime(work[ts_col], errors="coerce")
        work["trans_date"] = work["trans_ts"].dt.date.astype(str)
        work["hour"] = work["trans_ts"].dt.hour.fillna(0).astype(int)
    else:
        work["trans_ts"] = pd.NaT
        work["trans_date"] = pd.NA
        work["hour"] = 0

    # Geo join
    is_degrees = work[lat_col].between(-90, 90).all() and work[lon_col].between(-180, 180).all()
    gdf = gpd.GeoDataFrame(
        work.reset_index(drop=True),
        geometry=gpd.points_from_xy(work[lon_col], work[lat_col]),
        crs="EPSG:4326" if is_degrees else "EPSG:3857",
    )
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    join_left = gdf.reset_index(drop=False).rename(columns={"index": "pt_idx"})
    joined = gpd.sjoin(join_left, admin_small, how="left", predicate="intersects")
    joined = (
        joined.sort_values(["pt_idx", "area_m2"], ascending=[True, True])
        .drop_duplicates(subset=["pt_idx"], keep="first")
        .set_index("pt_idx")
        .sort_index()
    )

    out = pd.DataFrame(joined.drop(columns=["geometry", "index_right", "area_m2"], errors="ignore"))
    out["lat"] = pd.to_numeric(out[lat_col], errors="coerce")
    out["lon"] = pd.to_numeric(out[lon_col], errors="coerce")

    # Pass-through transfer fields
    for src, dst in [
        (iin_col,  "iin"),
        (cname,    "client_name"),
        (account,  "account"),
        (partner,  "partner_id"),
        (amt_kzt,  "amount_kzt"),
        (amt_cur,  "amount_cur"),
        (currency, "target_currency"),
        (dir_col,  "direction"),
        (purp_col, "purpose"),
        (comm_col, "bank_commission"),
    ]:
        if src and src in out.columns:
            out[dst] = out[src]
        elif dst not in out.columns:
            out[dst] = pd.NA

    out["amount_kzt"]     = pd.to_numeric(out["amount_kzt"],     errors="coerce")
    out["amount_cur"]     = pd.to_numeric(out["amount_cur"],     errors="coerce")
    out["bank_commission"]= pd.to_numeric(out["bank_commission"],errors="coerce")

    out_cols = [
        "lat", "lon",
        "iin", "client_name", "account", "partner_id",
        "amount_kzt", "amount_cur", "target_currency",
        "direction", "purpose", "bank_commission",
        "trans_ts", "trans_date", "hour",
        "oblast_kk", "rayon_id", "rayon_name",
    ]
    for c in out_cols:
        if c not in out.columns:
            out[c] = pd.NA

    return out[out_cols].copy()


def run_transfers(db_url: str, query: str, out_parquet: Path, geojson_path: Path) -> None:
    log("STAGE-0T", "Запуск сборки transfers parquet")
    log("STAGE-0T", f"Выходной parquet: {out_parquet}")

    log("STAGE-1T", "Загрузка границ районов (переиспользуем admin)")
    admin_small = load_admin(geojson_path)

    log("STAGE-1T", "Подключение к БД и выполнение transfers SQL")
    engine = create_engine(db_url)
    with engine.connect() as conn:
        rs = conn.exec_driver_sql(query)
        cols = list(rs.keys())
        rows = rs.fetchall()
        raw_df = pd.DataFrame(rows, columns=cols)

    if raw_df.empty:
        raise RuntimeError("No rows selected from transfers query")

    log("STAGE-1T", f"Строк из БД: {len(raw_df)}")
    out = enrich_transfers_df(raw_df, admin_small)

    log("STAGE-4T", "Запись transfers в parquet")
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(out, out_parquet)

    tagged = int(out["rayon_id"].notna().sum())
    log("DONE-T", f"Built: {out_parquet}")
    log("DONE-T", f"Rows: {len(out)}")
    log("DONE-T", f"Tagged with rayon: {tagged}")


def run(db_url: str, query: str, out_parquet: Path, geojson_path: Path) -> None:
    log("STAGE-0", "Запуск сборки parquet")
    log("STAGE-0", f"GeoJSON: {geojson_path}")
    log("STAGE-0", f"Выходной parquet: {out_parquet}")

    log("STAGE-1", "Загрузка границ районов из GeoJSON")
    admin_small = load_admin(geojson_path)
    log("STAGE-1", f"Границы загружены: {len(admin_small)}")

    log("STAGE-1", "Подключение к БД и выполнение SQL")
    engine = create_engine(db_url)
    with engine.connect() as conn:
        rs = conn.exec_driver_sql(query)
        cols = list(rs.keys())
        rows = rs.fetchall()
        raw_df = pd.DataFrame(rows, columns=cols)

    if raw_df.empty:
        raise RuntimeError("No rows selected from DB/query")

    log("STAGE-1", f"Строк из БД: {len(raw_df)}")
    out = enrich_df(raw_df, admin_small)

    log("STAGE-4", "Запись в parquet")
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(out, out_parquet)

    tagged = int(out["rayon_id"].notna().sum())
    log("DONE", f"Built: {out_parquet}")
    log("DONE", f"Rows: {len(out)}")
    log("DONE", f"Tagged with rayon: {tagged}")


def enrich_with_transfers(
    auth_df: pd.DataFrame,
    transfers_df: pd.DataFrame,
    window_minutes: int = SESSION_WINDOW_MINUTES,
) -> pd.DataFrame:
    """
    Adds session-level transfer columns to each auth event row.

    For each auth row finds transfers by the same IIN whose trans_ts falls
    within ±window_minutes of the auth event_ts.  Appends four columns:
      has_transfer          BOOL    – at least one transfer found in session
      session_transfer_count INT    – number of transfers in the window
      session_transfer_kzt  FLOAT  – total KZT in the window
      session_purpose_cat   STR    – dominant transfer category
    """
    auth_df = auth_df.copy().reset_index(drop=True)

    # Default values
    auth_df["has_transfer"] = False
    auth_df["session_transfer_count"] = 0
    auth_df["session_transfer_kzt"] = 0.0
    auth_df["session_purpose_cat"] = pd.NA

    # Parse timestamps
    auth_ts = pd.to_datetime(auth_df.get("event_ts"), errors="coerce")
    tr_ts_col = next(
        (c for c in ("trans_ts", "trans_date", "event_ts") if c in transfers_df.columns),
        None,
    )
    if tr_ts_col is None:
        log("SESSION", "Transfers has no timestamp column – skipping session join")
        return auth_df

    tr_ts = pd.to_datetime(transfers_df[tr_ts_col], errors="coerce")

    # Only rows with valid timestamp + non-null IIN
    auth_mask = auth_ts.notna() & auth_df["iin"].notna() & (auth_df["iin"] != "<NA>")
    tr_mask = tr_ts.notna() & transfers_df["iin"].notna() & (transfers_df["iin"] != "<NA>")

    auth_valid = auth_df[auth_mask][["iin"]].copy()
    auth_valid["_ets"] = auth_ts[auth_mask].values
    auth_valid.index.name = "_aidx"
    auth_valid = auth_valid.reset_index()  # column _aidx = row position in auth_df

    tr_valid = transfers_df[tr_mask][["iin"]].copy()
    tr_valid["_tts"] = tr_ts[tr_mask].values
    tr_valid["_akzt"] = pd.to_numeric(
        transfers_df.loc[tr_mask, "amount_kzt"] if "amount_kzt" in transfers_df.columns
        else pd.Series(0.0, index=transfers_df[tr_mask].index),
        errors="coerce",
    ).fillna(0.0).values
    tr_valid["_pcat"] = (
        transfers_df.loc[tr_mask, "purpose"].apply(_categorize_purpose).values
        if "purpose" in transfers_df.columns
        else "transfer"
    )

    if auth_valid.empty or tr_valid.empty:
        log("SESSION", "No valid rows after timestamp/IIN filter – skipping session join")
        return auth_df

    common_iins = set(auth_valid["iin"]) & set(tr_valid["iin"])
    if not common_iins:
        log("SESSION", "No overlapping IINs between auth and transfers – skipping session join")
        return auth_df

    log("SESSION", f"IINs in both datasets: {len(common_iins):,}  |  window: ±{window_minutes} min")

    a = auth_valid[auth_valid["iin"].isin(common_iins)]
    t = tr_valid[tr_valid["iin"].isin(common_iins)]

    merged = a.merge(t, on="iin", how="inner")
    delta = (merged["_tts"] - merged["_ets"]).abs()
    in_win = merged[delta <= pd.Timedelta(minutes=window_minutes)]

    if in_win.empty:
        log("SESSION", "No auth-transfer pairs within time window")
        return auth_df

    agg = (
        in_win.groupby("_aidx")
        .agg(
            _stc=("_pcat", "count"),
            _stk=("_akzt", "sum"),
            _stp=("_pcat", lambda s: s.mode().iloc[0]),
        )
    )

    auth_df.loc[agg.index, "has_transfer"] = True
    auth_df.loc[agg.index, "session_transfer_count"] = agg["_stc"].astype(int)
    auth_df.loc[agg.index, "session_transfer_kzt"] = agg["_stk"]
    auth_df.loc[agg.index, "session_purpose_cat"] = agg["_stp"]

    n = int(agg.shape[0])
    pct = n * 100 // max(len(auth_df), 1)
    log("SESSION", f"Auth events with session transfer: {n:,} / {len(auth_df):,} ({pct}%)")
    return auth_df


def run_enriched(
    db_url: str,
    auth_query: str,
    transfers_query: str,
    session_transfers_query: str,
    out_parquet: Path,
    transfers_parquet: Path,
    geojson_path: Path,
    window_minutes: int = SESSION_WINDOW_MINUTES,
) -> None:
    """
    Single-pass build that:
      1. Loads auth events and geo-enriches them.
      2. Loads transfers and geo-enriches them (→ standalone transfers parquet).
      3. Session-joins auth ↔ transfers by IIN + time window.
      4. Writes enriched auth parquet (with has_transfer* columns).
    """
    log("STAGE-0", "=== run_enriched: объединённая сборка auth + transfers ===")

    admin_small = load_admin(geojson_path)
    log("STAGE-1", f"Границы загружены: {len(admin_small):,} районов")

    engine = create_engine(db_url)

    # ── Auth ──────────────────────────────────────────────────────────
    log("STAGE-1", "Загрузка авторизаций из БД")
    with engine.connect() as conn:
        rs = conn.exec_driver_sql(auth_query)
        auth_raw = pd.DataFrame(rs.fetchall(), columns=list(rs.keys()))
    if auth_raw.empty:
        raise RuntimeError("No auth rows returned from DB")
    log("STAGE-1", f"Auth строк: {len(auth_raw):,}")

    auth_df = enrich_df(auth_raw, admin_small)

    # ── Transfers ─────────────────────────────────────────────────────
    log("STAGE-1T", "Загрузка переводов из БД")
    try:
        with engine.connect() as conn:
            rs = conn.exec_driver_sql(transfers_query)
            tr_raw = pd.DataFrame(rs.fetchall(), columns=list(rs.keys()))
        log("STAGE-1T", f"Transfers строк: {len(tr_raw):,}")
    except Exception as exc:
        log("STAGE-1T", f"Transfers query failed: {exc} – продолжаем без переводов")
        tr_raw = pd.DataFrame()

    if not tr_raw.empty:
        tr_df = enrich_transfers_df(tr_raw, admin_small)
        log("STAGE-4T", "Запись transfers parquet")
        transfers_parquet.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(tr_df, transfers_parquet)
        log("DONE-T", f"Transfers: {len(tr_df):,} строк → {transfers_parquet}")
    else:
        tr_df = pd.DataFrame()

    # ── Session join ──────────────────────────────────────────────────
    # Load transfers again with session query (may include finer timestamp)
    if session_transfers_query and session_transfers_query.strip():
        log("SESSION", "Загрузка transfers для session join (SESSION_TRANSFERS_QUERY)")
        try:
            with engine.connect() as conn:
                rs = conn.exec_driver_sql(session_transfers_query)
                sess_tr_raw = pd.DataFrame(rs.fetchall(), columns=list(rs.keys()))
            log("SESSION", f"Session transfers строк: {len(sess_tr_raw):,}")
            if not sess_tr_raw.empty:
                # Minimal normalisation: just need iin, trans_ts, amount_kzt, purpose
                sess_tr_raw["iin"] = sess_tr_raw["iin"].astype("string") if "iin" in sess_tr_raw else pd.NA
                ts_col = next((c for c in ("trans_ts", "trans_date") if c in sess_tr_raw.columns), None)
                if ts_col and ts_col != "trans_ts":
                    sess_tr_raw["trans_ts"] = sess_tr_raw[ts_col]
                lat_c = next((c for c in ("lat", "latitude") if c in sess_tr_raw.columns), None)
                lon_c = next((c for c in ("lon", "longitude") if c in sess_tr_raw.columns), None)
                if lat_c:
                    sess_tr_raw["lat"] = pd.to_numeric(sess_tr_raw[lat_c], errors="coerce")
                if lon_c:
                    sess_tr_raw["lon"] = pd.to_numeric(sess_tr_raw[lon_c], errors="coerce")
                auth_df = enrich_with_transfers(auth_df, sess_tr_raw, window_minutes)
            else:
                log("SESSION", "Session transfers query returned 0 rows – skipping join")
        except Exception as exc:
            log("SESSION", f"Session transfers query failed: {exc} – skipping join")
    elif not tr_df.empty:
        # Fallback: use already-loaded transfers_df (may have date-only precision)
        log("SESSION", "SESSION_TRANSFERS_QUERY not set – using transfers parquet data for session join")
        auth_df = enrich_with_transfers(auth_df, tr_df, window_minutes)

    # ── Write enriched auth ───────────────────────────────────────────
    log("STAGE-4", "Запись enriched auth parquet")
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    write_parquet(auth_df, out_parquet)

    tagged = int(auth_df["rayon_id"].notna().sum())
    has_tr = int(auth_df.get("has_transfer", pd.Series(False)).sum())
    log("DONE", f"Auth enriched: {len(auth_df):,} строк  |  geo-tagged: {tagged:,}  |  с transfer сессией: {has_tr:,}")
    log("DONE", f"Output: {out_parquet}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build map parquet from DB")
    parser.add_argument("--db-url", default=(CONFIG_DB_URL or os.getenv("DB_URL", "")), help="SQLAlchemy DB URL")
    parser.add_argument(
        "--query",
        default=os.getenv("SOURCE_QUERY", DEFAULT_QUERY),
        help="SQL query for clients (auth events)",
    )
    parser.add_argument(
        "--transfers-query",
        default=os.getenv("TRANSFERS_QUERY", DEFAULT_TRANSFERS_QUERY),
        help="SQL query for transfers standalone parquet (dm_mobile_transfers)",
    )
    parser.add_argument(
        "--session-transfers-query",
        default=DEFAULT_SESSION_TRANSFERS_QUERY,
        help="SQL query for session join (must return trans_ts as TIMESTAMP)",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=SESSION_WINDOW_MINUTES,
        help="Session window in minutes for auth↔transfer join",
    )
    parser.add_argument("--chunksize", type=int, default=int(os.getenv("CHUNKSIZE", "200000")))
    parser.add_argument("--out", default=str(OUT_PARQUET))
    parser.add_argument("--transfers-out", default=str(TRANSFERS_PARQUET_PATH))
    parser.add_argument("--geojson", default=str(GEOJSON_PATH))
    parser.add_argument(
        "--no-session-join", action="store_true", default=False,
        help="Skip session join; build auth and transfers parquets separately (legacy mode)",
    )
    args = parser.parse_args()
    if not args.db_url:
        raise SystemExit("Provide --db-url or DB_URL env var")
    return args


def main() -> None:
    args = parse_args()

    if args.no_session_join:
        # Legacy mode: separate builds without enrichment
        run(
            db_url=args.db_url,
            query=args.query,
            out_parquet=Path(args.out),
            geojson_path=Path(args.geojson),
        )
        run_transfers(
            db_url=args.db_url,
            query=args.transfers_query,
            out_parquet=Path(args.transfers_out),
            geojson_path=Path(args.geojson),
        )
        return

    run_enriched(
        db_url=args.db_url,
        auth_query=args.query,
        transfers_query=args.transfers_query,
        session_transfers_query=args.session_transfers_query,
        out_parquet=Path(args.out),
        transfers_parquet=Path(args.transfers_out),
        geojson_path=Path(args.geojson),
        window_minutes=args.window_minutes,
    )


if __name__ == "__main__":
    main()
