from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

from config import DB_URL as CONFIG_DB_URL

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
GEOJSON_PATH = DATA_DIR / "Open_dataset_of_administrative_boundaries_of_Kazakhstan.geojson"
OUT_PARQUET = DATA_DIR / "clients_enriched.parquet"

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build map parquet from DB")
    parser.add_argument("--db-url", default=(CONFIG_DB_URL or os.getenv("DB_URL", "")), help="SQLAlchemy DB URL")
    parser.add_argument(
        "--query",
        default=os.getenv(
            "SOURCE_QUERY",
            """
            SELECT
              updated_at,
              iin,
              phone,
              device_name,
              device_geo_latitude AS lat,
              device_geo_longitude AS lon
            FROM ods.parsed_auth_entries pae
            WHERE updated_at >= (CURRENT_DATE - INTERVAL '3 months')
              AND updated_at < (CURRENT_DATE + INTERVAL '1 day')
              AND device_geo_latitude IS NOT NULL
              AND device_geo_longitude IS NOT NULL
              AND device_geo_latitude > 0.0
            GROUP BY 1,2,3,4,5,6
            """,
        ),
        help="SQL query returning points and metadata",
    )
    parser.add_argument("--chunksize", type=int, default=int(os.getenv("CHUNKSIZE", "200000")))
    parser.add_argument("--out", default=str(OUT_PARQUET))
    parser.add_argument("--geojson", default=str(GEOJSON_PATH))
    args = parser.parse_args()
    if not args.db_url:
        raise SystemExit("Provide --db-url or DB_URL env var")
    return args


def main() -> None:
    args = parse_args()
    run(
        db_url=args.db_url,
        query=args.query,
        out_parquet=Path(args.out),
        geojson_path=Path(args.geojson),
    )


if __name__ == "__main__":
    main()
