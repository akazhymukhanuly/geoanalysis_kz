from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine


# =========================
# LOCAL CONFIG ONLY
# =========================
BASE_DIR = Path(__file__).resolve().parent

# Put the actual DB URL here.
# Examples:
#   "postgresql+psycopg2://user:pass@host:5432/dbname"
#   "mssql+pyodbc://user:pass@dsn"
DB_URL = ""

COUNTRIES_GEOJSON_PATH = BASE_DIR / "data" / "ne_50m_admin_0_countries.geojson"
OUTPUT_PARQUET_PATH = BASE_DIR / "data" / "clients_with_country.parquet"

SQL_QUERY = """
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
"""


def log(stage: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{stage}] {msg}", flush=True)


def pick_col(cols: list[str], *candidates: str) -> str | None:
    lowered = {c.lower(): c for c in cols}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def load_countries(geojson_path: Path) -> gpd.GeoDataFrame:
    if not geojson_path.exists():
        raise FileNotFoundError(f"GeoJSON not found: {geojson_path}")

    gdf = gpd.read_file(geojson_path)
    cols = list(gdf.columns)

    country_col = pick_col(
        cols,
        "ADMIN",
        "ADMIN_EN",
        "NAME",
        "NAME_EN",
        "SOVEREIGNT",
        "BRK_NAME",
    )
    if not country_col:
        raise RuntimeError(
            f"Country name column not found in {geojson_path.name}. "
            f"Available columns: {', '.join(cols)}"
        )

    gdf = gdf[gdf["geometry"].notna()].copy()
    gdf["country"] = gdf[country_col].astype("string")
    gdf = gdf[gdf["country"].notna()].copy()

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs(4326)

    try:
        gdf["geometry"] = gdf["geometry"].make_valid()
    except Exception:
        gdf["geometry"] = gdf["geometry"].buffer(0)

    gdf["area_m2"] = gdf.to_crs(3857).area
    return gdf[["country", "area_m2", "geometry"]].copy()


def read_source_df(db_url: str, query: str) -> pd.DataFrame:
    if not db_url.strip():
        raise RuntimeError("DB_URL is empty. Fill it inside build_country_from_db.py")

    engine = create_engine(db_url)
    with engine.connect() as conn:
        rs = conn.exec_driver_sql(query)
        rows = rs.fetchall()
        cols = list(rs.keys())
    if not rows:
        raise RuntimeError("Query returned 0 rows")
    return pd.DataFrame(rows, columns=cols)


def normalize_points(df: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
    cols = list(df.columns)
    lat_col = pick_col(cols, "lat", "latitude", "device_geo_latitude", "geo_lat", "geo_latitude")
    lon_col = pick_col(cols, "lon", "lng", "longitude", "device_geo_longitude", "geo_lon", "geo_longitude")
    if not lat_col or not lon_col:
        raise RuntimeError("Source query must return lat/lon columns")

    out = df.copy()
    out[lat_col] = pd.to_numeric(out[lat_col], errors="coerce")
    out[lon_col] = pd.to_numeric(out[lon_col], errors="coerce")
    out = out.dropna(subset=[lat_col, lon_col]).copy()
    out = out[
        out[lat_col].between(-90, 90)
        & out[lon_col].between(-180, 180)
    ].copy()

    if out.empty:
        raise RuntimeError("No valid rows after lat/lon normalization")

    return out, lat_col, lon_col


def enrich_with_country(df: pd.DataFrame, countries: gpd.GeoDataFrame) -> pd.DataFrame:
    work, lat_col, lon_col = normalize_points(df)

    points = gpd.GeoDataFrame(
        work.reset_index(drop=False).rename(columns={"index": "src_idx"}),
        geometry=gpd.points_from_xy(work[lon_col], work[lat_col]),
        crs="EPSG:4326",
    )

    joined = gpd.sjoin(
        points,
        countries,
        how="left",
        predicate="intersects",
    )

    joined = (
        joined.sort_values(["src_idx", "area_m2"], ascending=[True, True])
        .drop_duplicates(subset=["src_idx"], keep="first")
        .sort_values("src_idx")
    )

    out = pd.DataFrame(joined.drop(columns=["geometry", "index_right", "area_m2"], errors="ignore"))
    out["country"] = out["country"].fillna("Unknown").astype("string")
    out = out.drop(columns=["src_idx"], errors="ignore")
    return out.reset_index(drop=True)


def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(output_path, index=False)
    except Exception as exc:
        log("WRITE", f"pandas.to_parquet failed ({exc}); fallback to duckdb COPY")
        con = duckdb.connect(database=":memory:")
        con.register("out_df", df)
        target = str(output_path).replace("\\", "/").replace("'", "''")
        con.execute(f"COPY out_df TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.close()


def main() -> None:
    log("START", "Loading countries GeoJSON")
    countries = load_countries(COUNTRIES_GEOJSON_PATH)
    log("START", f"Countries loaded: {len(countries)}")

    log("DB", "Reading source data from DB")
    raw_df = read_source_df(DB_URL, SQL_QUERY)
    log("DB", f"Rows from DB: {len(raw_df)}")

    log("ENRICH", "Assigning country by point-in-polygon")
    result = enrich_with_country(raw_df, countries)
    tagged = int((result["country"] != "Unknown").sum())
    log("ENRICH", f"Rows with country: {tagged} / {len(result)}")

    log("WRITE", f"Writing parquet: {OUTPUT_PARQUET_PATH}")
    write_parquet(result, OUTPUT_PARQUET_PATH)
    log("DONE", f"Saved: {OUTPUT_PARQUET_PATH}")


if __name__ == "__main__":
    main()
