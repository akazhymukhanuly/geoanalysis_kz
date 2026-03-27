from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
SRC_PARQUET = Path(os.getenv("SRC_PARQUET", str(DATA_DIR / "clients_ready.parquet")))
GEOJSON = DATA_DIR / "Open_dataset_of_administrative_boundaries_of_Kazakhstan.geojson"
OUT_PARQUET = DATA_DIR / "clients_enriched.parquet"


def pick_col(cols: list[str], *candidates: str) -> str | None:
    m = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in m:
            return m[c.lower()]
    return None


def main() -> None:
    if not SRC_PARQUET.exists():
        raise FileNotFoundError(f"Missing source parquet: {SRC_PARQUET}")
    if not GEOJSON.exists():
        raise FileNotFoundError(f"Missing geojson: {GEOJSON}")

    df = pd.read_parquet(SRC_PARQUET)
    cols = list(df.columns)

    lat_col = pick_col(cols, "lat", "latitude")
    lon_col = pick_col(cols, "lon", "lng", "longitude")
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
    device_id_col = pick_col(cols, "device_id", "device_uuid", "device_guid", "device")
    device_type_col = pick_col(cols, "device_type", "device_model", "platform", "os")
    ip_col = pick_col(cols, "ip", "ip_address", "ip_addr")

    if not lat_col or not lon_col:
        raise RuntimeError("Source parquet must contain lat/lon columns")

    # ---- 1) Координаты в числа + фильтр ----
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    df = df.dropna(subset=[lat_col, lon_col]).copy()

    # ---- 2) hour / event_ts / event_date (как у тебя) ----
    if hour_col:
        df["hour"] = pd.to_numeric(df[hour_col], errors="coerce").fillna(0).astype(int)
    elif dt_col:
        ts = pd.to_datetime(df[dt_col], errors="coerce")
        df["hour"] = ts.dt.hour.fillna(0).astype(int)
    elif iin_col:
        key = df[iin_col].astype(str) + "|" + df[lat_col].astype(str) + "|" + df[lon_col].astype(str)
        df["hour"] = (key.map(hash).abs() % 24).astype(int)
    else:
        df["hour"] = 0

    if dt_col:
        df["event_ts"] = pd.to_datetime(df[dt_col], errors="coerce")
        df["event_date"] = df["event_ts"].dt.date
    else:
        key = df[lat_col].astype(str) + "|" + df[lon_col].astype(str)
        days_back = (key.map(hash).abs() % 90).astype(int)
        base = pd.Timestamp.today().normalize() - pd.to_timedelta(days_back, unit="D")
        df["event_ts"] = base + pd.to_timedelta(df["hour"], unit="h")
        df["event_date"] = df["event_ts"].dt.date

    # ---- 3) ip/device поля (как у тебя) ----
    df["ip_addr"] = df[ip_col].astype(str) if ip_col else None
    df["device_id"] = df[device_id_col].astype(str) if device_id_col else None
    df["device_type"] = df[device_type_col].astype(str) if device_type_col else None

    # ---- 4) GeoDataFrame точек: авто-CRS (4326 vs 3857) ----
    is_degrees_like = (
        df[lat_col].between(-90, 90).all()
        and df[lon_col].between(-180, 180).all()
    )

    gdf = gpd.GeoDataFrame(
        df.reset_index(drop=True),
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326" if is_degrees_like else "EPSG:3857",
    )
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    # ---- 5) Полигоны районов ----
    admin = gpd.read_file(GEOJSON)

    # Берём именно районы: у тебя для районов заполнен oblast_kk
    admin = admin[
        admin["full_id"].notna()
        & admin["name_kk"].notna()
        & admin["oblast_kk"].notna()
    ].copy()

    admin["rayon_id"] = admin["full_id"].astype("string")
    admin["rayon_name"] = admin["name_kk"].astype("string")
    admin["oblast_kk"] = admin["oblast_kk"].astype("string")

    admin = admin.set_geometry("geometry").to_crs(4326)

    # чиним геометрию (OSM часто даёт невалидные)
    try:
        admin["geometry"] = admin["geometry"].make_valid()
    except Exception:
        admin["geometry"] = admin["geometry"].buffer(0)

    # площадь для выбора самого маленького совпадения
    admin["area_m2"] = admin.to_crs(3857).area

    admin_small = admin[["rayon_id", "rayon_name", "oblast_kk", "area_m2", "geometry"]].copy()

    # ---- 6) Spatial join ----
    # intersects устойчивее, чем within (точки на границе не теряются)
    joined = gpd.sjoin(
        gdf.reset_index(drop=False).rename(columns={"index": "pt_idx"}),
        admin_small,
        how="left",
        predicate="intersects",
    )

    # если несколько совпадений — берём самый маленький полигон (район)
    joined = (
        joined.sort_values(["pt_idx", "area_m2"], ascending=[True, True])
        .drop_duplicates(subset=["pt_idx"], keep="first")
        .set_index("pt_idx")
        .sort_index()
    )

    # ВАЖНО: не превращаем NaN в "nan"
    joined["rayon_id"] = joined["rayon_id"].astype("string")
    joined["rayon_name"] = joined["rayon_name"].astype("string")
    joined["oblast_kk"] = joined["oblast_kk"].astype("string")

    # ---- 7) Формируем выход (с теми же колонками, как раньше) ----
    reserved = {
        "event_ts",
        "event_date",
        "hour",
        "ip_addr",
        "device_id",
        "device_type",
        "oblast_kk",
        "rayon_id",
        "rayon_name",
        "geometry",
        "index_right",
        "area_m2",
        "pt_idx",
    }
    base_cols = [c for c in df.columns if c.lower() not in reserved]

    out_cols = base_cols + [
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

    out = pd.DataFrame(joined.drop(columns=["geometry", "index_right", "area_m2"], errors="ignore"))

    # гарантируем порядок и наличие нужных колонок
    out = out.reindex(columns=out_cols)

    out.to_parquet(OUT_PARQUET, index=False)

    print(f"Built: {OUT_PARQUET}")
    print(f"Rows: {len(df)}")
    print(f"Tagged with rayon: {out['rayon_id'].notna().sum()}")
    print("Detected columns:")
    print(f"  lat_col={lat_col}, lon_col={lon_col}")
    print(f"  dt_col={dt_col}")
    print(f"  hour_col={hour_col}")
    print(f"  ip_col={ip_col}")
    print(f"  device_id_col={device_id_col}")
    print(f"  device_type_col={device_type_col}")


if __name__ == "__main__":
    main()