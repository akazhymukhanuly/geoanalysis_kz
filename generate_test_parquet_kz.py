from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
GEOJSON_PATH = DATA_DIR / "Open_dataset_of_administrative_boundaries_of_Kazakhstan.geojson"
OUT_PARQUET = DATA_DIR / "clients_enriched.parquet"
N_POINTS = 100_000
SEED = 42
WORK_CRS = "EPSG:32643"


def _rand_iin(rng: np.random.Generator) -> str:
    return "".join(rng.choice(list("0123456789"), size=12))


def _rand_ip(rng: np.random.Generator) -> str:
    return f"{int(rng.integers(1,255))}.{int(rng.integers(0,256))}.{int(rng.integers(0,256))}.{int(rng.integers(1,255))}"


def _format_with_random_precision(value: float, rng: np.random.Generator) -> str:
    decimals = int(rng.integers(6, 15))  # 6..14
    return f"{value:.{decimals}f}"


def _sample_point_in_polygon(poly, rng: np.random.Generator, max_tries: int = 500):
    minx, miny, maxx, maxy = poly.bounds
    for _ in range(max_tries):
        x = rng.uniform(minx, maxx)
        y = rng.uniform(miny, maxy)
        p = Point(x, y)
        if poly.covers(p):
            return p
    return poly.representative_point()


def main() -> None:
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(f"Missing geojson: {GEOJSON_PATH}")

    rng = np.random.default_rng(SEED)

    admin = gpd.read_file(GEOJSON_PATH)
    admin = admin[admin["full_id"].notna() & admin["name_kk"].notna() & admin["oblast_kk"].notna()].copy()
    admin = admin[admin.geometry.notna()].copy()
    if admin.crs is None:
        admin = admin.set_crs("EPSG:4326")
    else:
        admin = admin.to_crs("EPSG:4326")

    admin["rayon_id"] = admin["full_id"].astype(str)
    admin["rayon_name"] = admin["name_kk"].astype(str)
    admin["oblast_kk"] = admin["oblast_kk"].astype(str)

    admin_m = admin.to_crs(WORK_CRS).copy()
    admin_m["area_m2"] = admin_m.geometry.area
    admin_m = admin_m[admin_m["area_m2"] > 0].reset_index(drop=True)

    weights = admin_m["area_m2"].to_numpy(dtype=float)
    weights = weights / weights.sum()

    chosen_idx = rng.choice(len(admin_m), size=N_POINTS, p=weights)

    sampled_geom = []
    rayon_ids = []
    rayon_names = []
    oblasts = []

    for i, idx in enumerate(chosen_idx, start=1):
        row = admin_m.iloc[int(idx)]
        p = _sample_point_in_polygon(row.geometry, rng)
        sampled_geom.append(p)
        rayon_ids.append(row.rayon_id)
        rayon_names.append(row.rayon_name)
        oblasts.append(row.oblast_kk)
        if i % 20000 == 0:
            print(f"sampled: {i}/{N_POINTS}", flush=True)

    points_m = gpd.GeoDataFrame({"rayon_id": rayon_ids}, geometry=sampled_geom, crs=WORK_CRS)
    points_wgs = points_m.to_crs("EPSG:4326")

    now = datetime.now().replace(microsecond=0)
    days_back = rng.integers(0, 90, size=N_POINTS)
    hours = rng.integers(0, 24, size=N_POINTS)
    mins = rng.integers(0, 60, size=N_POINTS)
    secs = rng.integers(0, 60, size=N_POINTS)

    event_ts = [
        now - timedelta(days=int(d), hours=int(23 - h), minutes=int(m), seconds=int(s))
        for d, h, m, s in zip(days_back, hours, mins, secs)
    ]

    device_types = np.array(["android", "ios", "web", "huawei"], dtype=object)

    lats = []
    lons = []
    for geom in points_wgs.geometry:
        lats.append(_format_with_random_precision(float(geom.y), rng))
        lons.append(_format_with_random_precision(float(geom.x), rng))

    out = pd.DataFrame(
        {
            "updated_at": event_ts,
            "event_ts": event_ts,
            "event_date": [x.date() for x in event_ts],
            "hour": [x.hour for x in event_ts],
            "iin": [_rand_iin(rng) for _ in range(N_POINTS)],
            "lat": lats,
            "lon": lons,
            "ip_addr": [_rand_ip(rng) for _ in range(N_POINTS)],
            "device_id": [f"dev-{int(x)}" for x in rng.integers(10_000_000, 99_999_999, size=N_POINTS)],
            "device_type": device_types[rng.integers(0, len(device_types), size=N_POINTS)],
            "oblast_kk": oblasts,
            "rayon_id": rayon_ids,
            "rayon_name": rayon_names,
        }
    )

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    con.register("out_df", out)
    target = str(OUT_PARQUET).replace("\\", "/").replace("'", "''")
    con.execute(f"COPY out_df TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.close()

    print(f"Built: {OUT_PARQUET}")
    print(f"Rows: {len(out)}")
    print(f"Unique rayons: {out['rayon_id'].nunique()}")
    print(f"Unique oblasts: {out['oblast_kk'].nunique()}")


if __name__ == "__main__":
    main()
