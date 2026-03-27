from __future__ import annotations
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "staticgeo"
TEMPLATES_DIR = BASE_DIR / "templates"
CACHE_DIR = BASE_DIR / "map_cache"

PARQUET_PATH = DATA_DIR / "clients_enriched.parquet"
KZ_GEOJSON_PATH = DATA_DIR / "Open_dataset_of_administrative_boundaries_of_Kazakhstan.geojson"
INFRA_XLSX_PATH = DATA_DIR / "2gis_Full_Data_Rubrics.xlsx"
INFRA_PARQUET_PATH = DATA_DIR / "infra_points.parquet"
DB_PATH = BASE_DIR / "geo.duckdb"
MAX_CACHE_ZOOM = 16
MAX_SERVE_ZOOM = 18

PROXY_TILE_URL = os.getenv("PROXY_TILE_URL", "10.152.203.53:3128").strip()
UPSTREAM_TILE_URL = os.getenv("UPSTREAM_TILE_URL", "https://tile.openstreetmap.org/{z}/{x}/{y}.png").strip()
DB_URL = os.getenv("DB_URL", "").strip()
ENABLE_INFRA = os.getenv("ENABLE_INFRA", "1").strip().lower() in ("1", "true", "yes", "on")
