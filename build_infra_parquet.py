from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import DATA_DIR, INFRA_PARQUET_PATH, INFRA_XLSX_PATH


TYPE_MAP = {
    "Супермаркеты": "supermarket",
    "СПА": "spa",
    "Медицинские_услуги": "medical",
    "Кафе_и_рестораны": "food",
    "Товары_для_детей": "kids",
    "Салоны_красоты_и_косметика": "beauty",
    "Мебель": "furniture",
    "Одежда_и_обувь": "fashion",
    "Путешествия": "travel",
    "Фитнес": "fitness",
    "Образование": "education",
    "АЗС": "gas",
}


def pick_col(cols: list[str], candidates: list[str]) -> str | None:
    lowered = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in lowered:
            return lowered[c.lower()]
    return None


def build_from_xlsx(xlsx_path: Path, out_path: Path) -> None:
    all_points: list[dict] = []
    all_sheets = pd.read_excel(xlsx_path, sheet_name=None, engine="openpyxl")

    for sheet_name, t in TYPE_MAP.items():
        df = all_sheets.get(sheet_name)
        if df is None:
            print(f"[infra] skip sheet '{sheet_name}': not found")
            continue

        if df.empty:
            continue

        if "CITY_ID_SEARCH" in df.columns:
            df = df[pd.to_numeric(df["CITY_ID_SEARCH"], errors="coerce") == 67].copy()
        if df.empty:
            continue

        cols = list(df.columns)
        col_lat = pick_col(cols, ["координаты (lon, lat) (point.lat)", "point.lat", "lat", "latitude"])
        col_lon = pick_col(cols, ["координаты (lon, lat) (point.lon)", "point.lon", "lon", "longitude"])
        col_name = pick_col(cols, ["name", "составное наименование (name_ex.primary)", "full_name"])
        col_addr = pick_col(cols, ["полный адрес с городом", "address_name", "address"])
        col_rating = pick_col(cols, ["статистика отзывов (reviews.general_rating)", "rating"])
        col_reviews = pick_col(cols, ["статистика отзывов (reviews.general_review_count)", "reviews"])

        if not col_lat or not col_lon:
            continue

        cur = pd.DataFrame()
        cur["lat"] = pd.to_numeric(df[col_lat], errors="coerce")
        cur["lon"] = pd.to_numeric(df[col_lon], errors="coerce")
        cur["name"] = (df[col_name].astype(str) if col_name else "Без названия").fillna("Без названия")
        cur["address"] = (df[col_addr].astype(str) if col_addr else "").fillna("")
        cur["rating"] = pd.to_numeric(df[col_rating], errors="coerce") if col_rating else None
        cur["reviews"] = pd.to_numeric(df[col_reviews], errors="coerce") if col_reviews else None
        cur["type"] = t

        cur = cur.dropna(subset=["lat", "lon"]).copy()
        cur = cur[(cur["lat"].between(-90, 90)) & (cur["lon"].between(-180, 180))].copy()
        all_points.extend(cur.to_dict(orient="records"))

    out = pd.DataFrame(all_points, columns=["name", "address", "type", "rating", "reviews", "lat", "lon"])
    out = out.drop_duplicates(subset=["name", "lat", "lon", "type"]).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print(f"[infra] parquet built: {out_path}")
    print(f"[infra] rows: {len(out)}")


def main() -> None:
    if not INFRA_XLSX_PATH.exists():
        raise SystemExit(f"xlsx not found: {INFRA_XLSX_PATH}")
    build_from_xlsx(INFRA_XLSX_PATH, INFRA_PARQUET_PATH)


if __name__ == "__main__":
    main()
