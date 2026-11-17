# -*- coding: utf-8 -*-
"""Cau hinh va ham dung chung cho pipeline tram y te Da Nang cu (MIS5007).

Moi script ghi log vao logs/ va dang ky file dau ra vao outputs/run_manifest.csv
(ten file, so dong, sha256, script tao ra, thoi diem) de moi con so trong bao cao
truy nguoc duoc ve mot file cu the.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
REF = ROOT / "data" / "reference"
INTERIM = ROOT / "data" / "interim"
OUT = ROOT / "outputs"
TAB = OUT / "tables"
FIG = OUT / "figures"
LOGS = ROOT / "logs"
LANDING = ROOT / "data" / "landing"     # CSV tho cho Spark (11a_landing.py) = vung "thu nhan"
SPARK_OUT = ROOT / "data" / "spark"     # vung curated / quarantine / gold cua Spark khi khong co HDFS
for _p in (INTERIM, OUT, TAB, FIG, LOGS):
    _p.mkdir(parents=True, exist_ok=True)

# ---- Tham so phan tich (moi tham so la GIA DINH, xem danh sach gia dinh trong bao cao) ----
BBOX = (107.78, 15.88, 108.36, 16.24)      # W, S, E, N - hop bao Da Nang cu, co vung dem
CRS_M = "EPSG:32649"                       # UTM 49N, don vi met, de do khoang cach
# Pham vi du an: 2015 -> 31/12/2024 (truoc khi sap xep don vi hanh chinh 01/01/2025). Khong du bao.
BASE_YEAR = 2024                           # nam hien trang = nam cuoi truoc sap xep (WorldPop R2025A)
YEARS = list(range(2015, 2025))            # 10 moc nam 2015..2024
PANEL = INTERIM / f"grid_panel_{YEARS[0]}_{YEARS[-1]}.parquet"
FORECAST_YEAR = 2030                       # nam dich cua buoc 08 (du bao); KHONG thuoc cac nam phan tich
THRESH_KM = [2.0, 3.0, 5.0]                # do nhay; nguong chinh 3 km
MAIN_KM = 3.0
WHITE_KM = 5.0                             # "vung trang": cach tram gan nhat > 5 km
AGE60_BANDS = [60, 65, 70, 75, 80, 85, 90]
# toc do xe may gia dinh (km/h) theo loai duong OSM - chi dung cho chi so thoi gian phu
SPEED_KMH = {
    "motorway": 50, "trunk": 40, "primary": 35, "secondary": 30, "tertiary": 30,
    "unclassified": 25, "residential": 20, "living_street": 15, "service": 15,
    "track": 12, "road": 20,
}
WALK_KMH = 5.0                             # doan tu tam o luoi ra duong gan nhat

DISTRICTS = ["Quận Hải Châu", "Quận Thanh Khê", "Quận Sơn Trà", "Quận Ngũ Hành Sơn",
             "Quận Liên Chiểu", "Quận Cẩm Lệ", "Huyện Hòa Vang"]

RUN_MANIFEST = OUT / "run_manifest.csv"


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_logger(name: str) -> logging.Logger:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lg = logging.getLogger(name)
    lg.setLevel(logging.INFO)
    lg.handlers.clear()
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    fh = logging.FileHandler(LOGS / f"{name}_{ts}.log", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    lg.addHandler(fh)
    lg.addHandler(sh)
    return lg


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def register(path: Path, script: str, rows: int | None = None, note: str = "") -> None:
    """Ghi (hoac cap nhat) mot dong trong outputs/run_manifest.csv."""
    path = Path(path)
    rel = str(path.relative_to(ROOT))
    rec = {"file": rel, "rows": "" if rows is None else rows, "bytes": path.stat().st_size,
           "sha256": sha256(path), "script": script, "created_utc": now_utc(), "note": note}
    recs = []
    if RUN_MANIFEST.exists():
        with open(RUN_MANIFEST, newline="", encoding="utf-8") as f:
            recs = [r for r in csv.DictReader(f) if r["file"] != rel]
    recs.append(rec)
    with open(RUN_MANIFEST, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rec.keys()))
        w.writeheader()
        w.writerows(recs)


def save_csv(df, path: Path, script: str, note: str = "") -> None:
    df.to_csv(path, index=False, encoding="utf-8-sig")
    register(path, script, rows=len(df), note=note)


def save_json(obj, path: Path, script: str, note: str = "") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=float)
    register(path, script, note=note)


# ---- Chuan hoa ten dia danh tieng Viet -------------------------------------
_TONE_FIX = {  # dua dau thanh ve cung mot kieu (hoà -> hòa, thuý -> thúy ...)
    "oà": "òa", "oá": "óa", "oả": "ỏa", "oã": "õa", "oạ": "ọa",
    "oè": "òe", "oé": "óe", "oẻ": "ỏe", "oẽ": "õe", "oẹ": "ọe",
    "uỳ": "ùy", "uý": "úy", "uỷ": "ủy", "uỹ": "ũy", "uỵ": "ụy",
}
_PREFIX = re.compile(r"^(phường|phuong|xã|xa|thị trấn|thi tran|p\.|x\.)\s+")
_ROMAN = {"1": "i", "2": "ii", "3": "iii"}


def norm_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFC", str(s)).lower().strip()
    for a, b in _TONE_FIX.items():
        s = s.replace(a, b)
    s = re.sub(r"\s+", " ", s)
    s = _PREFIX.sub("", s)
    s = re.sub(r"\b([123])$", lambda m: _ROMAN[m.group(1)], s)  # "hải châu 1" -> "hải châu i"
    return s.strip()


def strip_station_name(s: str) -> str:
    """'Trạm Y tế phường Hòa An' -> 'hòa an' (da chuan hoa)."""
    t = unicodedata.normalize("NFC", str(s)).lower()
    for a, b in _TONE_FIX.items():
        t = t.replace(a, b)
    t = re.sub(r"tr[aạ]m\s+y\s+t[eế]", " ", t)
    t = re.sub(r"\b(tyt)\b", " ", t)
    t = re.sub(r"[\(\)\-,–]", " ", t)
    t = re.sub(r"\b(quận|huyện|thành phố|tp\.?|đà nẵng)\b.*$", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return norm_name(t)


def to_metric(lon, lat):
    from pyproj import Transformer
    tr = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True)
    return tr.transform(np.asarray(lon), np.asarray(lat))


def wmedian(values, weights) -> float:
    v = np.asarray(values, float)
    w = np.asarray(weights, float)
    m = np.isfinite(v) & (w > 0)
    v, w = v[m], w[m]
    if len(v) == 0:
        return float("nan")
    o = np.argsort(v)
    v, w = v[o], w[o]
    cw = np.cumsum(w)
    return float(v[np.searchsorted(cw, 0.5 * cw[-1])])
