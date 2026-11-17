# -*- coding: utf-8 -*-
"""11a - THU NHAN: chuyen du lieu nguon (raster .tif, Excel, Parquet trung gian) thanh CSV THO cho Spark.

Spark khong doc duoc GeoTIFF / Excel, nen buoc nay chi "do" du lieu ra CSV, KHONG lam sach:
  - giu nguyen gia tri pixel, ke ca ma NoData (-99999) cua WorldPop;
  - giu moi pixel trong hop bao, ke ca pixel ngoai 56 xa/phuong;
  - giu moi dong cua file Excel nhap tay, ke ca dong tieu de / dong ten quan.
Viec kiem tra, tach loi (quarantine) va lam sach la cua Spark (11_spark_pipeline.py).

Dau ra (data/landing/, UTF-8 khong BOM, co dong tieu de):
  pop_100m_raw.csv   year,row,col,pop_raw            moi pixel 100 m x moi nam trong YEARS
  cells_dim.csv      row,col,cell_id,lon,lat,x_m,y_m,ma_tram,k1_row,k1_col,elev_m,slope_deg
  age_1km_raw.csv    year,k1_row,k1_col,bien,gia_tri bien = pop | t_60 | t_65 | ... | t_90 (luoi 1 km)
  stations_raw.csv   dong_excel,cot1,cot2,cot3       nguyen van data/raw/location-tyt.xlsx
  net_dist.csv       cell_id,net_dist_m,net_time_min,snap_cell_m,net_station   (san pham cua 05_network.py)
  dm_56_tram.csv     danh muc 56 tram / xa (data/reference/danh_sach_56_tram.csv)
Chay sau 05_network.py:  cd scripts && python 11a_landing.py
"""
import glob
import json

import numpy as np
import pandas as pd
import rasterio
import shapely
from pyproj import Transformer
from rasterio.merge import merge
from rasterio.windows import from_bounds
from shapely.geometry import shape

from common import (RAW, REF, INTERIM, LANDING, BBOX, BASE_YEAR, YEARS, AGE60_BANDS, CRS_M,
                    get_logger, register)

S = "11a_landing"
log = get_logger(S)
BBOX1K = (BBOX[0] - .02, BBOX[1] - .02, BBOX[2] + .02, BBOX[3] + .02)     # giong 03_grid.py


def raw_window(path, bbox):
    """Doc cua so raster, KHONG che NoData (giu gia tri tho)."""
    with rasterio.open(path) as ds:
        win = from_bounds(*bbox, transform=ds.transform).round_offsets().round_lengths()
        return ds.read(1, window=win).astype("float64"), ds.window_transform(win), ds.nodata


def write(df, name, note):
    p = LANDING / name
    df.to_csv(p, index=False, encoding="utf-8")
    register(p, S, rows=len(df), note=note)
    log.info("Ghi %s: %d dong, %d cot", name, len(df), df.shape[1])


def main():
    LANDING.mkdir(parents=True, exist_ok=True)

    # ---- 1. pixel 100 m x nam (gia tri tho) ----
    parts, tr0, shape0 = [], None, None
    for y in YEARS:
        p = RAW / "worldpop" / "pop100m" / f"vnm_pop_{y}_CN_100m_R2025A_v1.tif"
        a, tr, nd = raw_window(p, BBOX)
        if tr0 is None:
            tr0, shape0 = tr, a.shape
        assert a.shape == shape0 and tr == tr0, f"Luoi {y} khong trung luoi {YEARS[0]}"
        rows, cols = np.indices(a.shape)
        parts.append(pd.DataFrame({"year": y, "row": rows.ravel(), "col": cols.ravel(), "pop_raw": a.ravel()}))
        log.info("100 m %d: %d pixel, NoData=%s (%d pixel), pixel > 0: %d", y, a.size, nd, int((a == nd).sum()), int((a > 0).sum()))
    write(pd.concat(parts, ignore_index=True), "pop_100m_raw.csv", "pixel 100 m x nam, gia tri tho (co NoData -99999)")

    # ---- 2. danh muc o luoi: toa do, xa chua tam o, o 1 km, do cao ----
    H, W = shape0
    rows, cols = np.indices((H, W))
    d = pd.DataFrame({"row": rows.ravel(), "col": cols.ravel()})
    d["cell_id"] = [f"R{r:04d}C{c:04d}" for r, c in zip(d.row, d["col"])]
    d["lon"] = tr0.c + (d["col"] + 0.5) * tr0.a
    d["lat"] = tr0.f + (d.row + 0.5) * tr0.e
    d["x_m"], d["y_m"] = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True).transform(d.lon.values, d.lat.values)
    feats = json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]
    d["ma_tram"] = None
    for f in feats:                                     # cung thu tu va quy tac "xa dau tien chua tam o" nhu 03_grid.py
        m = shapely.contains_xy(shape(f["geometry"]), d.lon.values, d.lat.values) & d.ma_tram.isna().values
        d.loc[m, "ma_tram"] = f["properties"]["ma_tram"]
    _, t1, _ = raw_window(RAW / "worldpop" / "pop1km" / f"vnm_pop_{BASE_YEAR}_CN_1km_R2025A_UA_v1.tif", BBOX1K)
    d["k1_row"] = np.floor((d.lat.values - t1.f) / t1.e).astype(int)
    d["k1_col"] = np.floor((d.lon.values - t1.c) / t1.a).astype(int)
    tiles = sorted(glob.glob(str(RAW / "terrain" / "z11_*.tif")))
    srcs = [rasterio.open(t) for t in tiles]
    dem, dtr = merge(srcs)
    crs = srcs[0].crs
    for s_ in srcs:
        s_.close()
    dem = dem[0].astype(float)
    dem[dem < -100] = np.nan
    tx, ty = Transformer.from_crs("EPSG:4326", crs, always_xy=True).transform(d.lon.values, d.lat.values)
    rr = np.floor((ty - dtr.f) / dtr.e).astype(int).clip(0, dem.shape[0] - 1)
    cc = np.floor((tx - dtr.c) / dtr.a).astype(int).clip(0, dem.shape[1] - 1)
    k = np.cos(np.deg2rad(16.05))
    dzdy, dzdx = np.gradient(dem, abs(dtr.e) * k, dtr.a * k)
    d["elev_m"] = dem[rr, cc]
    d["slope_deg"] = np.degrees(np.arctan(np.hypot(dzdx, dzdy)))[rr, cc]
    log.info("O luoi: %d (trong 56 xa: %d, ngoai: %d)", len(d), d.ma_tram.notna().sum(), d.ma_tram.isna().sum())
    write(d, "cells_dim.csv", "moi pixel 100 m trong hop bao; ma_tram rong = ngoai 56 xa")

    # ---- 3. luoi 1 km: tong dan + 7 nhom tuoi 60-90+ (dang dai, gia tri tho) ----
    parts = []
    for y in YEARS:
        srcs1 = [("pop", RAW / "worldpop" / "pop1km" / f"vnm_pop_{y}_CN_1km_R2025A_UA_v1.tif")]
        srcs1 += [(f"t_{b}", RAW / "worldpop" / "age1km" / f"vnm_t_{b}_{y}_CN_1km_R2025A_UA_v1.tif") for b in AGE60_BANDS]
        for bien, p in srcs1:
            a, t, _ = raw_window(p, BBOX1K)
            assert t == t1, f"Luoi 1 km {p.name} khong trung"
            r1, c1 = np.indices(a.shape)
            parts.append(pd.DataFrame({"year": y, "k1_row": r1.ravel(), "k1_col": c1.ravel(), "bien": bien, "gia_tri": a.ravel()}))
    write(pd.concat(parts, ignore_index=True), "age_1km_raw.csv", "luoi 1 km: pop + 7 nhom tuoi 60-90+, gia tri tho")

    # ---- 4. toa do tram nhap tay: nguyen van file Excel ----
    x = pd.read_excel(RAW / "location-tyt.xlsx", header=None, dtype=str).iloc[:, :3]
    x.columns = ["cot1", "cot2", "cot3"]
    x.insert(0, "dong_excel", range(1, len(x) + 1))
    write(x, "stations_raw.csv", "nguyen van location-tyt.xlsx (ca dong tieu de, dong ten quan)")

    # ---- 5. khoang cach mang duong moi o (tu 05_network.py) + 6. danh muc 56 tram ----
    write(pd.read_parquet(INTERIM / "grid_network.parquet")[["cell_id", "net_dist_m", "net_time_min", "snap_cell_m", "net_station"]],
          "net_dist.csv", "duong di ngan nhat (Dijkstra) tu o toi tram - san pham 05_network.py")
    dm = pd.read_csv(REF / "danh_sach_56_tram.csv", encoding="utf-8-sig")
    write(dm[["ma_tram", "quan_huyen", "loai_dv", "ten_xa_phuong", "ten_day_du", "ten_tram"]], "dm_56_tram.csv", "danh muc 56 tram / xa")


if __name__ == "__main__":
    main()
