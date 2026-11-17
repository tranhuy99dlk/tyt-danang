# -*- coding: utf-8 -*-
"""03 - Luoi dan so 100 m (WorldPop R2025A) + 60+ + do cao, gan vao 56 xa.

Dau ra:
  data/interim/grid_100m.parquet          (1 dong / o luoi co dan, nam co so = BASE_YEAR)
  data/interim/grid_panel_<nam dau>_<nam cuoi>.parquet (o luoi x nam: pop, pop60)
  outputs/tables/commune_series.csv        (chuoi dan so, 60+ theo xa, cac nam trong YEARS)
  outputs/tables/qa_grid.csv               (kiem tra chat luong luoi)
Gia dinh (danh dau trong bao cao):
  A3: ty le 60+ cua o 100 m = ty le 60+ cua o 1 km chua no (WorldPop agesex 1 km).
  A4: nam khong co file 100 m -> pop_100m(nam) = pop_100m(nam co so) x [pop_1km(nam)/pop_1km(nam co so)]
      (giu nguyen phan bo trong o 1 km, lay nhip tang cua o 1 km).
"""
import glob
import json
import re

import numpy as np
import pandas as pd
import rasterio
from rasterio.merge import merge
from rasterio.windows import from_bounds
from pyproj import Transformer
import shapely
from shapely.geometry import shape

from common import (RAW, INTERIM, TAB, BBOX, BASE_YEAR, YEARS, AGE60_BANDS, CRS_M, PANEL,
                    get_logger, save_csv, register, now_utc)

S = "03_grid"
log = get_logger(S)


def read_window(path, bbox=BBOX):
    with rasterio.open(path) as ds:
        win = from_bounds(*bbox, transform=ds.transform).round_offsets().round_lengths()
        a = ds.read(1, window=win).astype("float64")
        nd = ds.nodata
        tr = ds.window_transform(win)
    if nd is not None:
        a[a == nd] = np.nan
    a[a < 0] = np.nan
    return a, tr


def main():
    feats = json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]
    polys = [(f["properties"]["ma_tram"], shape(f["geometry"])) for f in feats]
    log.info("Ranh gioi: %d xa", len(polys))

    f100 = sorted(glob.glob(str(RAW / "worldpop" / "pop100m" / "vnm_pop_*_CN_100m_R2025A_v1.tif")))
    y100 = {int(re.search(r"pop_(\d{4})_", p).group(1)): p for p in f100}
    y100 = {y: p for y, p in y100.items() if y in YEARS}      # chi dung nam trong pham vi du an
    log.info("File 100 m co san: %s", sorted(y100))
    assert BASE_YEAR in y100, "Thieu file 100 m nam co so"
    base, tr = read_window(y100[BASE_YEAR])
    arr = {BASE_YEAR: base}
    for y, p in y100.items():
        if y == BASE_YEAR:
            continue
        a, t2 = read_window(p)
        assert a.shape == base.shape and t2 == tr, f"Luoi {y} khong trung luoi {BASE_YEAR}"
        arr[y] = a
    H, W = base.shape
    rows, cols = np.indices((H, W))
    lon = tr.c + (cols + 0.5) * tr.a
    lat = tr.f + (rows + 0.5) * tr.e
    anypop = np.zeros_like(base, bool)
    for a in arr.values():
        anypop |= np.nan_to_num(a) > 0
    idx = np.where(anypop.ravel())[0]
    log.info("O co dan trong hop bao: %d / %d", len(idx), H * W)
    g = pd.DataFrame({"row": rows.ravel()[idx], "col": cols.ravel()[idx],
                      "lon": lon.ravel()[idx], "lat": lat.ravel()[idx]})
    for y, a in sorted(arr.items()):
        g[f"pop_{y}"] = np.nan_to_num(a.ravel()[idx])
    tot_bbox = {y: float(g[f"pop_{y}"].sum()) for y in arr}

    # gan xa
    g["ma_tram"] = None
    for code, poly in polys:
        m = shapely.contains_xy(poly, g.lon.values, g.lat.values) & g.ma_tram.isna().values
        g.loc[m, "ma_tram"] = code
    out_mask = g.ma_tram.isna()
    log.info("O ngoai 56 xa (bo): %d o, %.0f nguoi nam %d", out_mask.sum(), g.loc[out_mask, f"pop_{BASE_YEAR}"].sum(), BASE_YEAR)
    g = g[~out_mask].reset_index(drop=True)
    g.insert(0, "cell_id", [f"R{r:04d}C{c:04d}" for r, c in zip(g.row, g.col)])
    x, y = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True).transform(g.lon.values, g.lat.values)
    g["x_m"], g["y_m"] = x, y

    # ---- 1 km: tong dan va 60+ moi nam ----
    k_pop, k_60, k_ok = {}, {}, {}
    for yy in YEARS:
        p = RAW / "worldpop" / "pop1km" / f"vnm_pop_{yy}_CN_1km_R2025A_UA_v1.tif"
        if not p.exists():
            log.warning("Thieu 1 km pop %d", yy)
            continue
        a, t1 = read_window(p, (BBOX[0] - .02, BBOX[1] - .02, BBOX[2] + .02, BBOX[3] + .02))
        r1 = np.floor((g.lat.values - t1.f) / t1.e).astype(int)
        c1 = np.floor((g.lon.values - t1.c) / t1.a).astype(int)
        ok = (r1 >= 0) & (r1 < a.shape[0]) & (c1 >= 0) & (c1 < a.shape[1])
        k_pop[yy] = np.where(ok, np.nan_to_num(a[np.clip(r1, 0, a.shape[0]-1), np.clip(c1, 0, a.shape[1]-1)]), np.nan)
        s60 = np.zeros(len(g))
        miss = []
        for b in AGE60_BANDS:
            pb = RAW / "worldpop" / "age1km" / f"vnm_t_{b}_{yy}_CN_1km_R2025A_UA_v1.tif"
            if not pb.exists():
                miss.append(b)
                continue
            ab, tb = read_window(pb, (BBOX[0] - .02, BBOX[1] - .02, BBOX[2] + .02, BBOX[3] + .02))
            assert tb == t1
            s60 += np.nan_to_num(ab[np.clip(r1, 0, ab.shape[0]-1), np.clip(c1, 0, ab.shape[1]-1)])
        if miss:
            log.warning("Thieu nhom tuoi %s nam %d -> bo 60+ nam nay", miss, yy)
        else:
            k_60[yy] = s60
        if yy == BASE_YEAR:
            g["k1_row"], g["k1_col"] = r1, c1
    assert BASE_YEAR in k_pop and BASE_YEAR in k_60, "Thieu 1 km nam co so"
    kb = k_pop[BASE_YEAR]
    valid1k = kb > 0
    share = np.where(valid1k, k_60[BASE_YEAR] / np.where(valid1k, kb, 1), np.nan)
    # o 100 m co dan nhung o 1 km = 0 -> dung ty le 60+ trung binh cua xa (co danh dau)
    g["share60_src"] = np.where(valid1k, "1km", "xa_tb")
    tmp = pd.DataFrame({"ma": g.ma_tram, "a60": np.where(valid1k, k_60[BASE_YEAR], 0), "p": np.where(valid1k, kb, 0)})
    xa_share = tmp.groupby("ma").apply(lambda d: d.a60.sum() / d.p.sum() if d.p.sum() > 0 else np.nan)
    share = np.where(valid1k, share, g.ma_tram.map(xa_share).values)
    S60 = f"share60_{BASE_YEAR}"
    g[S60] = share
    g[f"pop60_{BASE_YEAR}"] = g[f"pop_{BASE_YEAR}"] * g[S60]
    log.info("O 100 m nam trong o 1 km = 0: %d (%.0f nguoi)", (~valid1k).sum(), g.loc[~valid1k, f"pop_{BASE_YEAR}"].sum())

    # ---- do cao / do doc (AWS terrain tiles) ----
    tiles = sorted(glob.glob(str(RAW / "terrain" / "z11_*.tif")))
    if tiles:
        srcs = [rasterio.open(t) for t in tiles]
        dem, dtr = merge(srcs)
        crs = srcs[0].crs
        for s_ in srcs:
            s_.close()
        dem = dem[0].astype(float)
        dem[dem < -100] = np.nan
        tx, ty = Transformer.from_crs("EPSG:4326", crs, always_xy=True).transform(g.lon.values, g.lat.values)
        rr = np.floor((ty - dtr.f) / dtr.e).astype(int).clip(0, dem.shape[0]-1)
        cc = np.floor((tx - dtr.c) / dtr.a).astype(int).clip(0, dem.shape[1]-1)
        k = np.cos(np.deg2rad(16.05))  # he so co gian Mercator tai vi do Da Nang
        dzdy, dzdx = np.gradient(dem, abs(dtr.e) * k, dtr.a * k)
        slope = np.degrees(np.arctan(np.hypot(dzdx, dzdy)))
        g["elev_m"] = dem[rr, cc]
        g["slope_deg"] = slope[rr, cc]
        log.info("Do cao: %d tile, do phan giai %.1f m; do cao trung vi o co dan %.1f m", len(tiles), dtr.a * k, np.nanmedian(g.elev_m))
    else:
        log.warning("Khong co tile do cao -> bo elev/slope")
        g["elev_m"] = np.nan
        g["slope_deg"] = np.nan

    # ---- chuoi theo nam (truc tiep 100 m neu co, neu khong thi ha cap tu 1 km - A4) ----
    # pop_ds/pop60_ds: chuoi NHAT QUAN (ha cap A4 cho moi nam, ke ca nam co 100 m) -> dung de du bao
    # pop/pop60: gia tri tot nhat co (100 m truc tiep neu co, neu khong = ha cap)
    panel = []
    src_year = {}
    p25 = g[f"pop_{BASE_YEAR}"].values
    for yy in YEARS:
        if yy not in k_pop:
            continue
        ratio = np.where(kb > 0, k_pop[yy] / np.where(kb > 0, kb, 1), 1.0)
        pop_ds = p25 * ratio
        if yy in k_60:
            r60 = np.where(kb > 0, k_60[yy] / np.where(kb > 0, kb, 1), np.nan)
            pop60_ds = np.where(np.isnan(r60), pop_ds * g[S60].values, p25 * r60)
        else:
            pop60_ds = np.full(len(g), np.nan)
        if yy in y100:
            pop = g[f"pop_{yy}"].values
            s_y = np.where(k_pop[yy] > 0, k_60.get(yy, np.zeros(len(g))) / np.where(k_pop[yy] > 0, k_pop[yy], 1), g[S60].values)
            pop60 = pop * s_y if yy in k_60 else np.full(len(g), np.nan)
            src_year[yy] = "100m_truc_tiep"
        else:
            pop, pop60 = pop_ds, pop60_ds
            src_year[yy] = "ha_cap_1km_A4"
        panel.append(pd.DataFrame({"cell_id": g.cell_id, "ma_tram": g.ma_tram, "year": yy, "pop": pop, "pop60": pop60,
                                   "pop_ds": pop_ds, "pop60_ds": pop60_ds, "nguon": src_year[yy]}))
    panel = pd.concat(panel, ignore_index=True)
    log.info("Panel o luoi x nam: %d dong (%d o x %d nam)", len(panel), len(g), panel.year.nunique())

    # kiem dinh A4: so tong xa tu 100 m truc tiep voi ha cap tu 1 km o cac nam co ca hai
    val = []
    for yy in sorted(y100):
        if yy == BASE_YEAR or yy not in k_pop:
            continue
        ratio = np.where(kb > 0, k_pop[yy] / np.where(kb > 0, kb, 1), 1.0)
        ds_ = pd.Series(g[f"pop_{BASE_YEAR}"].values * ratio).groupby(g.ma_tram).sum()
        di_ = g.groupby("ma_tram")[f"pop_{yy}"].sum()
        ape = (ds_ - di_).abs() / di_
        val.append(dict(nam=yy, mape_xa_pct=round(100 * ape.mean(), 2), ape_max_pct=round(100 * ape.max(), 2),
                        tong_truc_tiep=round(di_.sum()), tong_ha_cap=round(ds_.sum())))
        log.info("Kiem dinh A4 nam %d: MAPE theo xa %.2f%%, max %.2f%%", yy, 100 * ape.mean(), 100 * ape.max())

    # ---- chuoi theo xa ----
    cs = panel.groupby(["ma_tram", "year"]).agg(pop=("pop", "sum"), pop60=("pop60", "sum"),
                                                pop_ds=("pop_ds", "sum"), pop60_ds=("pop60_ds", "sum"),
                                                nguon=("nguon", "first")).reset_index()
    cs["ty_le_60_ds"] = cs.pop60_ds / cs.pop_ds

    # ---- QA luoi ----
    qa = [dict(kiem_tra="So o 100 m co dan trong 56 xa", gia_tri=len(g)),
          dict(kiem_tra=f"Tong dan 56 xa nam {BASE_YEAR} (100 m)", gia_tri=round(g[f'pop_{BASE_YEAR}'].sum())),
          dict(kiem_tra=f"Tong dan 60+ 56 xa nam {BASE_YEAR} (A3)", gia_tri=round(g[f'pop60_{BASE_YEAR}'].sum())),
          dict(kiem_tra="Tong 1 km (UA) cung vung, nam co so", gia_tri=round(float(np.nansum(kb)) )),
          dict(kiem_tra="Gia tri am / NaN sau lam sach (100 m nam co so)", gia_tri=int(np.isnan(g[f'pop_{BASE_YEAR}']).sum())),
          dict(kiem_tra="O dung ty le 60+ trung binh xa (o 1 km = 0)", gia_tri=int((~valid1k).sum()))]
    for yy, t in sorted(tot_bbox.items()):
        qa.append(dict(kiem_tra=f"Tong dan hop bao (ca ngoai 56 xa) nam {yy}", gia_tri=round(t)))
    for v in val:
        qa.append(dict(kiem_tra=f"Kiem dinh A4 nam {v['nam']}: MAPE theo xa (%)", gia_tri=v["mape_xa_pct"]))
        qa.append(dict(kiem_tra=f"Kiem dinh A4 nam {v['nam']}: APE lon nhat (%)", gia_tri=v["ape_max_pct"]))
    # do lech 1 km UA so voi 100 m (tren cung tap o) nam co so
    k_sum_cells = pd.DataFrame({"r": g.k1_row, "c": g.k1_col, "k": kb}).drop_duplicates(["r", "c"]).k.sum()
    qa.append(dict(kiem_tra="Tong 1 km (UA) tren cac o 1 km chua dan 56 xa, nam co so", gia_tri=round(k_sum_cells)))

    gp = INTERIM / "grid_100m.parquet"
    g.to_parquet(gp, index=False)
    register(gp, S, rows=len(g))
    pp = PANEL
    panel.to_parquet(pp, index=False)
    register(pp, S, rows=len(panel), note=f"panel o luoi x nam {YEARS[0]}-{YEARS[-1]}")
    save_csv(cs, TAB / "commune_series.csv", S, note="tong theo xa tu panel")
    save_csv(pd.DataFrame(qa), TAB / "qa_grid.csv", S)
    save_csv(pd.DataFrame(val), TAB / "validation_downscale_A4.csv", S)
    save_csv(pd.DataFrame([{"year": k, "nguon": v} for k, v in src_year.items()]), TAB / "panel_year_source.csv", S)


if __name__ == "__main__":
    main()
