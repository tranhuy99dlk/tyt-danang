# -*- coding: utf-8 -*-
"""06 - KPI tiep can tram y te (nam co so = BASE_YEAR, hien la 2024).

KPI chinh: ty le dan song trong ban kinh 3 km (duong chim bay) toi tram gan nhat.
Do nhay: 2 km, 5 km; theo mang duong (neu co 05_network); bo cac tram uoc luong (A5).
Dau ra: outputs/tables/kpi_*.csv, data/interim/grid_access.parquet
"""
import json

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

from common import (INTERIM, TAB, BASE_YEAR, THRESH_KM, MAIN_KM, WHITE_KM, get_logger, save_csv,
                    register, wmedian)

S = "06_kpi"
log = get_logger(S)
P = f"pop_{BASE_YEAR}"
P60 = f"pop60_{BASE_YEAR}"


def mountain_flags(grid, comm):
    """Xa mien nui THEO DIA HINH (gia dinh A7): xa (khong phai phuong) co do doc trung binh
    theo dan > 5 do HOAC do cao trung binh theo dan > 50 m. Phan loai chinh thuc (Uy ban Dan toc)
    can doi chieu rieng - xem bao cao."""
    t = grid.groupby("ma_tram").apply(lambda d: pd.Series({
        "doc_tb_theo_dan": np.average(d.slope_deg.fillna(0), weights=d[P] + 1e-9),
        "cao_tb_theo_dan": np.average(d.elev_m.fillna(0), weights=d[P] + 1e-9),
        "cao_tb_dien_tich": d.elev_m.mean()}))
    t = t.join(comm.set_index("ma_tram")[["loai_dv"]])
    t["mien_nui_dia_hinh"] = (t.loai_dv == "Xã") & ((t.doc_tb_theo_dan > 5) | (t.cao_tb_theo_dan > 50))
    return t


def access_table(df, by, dist_col, w=P):
    rows = []
    for k, d in (df.groupby(by) if by else [("Toan TP", df)]):
        tot = d[w].sum()
        r = {"nhom": k, "dan": round(tot)}
        for km in THRESH_KM:
            r[f"ty_le_trong_{km:g}km_pct"] = round(100 * d.loc[d[dist_col] <= km * 1000, w].sum() / tot, 2) if tot else np.nan
        r["kc_trung_vi_m"] = round(wmedian(d[dist_col], d[w]))
        r["kc_p90_m"] = round(_wq(d[dist_col], d[w], 0.9))
        r["dan_tren_5km"] = round(d.loc[d[dist_col] > WHITE_KM * 1000, w].sum())
        rows.append(r)
    return pd.DataFrame(rows)


def _wq(v, w, q):
    v, w = np.asarray(v, float), np.asarray(w, float)
    o = np.argsort(v)
    cw = np.cumsum(w[o])
    return float(v[o][np.searchsorted(cw, q * cw[-1])]) if cw[-1] > 0 else np.nan


def main():
    grid = pd.read_parquet(INTERIM / "grid_100m.parquet")
    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    comm = pd.DataFrame([f["properties"] for f in json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]])
    tree = cKDTree(st[["x_m", "y_m"]].values)
    d, i = tree.query(grid[["x_m", "y_m"]].values)
    grid["dist_m"] = d
    grid["tram_gan_nhat"] = st.ma_tram.values[i]
    real = ~st.uoc_luong.values
    if real.sum() < len(st):
        d2, _ = cKDTree(st.loc[real, ["x_m", "y_m"]].values).query(grid[["x_m", "y_m"]].values)
        grid["dist_m_chi_osm"] = d2
    else:
        grid["dist_m_chi_osm"] = grid.dist_m
    netp = INTERIM / "grid_network.parquet"
    has_net = netp.exists()
    if has_net:
        net = pd.read_parquet(netp)
        grid = grid.merge(net, on="cell_id", how="left")
        grid["detour"] = grid.net_dist_m / grid.dist_m.clip(lower=50)

    mf = mountain_flags(grid, comm)
    grid = grid.join(mf[["mien_nui_dia_hinh"]], on="ma_tram")
    grid = grid.join(comm.set_index("ma_tram")[["loai_dv", "quan_huyen", "ten_day_du"]], on="ma_tram")
    grid["nhom"] = np.where(grid.loai_dv == "Phường", "Phường",
                            np.where(grid.mien_nui_dia_hinh, "Xã miền núi (địa hình)", "Xã đồng bằng/ven đô"))

    tot = grid[P].sum()
    tot60 = grid[P60].sum()
    kpi = []
    def add(k, v, u, note=""):
        kpi.append(dict(chi_so=k, gia_tri=v, don_vi=u, ghi_chu=note))
    add(f"Tổng dân 56 xã/phường (WorldPop {BASE_YEAR}, ước tính)", round(tot), "người")
    add("Tổng dân 60+ (A3)", round(tot60), "người")
    add("Số trạm", len(st), "trạm", f"{int(st.nguon_toa_do.str.startswith('NHAP TAY').sum())} toạ độ nhập tay (A13), "
        f"{int(st.nguon_toa_do.str.startswith('OSM').sum())} toạ độ OSM, {int((~real).sum())} ước lượng (A5)")
    for km in THRESH_KM:
        add(f"Tỷ lệ dân trong {km:g} km (đường chim bay)", round(100 * grid.loc[grid.dist_m <= km * 1000, P].sum() / tot, 2), "%",
            "KPI chính" if km == MAIN_KM else "độ nhạy")
    add("Tỷ lệ dân trong 3 km - chỉ dùng trạm có toạ độ thực (nhập tay/OSM)", round(100 * grid.loc[grid.dist_m_chi_osm <= 3000, P].sum() / tot, 2), "%", "độ nhạy A5")
    add("Tỷ lệ dân 60+ trong 3 km", round(100 * grid.loc[grid.dist_m <= 3000, P60].sum() / tot60, 2), "%")
    add("Khoảng cách trung vị (theo dân) tới trạm gần nhất", round(wmedian(grid.dist_m, grid[P])), "m")
    add("Khoảng cách p90 (theo dân)", round(_wq(grid.dist_m, grid[P], .9)), "m")
    add("Dân cách trạm gần nhất > 5 km (vùng trắng)", round(grid.loc[grid.dist_m > 5000, P].sum()), "người")
    add("Dân 60+ cách trạm > 5 km", round(grid.loc[grid.dist_m > 5000, P60].sum()), "người")
    add("Dân bình quân mỗi trạm (tổng dân / số trạm)", round(tot / len(st)), "người/trạm")
    if has_net:
        ok = np.isfinite(grid.net_dist_m)
        add("Tỷ lệ dân trong 3 km theo mạng đường", round(100 * grid.loc[ok & (grid.net_dist_m <= 3000), P].sum() / tot, 2), "%", "A6")
        add("Tỷ lệ dân trong 5 km theo mạng đường", round(100 * grid.loc[ok & (grid.net_dist_m <= 5000), P].sum() / tot, 2), "%", "A6")
        add("Hệ số vòng đường trung vị (mạng/chim bay, theo dân, ô cách trạm > 500 m)",
            round(wmedian(grid.loc[grid.dist_m > 500, "detour"], grid.loc[grid.dist_m > 500, P]), 3), "lần", "A6")
        add("Thời gian trung vị tới trạm (xe máy, A6)", round(wmedian(grid.net_time_min, grid[P]), 1), "phút")
        add("Tỷ lệ dân tới trạm ≤ 15 phút (xe máy, A6)", round(100 * grid.loc[ok & (grid.net_time_min <= 15), P].sum() / tot, 2), "%")
    kpi = pd.DataFrame(kpi)
    save_csv(kpi, TAB / "kpi_overview.csv", S)

    # theo nhom / quan / xa
    g_grp = access_table(grid, "nhom", "dist_m")
    g_grp60 = access_table(grid, "nhom", "dist_m", w=P60).add_suffix("_60").rename(columns={"nhom_60": "nhom"})
    g_grp = g_grp.merge(g_grp60[["nhom", "ty_le_trong_3km_pct_60", "dan_60"]], on="nhom")
    if has_net:
        gn = grid[np.isfinite(grid.net_dist_m)]
        g_net = access_table(gn, "nhom", "net_dist_m")[["nhom", "ty_le_trong_3km_pct", "kc_trung_vi_m"]]
        g_net.columns = ["nhom", "ty_le_trong_3km_mang_duong_pct", "kc_mang_duong_trung_vi_m"]
        g_grp = g_grp.merge(g_net, on="nhom")
    g_grp["so_xa"] = g_grp.nhom.map(grid.groupby("nhom").ma_tram.nunique())
    save_csv(g_grp, TAB / "kpi_by_group.csv", S)
    save_csv(access_table(grid, "quan_huyen", "dist_m"), TAB / "kpi_by_district.csv", S)

    by_c = access_table(grid, "ma_tram", "dist_m").rename(columns={"nhom": "ma_tram"})
    c60 = access_table(grid, "ma_tram", "dist_m", w=P60)[["nhom", "ty_le_trong_3km_pct", "dan", "dan_tren_5km"]]
    c60.columns = ["ma_tram", "ty_le_60_trong_3km_pct", "dan_60", "dan_60_tren_5km"]
    by_c = by_c.merge(c60, on="ma_tram")
    if has_net:
        cn = access_table(grid[np.isfinite(grid.net_dist_m)], "ma_tram", "net_dist_m")[["nhom", "ty_le_trong_3km_pct", "kc_trung_vi_m"]]
        cn.columns = ["ma_tram", "ty_le_trong_3km_mang_duong_pct", "kc_mang_duong_trung_vi_m"]
        by_c = by_c.merge(cn, on="ma_tram", how="left")
        dt = grid[grid.dist_m > 500].groupby("ma_tram").apply(lambda d: wmedian(d.detour, d[P]))
        by_c["he_so_vong_trung_vi"] = by_c.ma_tram.map(dt).round(3)
    by_c = comm[["ma_tram", "ten_day_du", "quan_huyen", "loai_dv", "dien_tich_km2"]].merge(by_c, on="ma_tram")
    by_c = by_c.merge(mf.reset_index()[["ma_tram", "doc_tb_theo_dan", "cao_tb_theo_dan", "mien_nui_dia_hinh"]], on="ma_tram")
    by_c["mat_do_nguoi_km2"] = (by_c.dan / by_c.dien_tich_km2).round(0)
    save_csv(by_c, TAB / "kpi_by_commune.csv", S)

    # vung phuc vu tram (gan nhat theo duong chim bay)
    cat = grid.groupby("tram_gan_nhat").agg(dan_phuc_vu=(P, "sum"), dan60_phuc_vu=(P60, "sum"),
                                            kc_tb_m=("dist_m", "mean")).reset_index().rename(columns={"tram_gan_nhat": "ma_tram"})
    cat = st[["ma_tram", "ten_tram", "quan_huyen", "loai_dv", "uoc_luong"]].merge(cat, on="ma_tram", how="left").fillna({"dan_phuc_vu": 0})
    cat["dan_phuc_vu"] = cat.dan_phuc_vu.round()
    cat["dan60_phuc_vu"] = cat.dan60_phuc_vu.round()
    save_csv(cat, TAB / "station_catchment.csv", S)
    log.info("Dan phuc vu moi tram (gan nhat): trung vi %.0f, min %.0f, max %.0f, CV %.2f",
             cat.dan_phuc_vu.median(), cat.dan_phuc_vu.min(), cat.dan_phuc_vu.max(), cat.dan_phuc_vu.std() / cat.dan_phuc_vu.mean())

    gp = INTERIM / "grid_access.parquet"
    grid.to_parquet(gp, index=False)
    register(gp, S, rows=len(grid))
    for _, r in kpi.iterrows():
        log.info("KPI | %s = %s %s", r.chi_so, r.gia_tri, r.don_vi)


if __name__ == "__main__":
    main()
