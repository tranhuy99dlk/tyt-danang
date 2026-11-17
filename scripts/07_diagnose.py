# -*- coding: utf-8 -*-
"""07 - Chan doan vung trang (> 5 km) va vung xam (3-5 km); kich ban gop tram theo NQ 1251.

Quy tac chan doan (gia dinh A8, nguong cong khai, co the chinh):
  DIA HINH  : do doc TB vung > 8 do HOAC do cao TB > 150 m HOAC he so vong duong trung vi > 1,5
  PHAN TAN  : mat do trong vung < 10 nguoi/o 100 m (~1.200 nguoi/km2) HOAC tong dan vung < 300
  GOP TRAM  : o dang <= 3 km voi 56 tram nhung > 3 km khi chi con 47 tram (kich ban NQ 1251)
Dau ra: outputs/tables/white_zones.csv, scenario_nq1251.csv, figures/*.png
"""
import itertools
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
from scipy import ndimage
from scipy.spatial import cKDTree
from shapely.geometry import shape

from common import INTERIM, TAB, FIG, BASE_YEAR, get_logger, save_csv, register, norm_name, wmedian

S = "07_diagnose"
log = get_logger(S)
P = f"pop_{BASE_YEAR}"
P60 = f"pop60_{BASE_YEAR}"
# bang mau (dataviz reference palette): nhom = 3 slot dau; khoang cach = ramp xanh tuan tu
C_GRP = {"Phường": "#2a78d6", "Xã đồng bằng/ven đô": "#eb6834", "Xã miền núi (địa hình)": "#1baf7a"}
C_SEQ = ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d9d8d3"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": INK2,
                     "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2})

# NQ 1251/NQ-UBTVQH15 (hieu luc 01/01/2025): nhom phuong gop -> 1 don vi
NQ1251_GROUPS = [["Thanh Khê Đông", "Hòa Khê"], ["Xuân Hà", "Tam Thuận"], ["Thạc Gián", "Vĩnh Trung"],
                 ["Chính Gián", "Tân Chính"], ["An Hải Đông", "An Hải Tây"],
                 ["Phước Ninh", "Nam Dương", "Bình Hiên"], ["Bình Thuận", "Hòa Thuận Đông"],
                 ["Hải Châu I", "Hải Châu II"]]


def cov(grid, pts, km):
    d, _ = cKDTree(pts).query(grid[["x_m", "y_m"]].values)
    return d, grid.loc[d <= km * 1000, P].sum() / grid[P].sum()


def main():
    g = pd.read_parquet(INTERIM / "grid_access.parquet")
    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    feats = json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]
    has_net = "net_dist_m" in g.columns

    # ---- vung trang & vung xam: gom cum o lan can (gian no 2 o = 200 m) ----
    zones = []
    for label, lo, hi in (("trang_>5km", 5000, np.inf), ("xam_3-5km", 3000, 5000)):
        m = (g.dist_m > lo) & (g.dist_m <= hi)
        if not m.any():
            continue
        R = np.zeros((g.row.max() + 3, g.col.max() + 3), bool)
        R[g.row[m] + 1, g.col[m] + 1] = True
        lab, n = ndimage.label(ndimage.binary_dilation(R, iterations=2), structure=np.ones((3, 3)))
        zid = lab[g.row[m] + 1, g.col[m] + 1]
        sub = g[m].assign(zone=zid)
        for z, d in sub.groupby("zone"):
            dt = wmedian(d.detour, d[P]) if has_net else np.nan
            dens = d[P].sum() / len(d)
            topo = (np.average(d.slope_deg.fillna(0), weights=d[P] + 1e-9) > 8) or \
                   (np.average(d.elev_m.fillna(0), weights=d[P] + 1e-9) > 150) or (dt == dt and dt > 1.5)
            sparse = dens < 10 or d[P].sum() < 300
            cause = " + ".join([c for c, f in (("Địa hình", topo), ("Dân cư phân tán", sparse)) if f]) or "Khác (xem tay)"
            zones.append(dict(loai_vung=label, ma_vung=f"{label[:5]}{z:03d}", so_o=len(d), dan=round(d[P].sum()),
                              dan_60=round(d[P60].sum()), dan_tb_moi_o=round(dens, 1),
                              kc_tb_toi_tram_m=round(np.average(d.dist_m, weights=d[P] + 1e-9)),
                              kc_mang_duong_tb_m=round(np.average(d.net_dist_m[np.isfinite(d.net_dist_m)], weights=d[P][np.isfinite(d.net_dist_m)] + 1e-9)) if has_net and np.isfinite(d.net_dist_m).any() else np.nan,
                              he_so_vong_trung_vi=round(dt, 2) if dt == dt else np.nan,
                              do_cao_tb_m=round(np.average(d.elev_m.fillna(0), weights=d[P] + 1e-9)),
                              do_doc_tb_deg=round(np.average(d.slope_deg.fillna(0), weights=d[P] + 1e-9), 1),
                              xa=", ".join(sorted(d.ten_day_du.unique())), tram_gan_nhat=", ".join(sorted(d.tram_gan_nhat.unique())),
                              nguyen_nhan=cause, lon=d.lon.mean(), lat=d.lat.mean()))
    zt = pd.DataFrame(zones).sort_values(["loai_vung", "dan"], ascending=[False, False])
    save_csv(zt, TAB / "white_zones.csv", S)
    if len(zt):
        for lv, d in zt.groupby("loai_vung"):
            log.info("%s: %d vung, %d nguoi; theo nguyen nhan: %s", lv, len(d), d.dan.sum(),
                     d.groupby("nguyen_nhan").dan.sum().to_dict())

    # ---- kich ban gop tram NQ 1251 (56 -> 47) ----
    st["k"] = st.ma_tram.map(dict(zip(st.ma_tram, st.ten_tram))).str.replace(r"^Trạm Y tế (phường|xã) ", "", regex=True).map(norm_name)
    k2id = dict(zip(st.k, st.ma_tram))
    groups = [[k2id[norm_name(n)] for n in grp] for grp in NQ1251_GROUPS]
    fixed = [s for s in st.ma_tram if not any(s in grp for grp in groups)]
    log.info("Kich ban NQ 1251: %d tram co dinh + %d nhom gop -> %d tram", len(fixed), len(groups), len(fixed) + len(groups))
    xy = st.set_index("ma_tram")[["x_m", "y_m"]]
    base_d = g.dist_m.values
    res = []
    for combo in itertools.product(*groups):
        keep = fixed + list(combo)
        d, _ = cKDTree(xy.loc[keep].values).query(g[["x_m", "y_m"]].values)
        w = g[P].values
        res.append(dict(giu_lai=";".join(combo),
                        ty_le_1km=100 * w[d <= 1000].sum() / w.sum(), ty_le_2km=100 * w[d <= 2000].sum() / w.sum(),
                        ty_le_3km=100 * w[d <= 3000].sum() / w.sum(),
                        kc_trung_vi_m=wmedian(d, w), dan_tang_kc=w[d > base_d + 1].sum(),
                        dan_mat_3km=w[(base_d <= 3000) & (d > 3000)].sum(),
                        dan_mat_1km=w[(base_d <= 1000) & (d > 1000)].sum()))
    rs = pd.DataFrame(res)
    w = g[P].values
    base = dict(kich_ban="56 trạm (hiện trạng)", ty_le_1km=100 * w[base_d <= 1000].sum() / w.sum(),
                ty_le_2km=100 * w[base_d <= 2000].sum() / w.sum(), ty_le_3km=100 * w[base_d <= 3000].sum() / w.sum(),
                kc_trung_vi_m=wmedian(base_d, w), dan_mat_3km=0, dan_mat_1km=0, dan_tang_kc=0, giu_lai="")
    best = rs.sort_values(["ty_le_1km", "kc_trung_vi_m"], ascending=[False, True]).iloc[0].to_dict()
    worst = rs.sort_values(["ty_le_1km", "kc_trung_vi_m"], ascending=[True, False]).iloc[0].to_dict()
    best["kich_ban"], worst["kich_ban"] = "47 trạm - giữ trạm tốt nhất mỗi nhóm", "47 trạm - giữ trạm kém nhất mỗi nhóm"
    sc = pd.DataFrame([base, best, worst])
    sc["so_to_hop_da_xet"] = [1, len(rs), len(rs)]
    sc = sc[["kich_ban", "giu_lai", "ty_le_1km", "ty_le_2km", "ty_le_3km", "kc_trung_vi_m", "dan_mat_1km", "dan_mat_3km", "dan_tang_kc", "so_to_hop_da_xet"]].round(2)
    save_csv(sc, TAB / "scenario_nq1251.csv", S)
    log.info("NQ1251: ty le 3 km hien trang %.2f%% -> %.2f..%.2f%%; 1 km %.2f%% -> %.2f..%.2f%%",
             base["ty_le_3km"], worst["ty_le_3km"], best["ty_le_3km"], base["ty_le_1km"], worst["ty_le_1km"], best["ty_le_1km"])

    # ---- HINH ----
    polys = [shape(f["geometry"]) for f in feats]

    def draw_bounds(ax, lw=0.4):
        for p in polys:
            for part in (p.geoms if p.geom_type == "MultiPolygon" else [p]):
                x, y = part.exterior.xy
                ax.plot(x, y, color=INK2, lw=lw, alpha=.7)

    def draw_st(ax):
        r = st[~st.uoc_luong]
        e = st[st.uoc_luong]
        ax.scatter(r.lon, r.lat, s=22, c=INK, edgecolor="white", linewidth=1, zorder=5)
        if len(e):
            ax.scatter(e.lon, e.lat, s=26, facecolor="white", edgecolor=INK, linewidth=1.2, zorder=5)

    bins = [0, 1000, 2000, 3000, 5000, np.inf]
    labs = ["≤ 1 km", "1–2 km", "2–3 km", "3–5 km", "> 5 km"]
    cls = np.digitize(g.dist_m, bins[1:-1])
    fig, ax = plt.subplots(figsize=(7.2, 6.2), dpi=200)
    ax.scatter(g.lon, g.lat, c=[C_SEQ[c] for c in cls], s=0.6, marker="s", linewidths=0)
    draw_bounds(ax)
    draw_st(ax)
    hs = [Patch(color=C_SEQ[i], label=f"{labs[i]}: {100 * g.loc[cls == i, P].sum() / g[P].sum():.1f}% dân") for i in range(5)]
    hs += [Line2D([], [], marker="o", ls="", mfc=INK, mec="white", label="Trạm (toạ độ nhập tay/OSM)")]
    if st.uoc_luong.any():
        hs += [Line2D([], [], marker="o", ls="", mfc="white", mec=INK, label="Trạm (vị trí ước lượng A5)")]
    ax.legend(handles=hs, loc="lower left", frameon=False, fontsize=7.5, title="Khoảng cách tới trạm gần nhất", title_fontsize=8)
    ax.set_title(f"Khoảng cách đường chim bay tới trạm y tế gần nhất, dân số {BASE_YEAR}", loc="left", color=INK, fontsize=10)
    ax.set_aspect(1 / np.cos(np.deg2rad(16.05)))
    ax.set_xticks([]); ax.set_yticks([])
    for s_ in ax.spines.values():
        s_.set_visible(False)
    fig.text(0.01, 0.01, "Nguồn: WorldPop R2025A (CC BY 4.0); OpenStreetMap (ODbL). File: outputs/tables/kpi_overview.csv", fontsize=6, color=INK2)
    fig.tight_layout()
    f1 = FIG / "h1_ban_do_khoang_cach.png"
    fig.savefig(f1); plt.close(fig); register(f1, S)

    # hinh 2: vung trang/xam + do cao
    fig, ax = plt.subplots(figsize=(7.2, 6.2), dpi=200)
    far = g.dist_m > 3000
    ax.scatter(g.lon[~far], g.lat[~far], c="#e9e8e4", s=0.5, marker="s", linewidths=0)
    sc_ = ax.scatter(g.lon[far], g.lat[far], c=g.elev_m[far], cmap="Oranges", vmin=0, vmax=max(300, np.nanpercentile(g.elev_m[far], 95) if far.any() else 300), s=1.2, marker="s", linewidths=0)
    draw_bounds(ax)
    draw_st(ax)
    if len(zt):
        for _, z in zt[zt.loai_vung.str.startswith("trang")].head(6).iterrows():
            ax.annotate(f"{z.ma_vung}: {z.dan:,} người\n{z.nguyen_nhan}".replace(",", "."), (z.lon, z.lat), xytext=(8, 8),
                        textcoords="offset points", fontsize=6.5, color=INK, arrowprops=dict(arrowstyle="-", color=INK2, lw=.5))
    cb = fig.colorbar(sc_, ax=ax, shrink=.5, pad=.01)
    cb.set_label("Độ cao (m) - chỉ tô ô cách trạm > 3 km", fontsize=7)
    ax.set_title("Vùng xám (3–5 km) và vùng trắng (> 5 km) trên nền độ cao", loc="left", color=INK, fontsize=10)
    ax.set_aspect(1 / np.cos(np.deg2rad(16.05)))
    ax.set_xticks([]); ax.set_yticks([])
    for s_ in ax.spines.values():
        s_.set_visible(False)
    fig.text(0.01, 0.01, "Nguồn: WorldPop R2025A; OSM; AWS Terrain Tiles. File: outputs/tables/white_zones.csv", fontsize=6, color=INK2)
    fig.tight_layout()
    f2 = FIG / "h2_vung_trang_dia_hinh.png"
    fig.savefig(f2); plt.close(fig); register(f2, S)

    # hinh 3: ty le trong 3 km theo xa (chi xa < 100%)
    kc = pd.read_csv(TAB / "kpi_by_commune.csv", encoding="utf-8-sig")
    kc["nhom"] = np.where(kc.loai_dv == "Phường", "Phường", np.where(kc.mien_nui_dia_hinh, "Xã miền núi (địa hình)", "Xã đồng bằng/ven đô"))
    sub = kc[kc.ty_le_trong_3km_pct < 99.95].sort_values("ty_le_trong_3km_pct")
    if len(sub):
        fig, ax = plt.subplots(figsize=(7.2, 0.32 * len(sub) + 1.2), dpi=200)
        y = np.arange(len(sub))
        ax.barh(y, sub.ty_le_trong_3km_pct, color=[C_GRP[n] for n in sub.nhom], height=.62)
        for yy, v, dd in zip(y, sub.ty_le_trong_3km_pct, sub.dan_tren_5km):
            ax.text(v + 1, yy, f"{v:.1f}%" + (f"  ({dd:,} người > 5 km)".replace(",", ".") if dd > 0 else ""), va="center", fontsize=7, color=INK)
        ax.set_yticks(y, sub.ten_day_du, fontsize=7.5)
        ax.set_xlim(0, 115)
        ax.set_xlabel("% dân trong 3 km tới trạm gần nhất")
        ax.grid(axis="x", color=GRID, lw=.5)
        ax.set_axisbelow(True)
        for s_ in ("top", "right"):
            ax.spines[s_].set_visible(False)
        ax.legend(handles=[Patch(color=c, label=k) for k, c in C_GRP.items()], frameon=False, fontsize=7, loc="lower right")
        ax.set_title(f"Các xã/phường chưa đạt 100% dân trong 3 km ({len(sub)}/56)", loc="left", color=INK, fontsize=10)
        fig.text(0.01, 0.005, "File: outputs/tables/kpi_by_commune.csv", fontsize=6, color=INK2)
        fig.tight_layout()
        f3 = FIG / "h3_ty_le_3km_theo_xa.png"
        fig.savefig(f3); plt.close(fig); register(f3, S)

    # hinh 4: mat do dan vs khoang cach trung vi (log x)
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=200)
    for n_, c_ in C_GRP.items():
        d = kc[kc.nhom == n_]
        ax.scatter(d.mat_do_nguoi_km2, d.kc_trung_vi_m, s=28, color=c_, edgecolor="white", linewidth=1, label=f"{n_} ({len(d)})")
    for _, r in kc.nlargest(6, "kc_trung_vi_m").iterrows():
        ax.annotate(r.ten_day_du.replace("Phường ", "P. ").replace("Xã ", "X. "), (r.mat_do_nguoi_km2, r.kc_trung_vi_m), xytext=(4, 3), textcoords="offset points", fontsize=6.5, color=INK)
    ax.set_xscale("log")
    ax.set_xlabel("Mật độ dân (người/km², thang log)")
    ax.set_ylabel("Khoảng cách trung vị tới trạm (m)")
    ax.grid(color=GRID, lw=.5)
    for s_ in ("top", "right"):
        ax.spines[s_].set_visible(False)
    ax.legend(frameon=False, fontsize=7)
    ax.set_title("Xã càng thưa dân, người dân càng ở xa trạm", loc="left", color=INK, fontsize=10)
    fig.text(0.01, 0.01, "File: outputs/tables/kpi_by_commune.csv", fontsize=6, color=INK2)
    fig.tight_layout()
    f4 = FIG / "h4_mat_do_vs_khoang_cach.png"
    fig.savefig(f4); plt.close(fig); register(f4, S)


if __name__ == "__main__":
    main()
