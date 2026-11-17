# -*- coding: utf-8 -*-
"""09 - De xuat K vi tri tram/diem tram moi bang thuat toan THAM LAM BAO PHU TOI DA (MCLP).

Bai toan: chon K diem tu tap ung vien de toi da hoa so dan DUOC PHU MOI trong ban kinh 3 km
(chi tinh dan hien chua duoc phu boi 56 tram). Tham lam: moi buoc chon ung vien co muc tang
lon nhat, cap nhat tap da phu, lap lai. Dam bao toan hoc: dat >= (1 - 1/e) ~ 63% toi uu
(Nemhauser, Wolsey & Fisher 1978) - trong thuc te thuong sat toi uu hon nhieu.
Ung vien (gia dinh A10): moi khoi 300 m x 300 m co dan -> o 100 m dong dan nhat trong khoi.
Muc tieu chinh: dan nam co so (BASE_YEAR); do nhay: dan 60+ nam co so. Khong dung so du bao cua buoc 08.
Dau ra: greedy_sites.csv, greedy_curve.csv, greedy_before_after.csv, h7.
"""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse
import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse import coo_matrix, csr_matrix
from scipy.sparse.csgraph import dijkstra
from scipy.spatial import cKDTree
from shapely.geometry import shape

from common import INTERIM, TAB, FIG, BASE_YEAR, MAIN_KM, CRS_M, get_logger, save_csv, register, wmedian

S = "09_greedy"
log = get_logger(S)
P, P60 = f"pop_{BASE_YEAR}", f"pop60_{BASE_YEAR}"
OBJ, OBJ60 = f"Dân {BASE_YEAR}", f"Dân 60+ {BASE_YEAR}"
KMAX = 10
R = MAIN_KM * 1000
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d9d8d3"
C1, C2, C3 = "#2a78d6", "#eb6834", "#1baf7a"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9})


def candidates(g):
    b = g.assign(br=g.row // 3, bc=g.col // 3)
    idx = b.groupby(["br", "bc"])[P].idxmax()
    c = g.loc[idx.values, ["cell_id", "x_m", "y_m", "lon", "lat", "ma_tram", "ten_day_du"]].reset_index(drop=True)
    return c


def greedy(g, cand, w, covered0, kmax=KMAX):
    """Tham lam tren ma tran phu thua (ung vien x o chua phu). Tra ve [(chi so ung vien, muc tang)]."""
    unc = np.where(~covered0)[0]
    if len(unc) == 0:
        return []
    tree = cKDTree(g[["x_m", "y_m"]].values[unc])
    nb = tree.query_ball_point(cand[["x_m", "y_m"]].values, R)
    rows = np.repeat(np.arange(len(nb)), [len(n) for n in nb])
    cols = np.concatenate([np.asarray(n, int) for n in nb]) if len(rows) else np.array([], int)
    M = csr_matrix((np.ones(len(rows)), (rows, cols)), shape=(len(nb), len(unc)))
    wu = w[unc].astype(float).copy()
    picks = []
    for _ in range(kmax):
        gains = M @ wu
        j = int(gains.argmax())
        if gains[j] <= 0:
            break
        picks.append((j, float(gains[j])))
        wu[M[j].indices] = 0.0  # da phu -> khong tinh lai
    return picks


def main():
    g = pd.read_parquet(INTERIM / "grid_access.parquet")
    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig")
    cand = candidates(g)
    covered0 = (g.dist_m <= R).values
    log.info("Ung vien: %d; o chua phu 3 km: %d (%.0f nguoi)", len(cand), (~covered0).sum(), g.loc[~covered0, P].sum())

    out_sites, curves = [], []
    for obj, col in ((OBJ, P), (OBJ60, P60)):
        if col not in g:
            continue
        w = g[col].fillna(0).values
        picks = greedy(g, cand, w, covered0)
        tot = w.sum()
        base_cov = w[covered0].sum()
        cum = base_cov
        curves.append(dict(muc_tieu=obj, K=0, ty_le_3km_pct=100 * base_cov / tot, dan_moi_duoc_phu=0))
        pts = st[["x_m", "y_m"]].values
        for k, (j, gain) in enumerate(picks, 1):
            cum += gain
            c = cand.iloc[j]
            pts = np.vstack([pts, [c.x_m, c.y_m]])
            curves.append(dict(muc_tieu=obj, K=k, ty_le_3km_pct=100 * cum / tot, dan_moi_duoc_phu=gain))
            if obj == OBJ:
                out_sites.append(dict(thu_tu=k, cell_id=c.cell_id, lon=round(c.lon, 5), lat=round(c.lat, 5), xa=c.ten_day_du,
                                      dan_moi_duoc_phu=round(gain), ty_le_3km_sau_pct=round(100 * cum / tot, 2)))
    curve = pd.DataFrame(curves).round(3)
    save_csv(curve, TAB / "greedy_curve.csv", S)
    sites = pd.DataFrame(out_sites)
    tr = Transformer.from_crs("EPSG:4326", CRS_M, always_xy=True)
    if len(sites):
        sites["x_m"], sites["y_m"] = tr.transform(sites.lon.values, sites.lat.values)
        # them 60+ duoc phu moi va kc toi tram hien huu gan nhat
        dd, ii = cKDTree(st[["x_m", "y_m"]].values).query(sites[["x_m", "y_m"]].values)
        sites["kc_toi_tram_hien_huu_m"] = dd.round(0)
        sites["tram_hien_huu_gan_nhat"] = st.ten_tram.values[ii]
    save_csv(sites, TAB / "greedy_sites.csv", S)

    # ---- truoc / sau cho K = 1, 3, 5, 10 (muc tieu dan nam co so) ----
    ba = []
    gnp = INTERIM / "road_graph.npz"
    G = None
    if gnp.exists() and "net_dist_m" in g:
        z = np.load(gnp)
        nk = len(z["x"])
        ntree = cKDTree(np.c_[z["x"], z["y"]])
        dc, nc = ntree.query(g[["x_m", "y_m"]].values)
    for K in (0, 1, 3, 5, 10):
        if K > len(sites):
            continue
        pts = np.vstack([st[["x_m", "y_m"]].values] + ([sites[["x_m", "y_m"]].values[:K]] if K else []))
        d, _ = cKDTree(pts).query(g[["x_m", "y_m"]].values)
        w, w60 = g[P].values, g[P60].values
        r = dict(K=K, so_tram=len(pts), ty_le_2km=100 * w[d <= 2000].sum() / w.sum(), ty_le_3km=100 * w[d <= 3000].sum() / w.sum(),
                 ty_le_5km=100 * w[d <= 5000].sum() / w.sum(), ty_le60_3km=100 * w60[d <= 3000].sum() / w60.sum(),
                 dan_tren_5km=w[d > 5000].sum(), dan60_tren_5km=w60[d > 5000].sum(), kc_trung_vi_m=wmedian(d, w))
        if gnp.exists() and "net_dist_m" in g:
            ds, ns = ntree.query(pts)
            Sn = len(pts)
            rows = np.r_[z["i"], z["j"], nk + np.arange(Sn), ns]
            cols = np.r_[z["j"], z["i"], ns, nk + np.arange(Sn)]
            vals = np.r_[z["l"], z["l"], ds + 1e-6, ds + 1e-6]
            M = coo_matrix((vals, (rows, cols)), shape=(nk + Sn, nk + Sn)).tocsr()
            dn = dijkstra(M, directed=False, indices=nk + np.arange(Sn), min_only=True)[:nk][nc] + dc
            ok = np.isfinite(dn)
            r["ty_le_3km_mang_duong"] = 100 * w[ok & (dn <= 3000)].sum() / w.sum()
            r["ty_le_5km_mang_duong"] = 100 * w[ok & (dn <= 5000)].sum() / w.sum()
        ba.append(r)
    ba = pd.DataFrame(ba).round(2)
    save_csv(ba, TAB / "greedy_before_after.csv", S)
    for _, r in ba.iterrows():
        log.info("K=%d: 3 km %.2f%%, 5 km %.2f%%, dan >5 km %.0f", r.K, r.ty_le_3km, r.ty_le_5km, r.dan_tren_5km)

    # ---- hinh 7 ----
    feats = json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]
    fig, (ax, bx) = plt.subplots(1, 2, figsize=(7.2, 4.0), dpi=200, gridspec_kw={"width_ratios": [1.5, 1]})
    far = g.dist_m > 3000
    ax.scatter(g.lon[~far], g.lat[~far], c="#e9e8e4", s=.4, marker="s", linewidths=0)
    ax.scatter(g.lon[far], g.lat[far], c=C2, s=.8, marker="s", linewidths=0, label="Ô dân cách trạm > 3 km")
    for f in feats:
        p = shape(f["geometry"])
        for part in (p.geoms if p.geom_type == "MultiPolygon" else [p]):
            x, y = part.exterior.xy
            ax.plot(x, y, color=INK2, lw=.3, alpha=.6)
    ax.scatter(st.lon, st.lat, s=12, c=INK, edgecolor="white", linewidth=.8, zorder=5, label="56 trạm hiện có")
    K5 = sites.head(5)
    ax.scatter(K5.lon, K5.lat, s=60, marker="*", c=C1, edgecolor="white", linewidth=.8, zorder=6, label="Đề xuất (K=5)")
    for _, s_ in K5.iterrows():
        circ = Ellipse((s_.lon, s_.lat), 2 * R / (111320 * np.cos(np.deg2rad(16.05))), 2 * R / 111320, fill=False, color=C1, lw=.6, ls="--")
        ax.add_patch(circ)
        ax.annotate(str(int(s_.thu_tu)), (s_.lon, s_.lat), xytext=(4, 4), textcoords="offset points", fontsize=7, color=INK, weight="bold")
    ax.set_aspect(1 / np.cos(np.deg2rad(16.05)))
    ax.set_xticks([]); ax.set_yticks([])
    for s_ in ax.spines.values():
        s_.set_visible(False)
    ax.legend(frameon=False, fontsize=6.5, loc="lower left", markerscale=1.5)
    ax.set_title("Vị trí đề xuất (tham lam, R = 3 km)", loc="left", fontsize=9, color=INK)
    for obj, c_ in ((OBJ, C1), (OBJ60, C2)):
        d = curve[curve.muc_tieu == obj]
        if len(d):
            bx.plot(d.K, d.ty_le_3km_pct, marker="o", ms=3.5, lw=2, color=c_, label=obj)
    bx.set_xlabel("Số vị trí mới K")
    bx.set_ylabel("% đối tượng trong 3 km")
    bx.grid(color=GRID, lw=.5)
    for s_ in ("top", "right"):
        bx.spines[s_].set_visible(False)
    bx.legend(frameon=False, fontsize=6.5)
    bx.set_title("Độ phủ tăng thêm theo K", loc="left", fontsize=9, color=INK)
    fig.text(0.01, 0.005, "File: outputs/tables/greedy_sites.csv, greedy_curve.csv", fontsize=6, color=INK2)
    fig.tight_layout()
    f7 = FIG / "h7_de_xuat_vi_tri.png"
    fig.savefig(f7); plt.close(fig); register(f7, S)


if __name__ == "__main__":
    main()
