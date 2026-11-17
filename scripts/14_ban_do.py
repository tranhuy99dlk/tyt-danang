# -*- coding: utf-8 -*-
"""14 - Ban do: (a) Da Nang cu truoc 01/01/2025 - pham vi toa do, 56 xa/phuong, 56 tram;
(b) so sanh TRUOC va SAU khi them cac diem de xuat.

Khong tinh KPI moi: doc ranh gioi (02), tram (04), luoi tiep can (06), diem de xuat (09).
So diem de xuat ve tren ban do "sau" = K_MAP (mac dinh 5; doi bang bien moi truong K_MAP=3 ...).
Dau ra:
  outputs/tables/pham_vi_da_nang_cu.csv        pham vi toa do, dien tich, so don vi cua Da Nang cu
  outputs/tables/xa_phuong_56_toa_do.csv       1 dong / xa: so tren ban do, tam, khung toa do, dien tich, toa do tram
  outputs/tables/ban_do_truoc_sau_de_xuat.csv  dan theo lop khoang cach, truoc va sau khi them K diem
  outputs/figures/h8_ban_do_hanh_chinh_truoc_2025.png
  outputs/figures/h9_truoc_sau_de_xuat.png
Chay sau 09_greedy.py:  cd scripts && python 14_ban_do.py
"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection
from matplotlib.lines import Line2D
from matplotlib.patches import Ellipse, Patch, Rectangle
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from shapely.geometry import shape
from shapely.ops import unary_union

from common import INTERIM, TAB, FIG, BASE_YEAR, MAIN_KM, WHITE_KM, DISTRICTS, get_logger, save_csv, register

S = "14_ban_do"
log = get_logger(S)
P = f"pop_{BASE_YEAR}"
K_MAP = int(os.environ.get("K_MAP", "5"))
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e1e0d9", "#fcfcfb"
C_SEQ = ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]          # lop khoang cach: 1 mau, nhat -> dam
C_NEW = "#eb6834"                                                        # diem de xuat
# 7 quan/huyen: bang mau phan loai theo thu tu co dinh (da kiem dinh mu mau; nhan chu luon di kem)
C_DIST = dict(zip(DISTRICTS, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"]))
KLAT = np.cos(np.deg2rad(16.05))
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": INK2,
                     "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2})


def vn(v, nd=1):
    """So kieu Viet: dau phay thap phan, dau cham nghin."""
    s = f"{v:,.{nd}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def rings(geom):
    return [np.asarray(p.exterior.coords) for p in (geom.geoms if geom.geom_type == "MultiPolygon" else [geom])]


def scale_bar(ax, x0, y0, km=10, dy=0.006):
    dx = km * 1000 / (111320 * KLAT)
    ax.plot([x0, x0 + dx], [y0, y0], color=INK, lw=2, solid_capstyle="butt")
    ax.text(x0 + dx / 2, y0 + dy, f"{km} km", ha="center", va="bottom", fontsize=7, color=INK, zorder=9,
            bbox=dict(boxstyle="round,pad=0.15", fc=SURF, ec="none", alpha=.9))


def main():
    feats = json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]
    st = pd.read_csv(INTERIM / "stations_56.csv", encoding="utf-8-sig").set_index("ma_tram")
    comm = []
    for f in feats:
        g, pr = shape(f["geometry"]), f["properties"]
        c = g.centroid if g.contains(g.centroid) else g.representative_point()
        w, s_, e, n = g.bounds
        comm.append(dict(so_tren_ban_do=int(pr["ma_tram"][3:]), ma_tram=pr["ma_tram"], ten_day_du=pr["ten_day_du"],
                         quan_huyen=pr["quan_huyen"], loai_dv=pr["loai_dv"], dien_tich_km2=pr["dien_tich_km2"],
                         lon_tam=round(c.x, 5), lat_tam=round(c.y, 5), lon_min=round(w, 5), lon_max=round(e, 5),
                         lat_min=round(s_, 5), lat_max=round(n, 5), nguon_ranh_gioi=pr["nguon_ranh_gioi"], geom=g))
    cm = pd.DataFrame(comm).sort_values("so_tren_ban_do").reset_index(drop=True)
    cm = cm.join(st[["lat", "lon", "trong_ranh_gioi_xa", "kc_ngoai_ranh_gioi_m"]].rename(
        columns={"lat": "lat_tram", "lon": "lon_tram", "trong_ranh_gioi_xa": "tram_trong_ranh_gioi_xa",
                 "kc_ngoai_ranh_gioi_m": "tram_cach_ranh_gioi_m"}), on="ma_tram")
    save_csv(cm.drop(columns="geom"), TAB / "xa_phuong_56_toa_do.csv", S, note="56 xa/phuong truoc 01/01/2025: so tren ban do h8, tam, khung toa do")

    # ---- pham vi toa do cua Da Nang cu (dat lien, 56 xa/phuong) ----
    U = unary_union(list(cm.geom))
    W, S_, E, N = U.bounds
    n_p, n_x = int((cm.loai_dv == "Phường").sum()), int((cm.loai_dv == "Xã").sum())
    src = "; ".join(sorted(cm.nguon_ranh_gioi.unique()))
    pv = [("Kinh độ cực tây", round(W, 4), "độ Đông", cm.loc[cm.lon_min.idxmin(), "ten_day_du"]),
          ("Kinh độ cực đông", round(E, 4), "độ Đông", cm.loc[cm.lon_max.idxmax(), "ten_day_du"]),
          ("Vĩ độ cực nam", round(S_, 4), "độ Bắc", cm.loc[cm.lat_min.idxmin(), "ten_day_du"]),
          ("Vĩ độ cực bắc", round(N, 4), "độ Bắc", cm.loc[cm.lat_max.idxmax(), "ten_day_du"]),
          ("Kinh độ tâm hình học", round(U.centroid.x, 4), "độ Đông", ""),
          ("Vĩ độ tâm hình học", round(U.centroid.y, 4), "độ Bắc", ""),
          ("Bề ngang đông–tây", round((E - W) * 111.32 * KLAT, 1), "km", "xấp xỉ, tại vĩ độ 16,05"),
          ("Bề dọc bắc–nam", round((N - S_) * 110.6, 1), "km", "xấp xỉ"),
          ("Tổng diện tích 56 xã/phường", round(cm.dien_tich_km2.sum(), 1), "km²", "tính trên UTM 49N từ " + src),
          ("Số quận/huyện", cm.quan_huyen.nunique(), "đơn vị", "; ".join(f"{d} ({(cm.quan_huyen == d).sum()})" for d in DISTRICTS)),
          ("Số xã/phường", len(cm), "đơn vị", f"{n_p} phường, {n_x} xã – bản đồ hành chính trước 01/01/2025"),
          ("Số trạm y tế", len(st), "trạm", "mỗi xã/phường một trạm (A13)")]
    save_csv(pd.DataFrame(pv, columns=["chi_so", "gia_tri", "don_vi", "ghi_chu"]), TAB / "pham_vi_da_nang_cu.csv", S,
             note=f"pham vi 56 xa/phuong (dat lien) theo {src}")
    log.info("Pham vi Da Nang cu: kinh do %.4f-%.4f, vi do %.4f-%.4f; %.1f km2; %d phuong + %d xa", W, E, S_, N, cm.dien_tich_km2.sum(), n_p, n_x)

    # =================== HINH 8: ban do hanh chinh truoc 01/01/2025 ===================
    fig = plt.figure(figsize=(12.4, 7.0), dpi=200, facecolor=SURF)
    gs = fig.add_gridspec(2, 2, width_ratios=[1.75, 1], height_ratios=[1, 1.15], left=0.045, right=0.985, top=0.885, bottom=0.085, wspace=0.05, hspace=0.12)
    ax, zx, kx = fig.add_subplot(gs[:, 0]), fig.add_subplot(gs[0, 1]), fig.add_subplot(gs[1, 1])
    ZOOM = (108.168, 108.262, 16.022, 16.098)                    # loi do thi: Thanh Khe, Hai Chau va vung ke
    dist_geom = {d: unary_union(list(cm.loc[cm.quan_huyen == d, "geom"])) for d in DISTRICTS}

    def in_zoom(r):
        return ZOOM[0] <= r.lon_tam <= ZOOM[1] and ZOOM[2] <= r.lat_tam <= ZOOM[3]

    def draw(a, lw_c, main, fs, ms):
        for _, r in cm.iterrows():
            a.add_collection(PolyCollection(rings(r.geom), facecolors=C_DIST[r.quan_huyen], edgecolors="white", linewidths=lw_c))
        for d, gg in dist_geom.items():
            for rr in rings(gg):
                a.plot(rr[:, 0], rr[:, 1], color=INK, lw=0.9, alpha=.75)
        a.scatter(st.lon, st.lat, s=ms, c=INK, edgecolor="white", linewidth=0.8, zorder=6)
        for _, r in cm.iterrows():
            if not (main and in_zoom(r)):          # ban do chinh: xa trong khung phong to chi danh so o hinh phong to
                a.text(r.lon_tam, r.lat_tam, str(r.so_tren_ban_do), ha="center", va="center", fontsize=fs, weight="bold",
                       color=INK if r.quan_huyen == DISTRICTS[3] else "white", zorder=7, clip_on=True)
        a.set_aspect(1 / KLAT)
        a.set_facecolor(SURF)

    draw(ax, 0.5, True, 6.4, 13)
    ax.set_xlim(W - 0.02, E + 0.02); ax.set_ylim(S_ - 0.03, N + 0.02)
    ax.set_xticks(np.arange(107.85, 108.36, 0.1)); ax.set_yticks(np.arange(15.95, 16.21, 0.05))
    ax.xaxis.set_major_formatter(lambda v, _: vn(v, 2) + "°Đ"); ax.yaxis.set_major_formatter(lambda v, _: vn(v, 2) + "°B")
    ax.tick_params(labelsize=7, length=3)
    ax.grid(color=GRID, lw=.6, zorder=0); ax.set_axisbelow(True)
    for s in ax.spines.values():
        s.set_color(GRID)
    ax.add_patch(Rectangle((ZOOM[0], ZOOM[2]), ZOOM[1] - ZOOM[0], ZOOM[3] - ZOOM[2], fill=False, ec=INK, lw=1, ls=(0, (4, 2)), zorder=7))
    ax.text(ZOOM[0], ZOOM[2] - 0.004, "khung phóng to", ha="left", va="top", fontsize=6.5, color=INK, zorder=9, bbox=dict(boxstyle="round,pad=0.2", fc=SURF, ec="none", alpha=.9))
    scale_bar(ax, W + 0.005, S_ - 0.018)
    # tram nam ngoai xa cua minh tren 500 m: noi tram voi tam xa de nguoi doc thay
    for _, r in cm[cm.tram_cach_ranh_gioi_m > 500].iterrows():
        ax.plot([r.lon_tram, r.lon_tam], [r.lat_tram, r.lat_tam], color=INK, lw=0.8, ls=":", zorder=6)
        ax.annotate(f"Trạm {r.ten_day_du.replace('Xã ', '').replace('Phường ', '')}: ngoài ranh giới xã {vn(r.tram_cach_ranh_gioi_m / 1000, 1)} km",
                    (r.lon_tram, r.lat_tram), xytext=(-30, 26), textcoords="offset points", fontsize=6.5, color=INK, ha="right", zorder=9,
                    bbox=dict(boxstyle="round,pad=0.2", fc=SURF, ec="none", alpha=.9), arrowprops=dict(arrowstyle="-", color=INK, lw=0.7))
    # mau quan/huyen duoc chu giai o bang ben phai (o mau + ten + danh sach xa); tren ban do chi can chu giai ky hieu
    hs = [Line2D([], [], marker="o", ls="", mfc=INK, mec="white", ms=5, label="Trạm y tế xã/phường (56)"),
          Line2D([], [], color=INK, lw=0.9, label="Ranh giới quận/huyện"), Line2D([], [], color="white", lw=1.2, label="Ranh giới xã/phường (nét trắng)")]
    leg = ax.legend(handles=hs, loc="lower right", frameon=True, facecolor="#d9d8d3", edgecolor=GRID, fontsize=7, borderpad=0.7)
    leg.set_zorder(9)

    draw(zx, 0.7, False, 6.4, 7)
    zx.set_xlim(ZOOM[0], ZOOM[1]); zx.set_ylim(ZOOM[2], ZOOM[3])
    zx.set_xticks([]); zx.set_yticks([])
    for s in zx.spines.values():
        s.set_color(INK); s.set_linestyle((0, (4, 2)))
    zx.set_title("Lõi đô thị (phóng to)", loc="left", fontsize=8.5, color=INK)
    scale_bar(zx, ZOOM[0] + 0.004, ZOOM[2] + 0.004, km=2, dy=0.0015)

    kx.axis("off")
    kx.set_xlim(0, 1); kx.set_ylim(0, 1)
    kx.text(0, 1.0, "Quận/huyện (màu) và số trên bản đồ → xã/phường", fontsize=8.2, color=INK, va="top", weight="bold")
    y, x = 0.925, 0.0
    for d in DISTRICTS:
        sub = cm[cm.quan_huyen == d]
        need = 0.05 + 0.034 * int(np.ceil(len(sub) / 2))
        if y - need < -0.03:
            y, x = 0.925, 0.5
        kx.add_patch(Rectangle((x, y - 0.026), 0.022, 0.026, fc=C_DIST[d], ec="none", transform=kx.transAxes, clip_on=False))
        kx.text(x + 0.03, y - 0.013, f"{d} ({len(sub)})", fontsize=6.9, color=INK, va="center", weight="bold")
        y -= 0.045
        names = [f"{r.so_tren_ban_do} {r.ten_day_du.replace('Phường ', '').replace('Xã ', '')}" for _, r in sub.iterrows()]
        for i in range(0, len(names), 2):
            kx.text(x, y, "  ·  ".join(names[i:i + 2]), fontsize=6.0, color=INK2, va="center")
            y -= 0.034
        y -= 0.012
    fig.suptitle("Đà Nẵng trước 01/01/2025: 56 xã/phường, 7 quận/huyện và 56 trạm y tế", x=0.045, ha="left", fontsize=12.5, color=INK, weight="bold")
    fig.text(0.045, 0.917, f"Phạm vi đất liền: {vn(W, 4)}–{vn(E, 4)}° Đông, {vn(S_, 4)}–{vn(N, 4)}° Bắc · {vn(cm.dien_tich_km2.sum(), 1)} km² · {n_p} phường, {n_x} xã",
             fontsize=8.5, color=INK2)
    fig.text(0.045, 0.018, f"Ranh giới: {src} (chỉ dùng học thuật; có thể lệch so với ranh giới pháp lý). Trạm: toạ độ nhóm nhập tay (A13). "
             "Bản đồ chỉ gồm 56 xã/phường có trạm; huyện đảo Hoàng Sa không thuộc phạm vi.\n"
             "File: outputs/tables/xa_phuong_56_toa_do.csv, pham_vi_da_nang_cu.csv", fontsize=6.3, color=INK2, va="bottom", linespacing=1.5)
    f8 = FIG / "h8_ban_do_hanh_chinh_truoc_2025.png"
    fig.savefig(f8, facecolor=SURF); plt.close(fig); register(f8, S, note="ban do hanh chinh 56 xa/phuong truoc 01/01/2025")

    # =================== HINH 9: truoc / sau khi them K diem de xuat ===================
    g = pd.read_parquet(INTERIM / "grid_access.parquet", columns=["cell_id", "lon", "lat", "x_m", "y_m", P, "dist_m"])
    sp = TAB / "greedy_sites.csv"
    if not sp.exists():
        log.warning("Khong co greedy_sites.csv -> bo qua ban do truoc/sau")
        return
    sites = pd.read_csv(sp, encoding="utf-8-sig").sort_values("thu_tu").head(K_MAP)
    K = len(sites)
    d_new, _ = cKDTree(sites[["x_m", "y_m"]].values).query(g[["x_m", "y_m"]].values)
    g["dist_sau"] = np.minimum(g.dist_m.values, d_new)
    bins = [1000, 2000, 3000, 5000]
    labs = ["≤ 1 km", "1–2 km", "2–3 km", "3–5 km", "> 5 km"]
    tot = g[P].sum()
    rows, cls = [], {}
    for key, col in (("truoc", "dist_m"), ("sau", "dist_sau")):
        cls[key] = np.digitize(g[col].values, bins, right=True)
    for i, lab in enumerate(labs):
        r = dict(lop_khoang_cach=lab)
        for key in ("truoc", "sau"):
            v = g.loc[cls[key] == i, P].sum()
            r[f"dan_{key}"] = round(v); r[f"ty_le_{key}_pct"] = round(100 * v / tot, 2)
        r["thay_doi_dan"] = r["dan_sau"] - r["dan_truoc"]
        rows.append(r)
    cmp_ = pd.DataFrame(rows)
    stat = {key: dict(p3=100 * g.loc[g[col] <= MAIN_KM * 1000, P].sum() / tot, far=g.loc[g[col] > WHITE_KM * 1000, P].sum())
            for key, col in (("truoc", "dist_m"), ("sau", "dist_sau"))}
    cmp_.loc[len(cmp_)] = dict(lop_khoang_cach=f"Trong {MAIN_KM:g} km (KPI chính)", dan_truoc=round(tot * stat["truoc"]["p3"] / 100),
                               ty_le_truoc_pct=round(stat["truoc"]["p3"], 2), dan_sau=round(tot * stat["sau"]["p3"] / 100),
                               ty_le_sau_pct=round(stat["sau"]["p3"], 2), thay_doi_dan=round(tot * (stat["sau"]["p3"] - stat["truoc"]["p3"]) / 100))
    cmp_.insert(0, "so_diem_de_xuat", K)
    save_csv(cmp_, TAB / "ban_do_truoc_sau_de_xuat.csv", S, note=f"dan {BASE_YEAR} theo lop khoang cach chim bay, truoc va sau khi them {K} diem (09_greedy)")
    log.info("Truoc/sau %d diem: 3 km %.2f%% -> %.2f%%; dan > 5 km %.0f -> %.0f", K, stat["truoc"]["p3"], stat["sau"]["p3"], stat["truoc"]["far"], stat["sau"]["far"])

    fig, axs = plt.subplots(1, 2, figsize=(12.4, 6.0), dpi=200, facecolor=SURF)
    fig.subplots_adjust(left=0.01, right=0.99, top=0.83, bottom=0.2, wspace=0.02)
    for a, key, ttl in ((axs[0], "truoc", f"TRƯỚC: {len(st)} trạm hiện có"), (axs[1], "sau", f"SAU: {len(st)} trạm + {K} điểm đề xuất")):
        a.scatter(g.lon, g.lat, c=[C_SEQ[c] for c in cls[key]], s=0.75, marker="s", linewidths=0, rasterized=True)
        for gg in dist_geom.values():
            for rr in rings(gg):
                a.plot(rr[:, 0], rr[:, 1], color=INK2, lw=0.7, alpha=.8)
        for rr in sum((rings(x) for x in cm.geom), []):
            a.plot(rr[:, 0], rr[:, 1], color=INK2, lw=0.25, alpha=.45)
        a.scatter(st.lon, st.lat, s=14, c=INK, edgecolor="white", linewidth=0.8, zorder=6)
        if key == "sau":
            for _, s_ in sites.iterrows():
                a.add_patch(Ellipse((s_.lon, s_.lat), 2 * MAIN_KM * 1000 / (111320 * KLAT), 2 * MAIN_KM * 1000 / 110600, fill=False, ec=C_NEW, lw=1, ls=(0, (4, 2)), zorder=6))
            a.scatter(sites.lon, sites.lat, s=95, marker="*", c=C_NEW, edgecolor="white", linewidth=0.9, zorder=7)
            for _, s_ in sites.iterrows():
                a.annotate(f"{int(s_.thu_tu)}  {s_.xa.replace('Xã ', '').replace('Phường ', '')}", (s_.lon, s_.lat), xytext=(7, 6), textcoords="offset points",
                           fontsize=7.2, color=INK, weight="bold", zorder=8,
                           bbox=dict(boxstyle="round,pad=0.15", fc=SURF, ec="none", alpha=.85))
        a.set_xlim(W - 0.015, E + 0.015); a.set_ylim(S_ - 0.02, N + 0.015)
        a.set_aspect(1 / KLAT); a.set_xticks([]); a.set_yticks([]); a.set_facecolor(SURF)
        for s in a.spines.values():
            s.set_visible(False)
        hs = [Patch(color=C_SEQ[i], label=f"{labs[i]}: {vn(100 * g.loc[cls[key] == i, P].sum() / tot, 1)}% dân") for i in range(5)]
        hs.append(Line2D([], [], marker="o", ls="", mfc=INK, mec="white", ms=5, label="Trạm hiện có"))
        if key == "sau":
            hs.append(Line2D([], [], marker="*", ls="", mfc=C_NEW, mec="white", ms=10, label=f"Điểm đề xuất (số = thứ tự ưu tiên), vòng {MAIN_KM:g} km"))
        a.legend(handles=hs, loc="upper left", bbox_to_anchor=(0, -0.01), ncol=3, frameon=False, fontsize=7.4, columnspacing=1.4,
                 title="Khoảng cách tới trạm/điểm gần nhất", title_fontsize=7.8, alignment="left")
        a.set_title(ttl, loc="left", fontsize=10.5, color=INK, weight="bold", pad=20)
        a.text(0, 1.012, f"{vn(stat[key]['p3'], 2)}% dân trong {MAIN_KM:g} km  ·  {vn(stat[key]['far'], 0)} người cách trên {WHITE_KM:g} km",
               transform=a.transAxes, fontsize=8.6, color=INK2, va="bottom")
        scale_bar(a, E - 0.1, S_ - 0.012)
    fig.suptitle(f"Tiếp cận trạm y tế trước và sau khi thêm {K} điểm đề xuất – dân số {BASE_YEAR}", x=0.01, ha="left", fontsize=12.5, color=INK, weight="bold")
    fig.text(0.01, 0.012, f"Khoảng cách đường chim bay. Điểm đề xuất do thuật toán tham lam chọn (A10), chưa xét quỹ đất và cấp đường. Dân số là ước tính WorldPop {BASE_YEAR}. "
             "File: outputs/tables/ban_do_truoc_sau_de_xuat.csv, greedy_sites.csv", fontsize=6.3, color=INK2)
    f9 = FIG / "h9_truoc_sau_de_xuat.png"
    fig.savefig(f9, facecolor=SURF); plt.close(fig); register(f9, S, note=f"ban do truoc/sau khi them {K} diem de xuat")


if __name__ == "__main__":
    main()
