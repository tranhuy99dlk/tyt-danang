# -*- coding: utf-8 -*-
"""3 – BÁO CÁO (giai đoạn 6 của vòng đời: trực quan hoá và diễn giải): hình, bản đồ, từ điển dữ liệu, kiểm tra sổ kết quả, số liệu chốt.

KHÔNG tính số mới: chỉ ĐỌC các bảng mà pipeline Spark 5 bước (2_spark_5_buoc/chay_spark.py) đã ghi trong outputs/tables/.
Đầu ra:
  outputs/figures/h1 … h9 (PNG)                     h6 chỉ có khi có xã giảm tiếp cận đến 2030
  outputs/tables/7_kiem_chung/pham_vi_da_nang_cu.csv             phạm vi toạ độ, diện tích, số đơn vị của Đà Nẵng cũ
  outputs/tables/1_du_lieu_nguon/xa_phuong_56_toa_do.csv            1 dòng / xã: số trên bản đồ h8, tâm, khung toạ độ, toạ độ trạm
  outputs/tables/5_khuyen_nghi/ban_do_truoc_sau_de_xuat.csv       dân theo lớp khoảng cách trước / sau khi thêm K điểm đề xuất
  outputs/tables/7_kiem_chung/tu_dien_du_lieu.csv                ý nghĩa các bảng Bronze / Silver / Gold và cột chính
  outputs/tables/7_kiem_chung/kiem_tra_run_manifest.csv          mỗi file kết quả: có trong sổ không, SHA-256 còn khớp không
  outputs/tables/7_kiem_chung/so_lieu_chot.csv                   các con số dùng trong báo cáo kèm file nguồn và SHA-256
Chạy CUỐI CÙNG:  python scripts/3_bao_cao/bao_cao.py   (biến môi trường K_MAP = số điểm vẽ trên h9, mặc định 5)
"""
import json  # đọc GeoJSON
import os  # đọc biến môi trường K_MAP
import warnings  # tắt cảnh báo của statsmodels

import matplotlib  # thư viện vẽ hình
matplotlib.use("Agg")  # vẽ ra file, không mở cửa sổ
import matplotlib.pyplot as plt  # giao diện vẽ
from matplotlib.collections import PolyCollection  # tô nhiều đa giác một lần
from matplotlib.lines import Line2D  # ký hiệu chú giải dạng đường/điểm
from matplotlib.patches import Ellipse, Patch, Rectangle  # hình elip, ô màu chú giải, hình chữ nhật
import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu
from scipy.spatial import cKDTree  # cây láng giềng gần nhất
from shapely.geometry import shape  # đổi GeoJSON sang hình học
from shapely.ops import unary_union  # hợp nhiều vùng

import sys  # danh sách thư mục Python tìm module
from pathlib import Path  # làm việc với đường dẫn
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "chung"))  # thêm scripts/chung/ để import được common.py, hinh_hoc.py
from common import (bang, ROOT, TAB, FIG, RUN_MANIFEST, BASE_YEAR, FORECAST_YEAR, MAIN_KM, WHITE_KM, DISTRICTS,  # tham số dùng chung
                    bronze_moi_nhat, get_logger, register, save_csv, sha256)  # hàm tiện ích
from hinh_hoc import utm49n  # đổi toạ độ sang mét

S = "bao_cao"  # tên bước, dùng cho log và sổ run_manifest
log = get_logger(S)  # bộ ghi log
B, FY = BASE_YEAR, FORECAST_YEAR  # năm hiện trạng, năm dự báo
K_MAP = int(os.environ.get("K_MAP", "5"))  # số điểm đề xuất vẽ trên bản đồ h9
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#d9d8d3", "#fcfcfb"  # màu chữ, chữ phụ, lưới, nền
C_SEQ = ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]  # 5 lớp khoảng cách: nhạt -> đậm
C_GRP = {"Phường": "#2a78d6", "Xã đồng bằng/ven đô": "#eb6834", "Xã miền núi (địa hình)": "#1baf7a"}  # màu 3 nhóm địa bàn
C_DIST = dict(zip(DISTRICTS, ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6"]))  # màu 7 quận/huyện
C1, C2 = "#2a78d6", "#eb6834"  # hai màu chính
KLAT = np.cos(np.deg2rad(16.05))  # hệ số co kinh độ tại vĩ độ Đà Nẵng (để bản đồ không bị bẹt)
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": INK2,  # phông chữ có dấu tiếng Việt, cỡ chữ
                     "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2})  # màu trục


def doc(ten):  # đọc một bảng CSV trong outputs/tables
    return pd.read_csv(bang(ten), encoding="utf-8-sig")  # đọc bảng trong thư mục của bước tạo ra nó (mã utf-8-sig)


def vn(v, nd=1):  # định dạng số kiểu Việt Nam: dấu phẩy thập phân, dấu chấm hàng nghìn
    return f"{v:,.{nd}f}".replace(",", "_").replace(".", ",").replace("_", ".")  # đổi chỗ dấu


def cac_vong(g):  # các vòng ngoài của một đa giác (để vẽ viền)
    return [np.asarray(p.exterior.coords) for p in (g.geoms if g.geom_type == "MultiPolygon" else [g])]  # danh sách mảng toạ độ


def lam_dep(ax):  # bỏ trục và khung cho bản đồ
    ax.set_aspect(1 / KLAT)  # tỷ lệ kinh/vĩ đúng
    ax.set_xticks([]); ax.set_yticks([])  # bỏ vạch chia
    for s_ in ax.spines.values():  # bốn cạnh khung
        s_.set_visible(False)  # ẩn


def luu(fig, ten, ghi_chu=""):  # lưu hình, đóng hình, đăng ký vào sổ
    p = FIG / ten  # đường dẫn file
    fig.savefig(p, facecolor=fig.get_facecolor())  # ghi PNG
    plt.close(fig)  # giải phóng bộ nhớ
    register(p, S, note=ghi_chu)  # đăng ký


def thanh_ty_le(ax, x0, y0, km=10, dy=0.006):  # vẽ thước tỷ lệ km trên bản đồ
    dx = km * 1000 / (111320 * KLAT)  # đổi km ra độ kinh
    ax.plot([x0, x0 + dx], [y0, y0], color=INK, lw=2, solid_capstyle="butt")  # đoạn thẳng
    ax.text(x0 + dx / 2, y0 + dy, f"{km} km", ha="center", va="bottom", fontsize=7, color=INK)  # nhãn


def main():  # hàm chính
    feats = json.load(open(bronze_moi_nhat("tra_cuu_khong_gian") / "ranh_gioi_56_xa.geojson", encoding="utf-8"))["features"]  # ranh giới 56 xã
    st = doc("tram_y_te_56.csv")  # 56 trạm
    kx = doc("kpi_theo_xa_spark.csv")  # KPI theo xã
    o = doc("tiep_can_luoi_100m.csv").merge(doc("dan_cu_luoi_100m.csv")[["cell_id", "lon", "lat", "elev_m"]], on="cell_id")  # ô có dân 2024 + toạ độ
    P = f"pop_{B}"  # tên cột dân năm hiện trạng
    polys = [shape(f["geometry"]) for f in feats]  # đa giác các xã

    def ve_ranh_gioi(ax, lw=0.4):  # vẽ viền 56 xã
        for p in polys:  # từng xã
            for v in cac_vong(p):  # từng vòng
                ax.plot(v[:, 0], v[:, 1], color=INK2, lw=lw, alpha=.7)  # đường viền

    def ve_tram(ax, s=22):  # vẽ 56 trạm
        ax.scatter(st.lon, st.lat, s=s, c=INK, edgecolor="white", linewidth=1, zorder=5)  # chấm đen viền trắng

    # ---------- h1: bản đồ khoảng cách tới trạm gần nhất ----------
    bins, nhan = [1000, 2000, 3000, 5000], ["≤ 1 km", "1–2 km", "2–3 km", "3–5 km", "> 5 km"]  # mốc và nhãn 5 lớp
    lop = np.digitize(o.dist_m, bins)  # lớp của từng ô
    fig, ax = plt.subplots(figsize=(7.2, 6.2), dpi=200)  # khung hình
    ax.scatter(o.lon, o.lat, c=[C_SEQ[c] for c in lop], s=0.6, marker="s", linewidths=0)  # tô ô theo lớp
    ve_ranh_gioi(ax); ve_tram(ax)  # viền xã và trạm
    hs = [Patch(color=C_SEQ[i], label=f"{nhan[i]}: {100 * o.loc[lop == i, P].sum() / o[P].sum():.1f}% dân") for i in range(5)]  # chú giải lớp
    ax.legend(handles=hs + [Line2D([], [], marker="o", ls="", mfc=INK, mec="white", label="Trạm y tế (toạ độ nhập tay)")],  # chú giải
              loc="lower left", frameon=False, fontsize=7.5, title="Khoảng cách tới trạm gần nhất", title_fontsize=8)  # (tiếp)
    ax.set_title(f"Khoảng cách đường chim bay tới trạm y tế gần nhất, dân số {B}", loc="left", color=INK, fontsize=10)  # tiêu đề
    lam_dep(ax)  # bỏ trục
    fig.text(0.01, 0.01, "Nguồn: WorldPop R2025A (CC BY 4.0); © OpenStreetMap contributors (ODbL). File: tiep_can_luoi_100m.csv", fontsize=6, color=INK2)  # ghi nguồn
    fig.tight_layout(); luu(fig, "h1_ban_do_khoang_cach.png")  # lưu

    # ---------- h2: vùng xám / trắng trên nền độ cao ----------
    vz = doc("white_zones.csv")  # các cụm vùng lõm
    xa_ = o.dist_m > MAIN_KM * 1000  # ô cách trạm > 3 km
    fig, ax = plt.subplots(figsize=(7.2, 6.2), dpi=200)  # khung hình
    ax.scatter(o.lon[~xa_], o.lat[~xa_], c="#e9e8e4", s=0.5, marker="s", linewidths=0)  # ô gần: tô xám nhạt
    sc = ax.scatter(o.lon[xa_], o.lat[xa_], c=o.elev_m[xa_], cmap="Oranges", vmin=0, vmax=max(300, np.nanpercentile(o.elev_m[xa_], 95)), s=1.2, marker="s", linewidths=0)  # ô xa: màu theo độ cao
    ve_ranh_gioi(ax); ve_tram(ax)  # viền và trạm
    for _, z in vz[vz.loai_vung.str.startswith("trang")].head(6).iterrows():  # 6 cụm vùng trắng đông dân nhất
        ax.annotate(f"{z.ma_vung}: {vn(z.dan, 0)} người\n{z.nguyen_nhan}", (z.lon, z.lat), xytext=(8, 8), textcoords="offset points",  # nhãn cụm
                    fontsize=6.5, color=INK, arrowprops=dict(arrowstyle="-", color=INK2, lw=.5))  # (tiếp)
    fig.colorbar(sc, ax=ax, shrink=.5, pad=.01).set_label("Độ cao (m) – chỉ tô ô cách trạm > 3 km", fontsize=7)  # thang màu
    ax.set_title("Vùng xám (3–5 km) và vùng trắng (> 5 km) trên nền độ cao", loc="left", color=INK, fontsize=10)  # tiêu đề
    lam_dep(ax)  # bỏ trục
    fig.text(0.01, 0.01, "Nguồn: WorldPop R2025A; OSM; AWS Terrain Tiles. File: white_zones.csv (giả định A8)", fontsize=6, color=INK2)  # nguồn
    fig.tight_layout(); luu(fig, "h2_vung_trang_dia_hinh.png")  # lưu

    # ---------- h3: xã chưa đạt 100% dân trong 3 km ----------
    dt = {f["properties"]["ma_tram"]: f["properties"]["dien_tich_km2"] for f in feats}  # diện tích từng xã
    kx["mat_do_nguoi_km2"] = kx.dan / kx.ma_tram.map(dt)  # mật độ dân
    sub = kx[kx.ty_le_trong_3km_pct < 99.95].sort_values("ty_le_trong_3km_pct")  # xã chưa đạt 100%
    if len(sub):  # có xã như vậy
        fig, ax = plt.subplots(figsize=(7.2, 0.32 * len(sub) + 1.2), dpi=200)  # khung hình cao theo số xã
        y = np.arange(len(sub))  # vị trí thanh
        ax.barh(y, sub.ty_le_trong_3km_pct, color=[C_GRP[n] for n in sub.nhom], height=.62)  # thanh ngang màu theo nhóm
        for yy, v, dd in zip(y, sub.ty_le_trong_3km_pct, sub.dan_tren_5km):  # nhãn giá trị
            ax.text(v + 1, yy, f"{v:.1f}%" + (f"  ({vn(dd, 0)} người > 5 km)" if dd >= 0.5 else ""), va="center", fontsize=7, color=INK)  # chữ
        ax.set_yticks(y, sub.ten_day_du, fontsize=7.5); ax.set_xlim(0, 115)  # nhãn trục y, giới hạn trục x
        ax.set_xlabel("% dân trong 3 km tới trạm gần nhất"); ax.grid(axis="x", color=GRID, lw=.5); ax.set_axisbelow(True)  # nhãn, lưới
        for s_ in ("top", "right"):  # bỏ khung trên, phải
            ax.spines[s_].set_visible(False)  # ẩn
        ax.legend(handles=[Patch(color=c, label=k) for k, c in C_GRP.items()], frameon=False, fontsize=7, loc="lower right")  # chú giải nhóm
        ax.set_title(f"Các xã/phường chưa đạt 100% dân trong 3 km ({len(sub)}/56)", loc="left", color=INK, fontsize=10)  # tiêu đề
        fig.text(0.01, 0.005, "File: kpi_theo_xa_spark.csv", fontsize=6, color=INK2)  # nguồn
        fig.tight_layout(); luu(fig, "h3_ty_le_3km_theo_xa.png")  # lưu

    # ---------- h4: mật độ dân và khoảng cách trung vị ----------
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=200)  # khung hình
    for n_, c_ in C_GRP.items():  # từng nhóm
        d = kx[kx.nhom == n_]  # xã của nhóm
        ax.scatter(d.mat_do_nguoi_km2, d.kc_trung_vi_m, s=28, color=c_, edgecolor="white", linewidth=1, label=f"{n_} ({len(d)})")  # chấm
    for _, r in kx.nlargest(6, "kc_trung_vi_m").iterrows():  # 6 xã xa nhất
        ax.annotate(r.ten_day_du.replace("Phường ", "P. ").replace("Xã ", "X. "), (r.mat_do_nguoi_km2, r.kc_trung_vi_m), xytext=(4, 3), textcoords="offset points", fontsize=6.5)  # nhãn
    ax.set_xscale("log"); ax.set_xlabel("Mật độ dân (người/km², thang log)"); ax.set_ylabel("Khoảng cách trung vị tới trạm (m)")  # trục
    ax.grid(color=GRID, lw=.5); ax.legend(frameon=False, fontsize=7)  # lưới, chú giải
    for s_ in ("top", "right"):  # bỏ khung
        ax.spines[s_].set_visible(False)  # ẩn
    ax.set_title("Xã càng thưa dân, người dân càng ở xa trạm", loc="left", color=INK, fontsize=10)  # tiêu đề
    fig.text(0.01, 0.01, "File: kpi_theo_xa_spark.csv", fontsize=6, color=INK2)  # nguồn
    fig.tight_layout(); luu(fig, "h4_mat_do_vs_khoang_cach.png")  # lưu

    # ---------- h5: dân số và dân 60+ – WorldPop 2015–2024, số chiếu, dự báo 2030 ----------
    from statsmodels.tsa.exponential_smoothing.ets import ETSModel  # mô hình ETS (vẽ khoảng 80% cho tổng thành phố)
    warnings.filterwarnings("ignore")  # bỏ cảnh báo hội tụ
    tt = doc("commune_series.csv").groupby("year")[["pop_ds", "pop60_ds"]].sum()  # tổng 56 xã theo năm
    ch = doc("forecast_worldpop_chieu.csv").set_index("year")  # số chiếu WorldPop 2025–2030
    fc = doc("forecast_commune_2030.csv")  # dự báo theo xã
    fig, axs = plt.subplots(1, 2, figsize=(7.2, 3.0), dpi=200)  # hai khung
    for ax, col, k, c, ttl in ((axs[0], "pop_ds", "dan", C1, "Dân số 56 xã/phường"), (axs[1], "pop60_ds", "dan60", C2, "Dân 60+")):  # dân số, 60+
        y = tt[col].values  # chuỗi 2015–2024
        ax.plot(tt.index, y / 1e3, color=c, lw=2, label=f"WorldPop 2015–{B} (ước tính)")  # đường lịch sử
        ax.plot([B] + list(ch.index), np.r_[y[-1], ch[k].values] / 1e3, color=c, lw=1.2, ls=":", label=f"WorldPop chiếu {B + 1}–{FY}")  # số chiếu
        mh = ETSModel(pd.Series(np.round(y, 6)), error="add", trend="add", damped_trend=True).fit(disp=False)  # khớp ETS cho tổng thành phố
        fr = mh.get_prediction(start=len(y), end=len(y) + FY - B - 1).summary_frame(alpha=0.2)  # dự báo kèm khoảng 80% (alpha = 0,2)
        nam = np.arange(B, FY + 1)  # trục năm dự báo
        ax.plot(nam, np.r_[y[-1], fr["mean"].values] / 1e3, color=INK, lw=1.5, ls="--", label="ETS(A,Ad,N) – tổng TP (khoảng 80%)")  # đường dự báo
        ax.fill_between(nam[1:], fr["pi_lower"] / 1e3, fr["pi_upper"] / 1e3, color=INK, alpha=.12, lw=0)  # dải 80%
        ax.scatter([FY], [fc[f"{k}_2030"].sum() / 1e3], s=24, color=c, edgecolor="white", zorder=5, label="Tổng dự báo theo xã")  # điểm tổng theo xã
        ax.set_title(ttl + " (nghìn người)", loc="left", fontsize=9, color=INK); ax.set_xticks(range(2015, FY + 1, 5)); ax.grid(color=GRID, lw=.5)  # trang trí
        for s_ in ("top", "right"):  # bỏ khung
            ax.spines[s_].set_visible(False)  # ẩn
    axs[0].legend(frameon=False, fontsize=6.5)  # chú giải
    fig.text(0.01, 0.005, "Sau 2024 là DỰ BÁO / SỐ CHIẾU, không phải số đo. File: commune_series.csv, forecast_total_2030.csv (A9)", fontsize=6, color=INK2)  # nguồn
    fig.tight_layout(); luu(fig, "h5_du_bao_dan_so.png")  # lưu

    # ---------- h6: xã giảm tiếp cận đến 2030 (nếu có) ----------
    ac = doc("access_2030_change.csv")  # thay đổi tiếp cận 2030
    sub = ac[ac.giam_tiep_can].sort_values("thay_doi_pp")  # xã giảm
    (FIG / "h6_xa_giam_tiep_can_2030.png").unlink(missing_ok=True)  # xoá hình cũ (nếu lần này không có xã nào giảm)
    if len(sub):  # có xã giảm
        fig, ax = plt.subplots(figsize=(7.2, 0.35 * len(sub) + 1.2), dpi=200)  # khung hình
        ax.barh(np.arange(len(sub)), sub.thay_doi_pp, color=C2, height=.6)  # thanh
        ax.set_yticks(np.arange(len(sub)), sub.ten_day_du, fontsize=7.5); ax.axvline(0, color=INK2, lw=.6)  # nhãn, trục 0
        ax.set_title(f"Xã/phường có tỷ lệ tiếp cận 3 km giảm đến 2030 ({len(sub)})", loc="left", fontsize=10, color=INK)  # tiêu đề
        fig.tight_layout(); luu(fig, "h6_xa_giam_tiep_can_2030.png")  # lưu

    # ---------- h7: vị trí đề xuất và đường cong độ phủ ----------
    sites = doc("greedy_sites.csv")  # điểm đề xuất
    cur = doc("greedy_curve.csv")  # đường cong độ phủ
    R = MAIN_KM * 1000  # 3 km (mét)
    fig, (ax, bx) = plt.subplots(1, 2, figsize=(7.2, 4.0), dpi=200, gridspec_kw={"width_ratios": [1.5, 1]})  # hai khung
    ax.scatter(o.lon[~xa_], o.lat[~xa_], c="#e9e8e4", s=.4, marker="s", linewidths=0)  # ô gần
    ax.scatter(o.lon[xa_], o.lat[xa_], c=C2, s=.8, marker="s", linewidths=0, label="Ô dân cách trạm > 3 km")  # ô xa
    ve_ranh_gioi(ax, .3); ve_tram(ax, 12)  # viền, trạm
    k5 = sites.head(5)  # 5 điểm đầu
    ax.scatter(k5.lon, k5.lat, s=60, marker="*", c=C1, edgecolor="white", linewidth=.8, zorder=6, label="Đề xuất (K=5)")  # sao
    for _, s_ in k5.iterrows():  # vòng 3 km quanh điểm
        ax.add_patch(Ellipse((s_.lon, s_.lat), 2 * R / (111320 * KLAT), 2 * R / 111320, fill=False, color=C1, lw=.6, ls="--"))  # elip
        ax.annotate(str(int(s_.thu_tu)), (s_.lon, s_.lat), xytext=(4, 4), textcoords="offset points", fontsize=7, weight="bold")  # số thứ tự
    lam_dep(ax); ax.legend(frameon=False, fontsize=6.5, loc="lower left", markerscale=1.5)  # trang trí
    ax.set_title("Vị trí đề xuất (tham lam, R = 3 km)", loc="left", fontsize=9, color=INK)  # tiêu đề
    for mt, c_ in ((f"Dân {B}", C1), (f"Dân 60+ {B}", C2)):  # hai mục tiêu
        d = cur[cur.muc_tieu == mt]  # điểm của mục tiêu
        bx.plot(d.K, d.ty_le_3km_pct, marker="o", ms=3.5, lw=2, color=c_, label=mt)  # đường
    bx.set_xlabel("Số vị trí mới K"); bx.set_ylabel("% đối tượng trong 3 km"); bx.grid(color=GRID, lw=.5); bx.legend(frameon=False, fontsize=6.5)  # trục
    for s_ in ("top", "right"):  # bỏ khung
        bx.spines[s_].set_visible(False)  # ẩn
    bx.set_title("Độ phủ tăng thêm theo K", loc="left", fontsize=9, color=INK)  # tiêu đề
    fig.text(0.01, 0.005, "File: greedy_sites.csv, greedy_curve.csv (giả định A10)", fontsize=6, color=INK2)  # nguồn
    fig.tight_layout(); luu(fig, "h7_de_xuat_vi_tri.png")  # lưu

    # ---------- bảng phạm vi + h8: bản đồ hành chính trước 01/01/2025 ----------
    stx = st.set_index("ma_tram")  # trạm theo mã
    cm = []  # danh sách xã
    for f in feats:  # từng xã
        g, pr = shape(f["geometry"]), f["properties"]  # hình học, thuộc tính
        c = g.centroid if g.contains(g.centroid) else g.representative_point()  # điểm đặt nhãn (tâm, hoặc điểm trong nếu tâm rơi ra ngoài)
        w_, s_, e_, n_ = g.bounds  # khung toạ độ
        cm.append(dict(so_tren_ban_do=int(pr["ma_tram"][3:]), ma_tram=pr["ma_tram"], ten_day_du=pr["ten_day_du"], quan_huyen=pr["quan_huyen"],  # dòng xã
                       loai_dv=pr["loai_dv"], dien_tich_km2=pr["dien_tich_km2"], lon_tam=round(c.x, 5), lat_tam=round(c.y, 5),  # (tiếp)
                       lon_min=round(w_, 5), lon_max=round(e_, 5), lat_min=round(s_, 5), lat_max=round(n_, 5),  # (tiếp)
                       nguon_ranh_gioi=pr["nguon_ranh_gioi"], lat_tram=stx.loc[pr["ma_tram"], "lat"], lon_tram=stx.loc[pr["ma_tram"], "lon"],  # (tiếp)
                       tram_trong_ranh_gioi_xa=stx.loc[pr["ma_tram"], "trong_ranh_gioi_xa"], tram_cach_ranh_gioi_m=stx.loc[pr["ma_tram"], "kc_ngoai_ranh_gioi_m"], geom=g))  # (tiếp)
    cm = pd.DataFrame(cm).sort_values("so_tren_ban_do").reset_index(drop=True)  # bảng xã theo số thứ tự
    save_csv(cm.drop(columns="geom"), bang("xa_phuong_56_toa_do.csv"), S, note="56 xa/phuong truoc 01/01/2025: so tren ban do h8, tam, khung toa do")  # ghi CSV
    U = unary_union(list(cm.geom))  # hợp 56 xã
    W, S_, E, N = U.bounds  # khung toạ độ Đà Nẵng cũ
    n_p, n_x = int((cm.loai_dv == "Phường").sum()), int((cm.loai_dv == "Xã").sum())  # số phường, số xã
    nguon = "; ".join(sorted(cm.nguon_ranh_gioi.unique()))  # nguồn ranh giới
    pv = [("Kinh độ cực tây", round(W, 4), "độ Đông", cm.loc[cm.lon_min.idxmin(), "ten_day_du"]),  # bảng phạm vi
          ("Kinh độ cực đông", round(E, 4), "độ Đông", cm.loc[cm.lon_max.idxmax(), "ten_day_du"]),  # (tiếp)
          ("Vĩ độ cực nam", round(S_, 4), "độ Bắc", cm.loc[cm.lat_min.idxmin(), "ten_day_du"]),  # (tiếp)
          ("Vĩ độ cực bắc", round(N, 4), "độ Bắc", cm.loc[cm.lat_max.idxmax(), "ten_day_du"]),  # (tiếp)
          ("Kinh độ tâm hình học", round(U.centroid.x, 4), "độ Đông", ""), ("Vĩ độ tâm hình học", round(U.centroid.y, 4), "độ Bắc", ""),  # (tiếp)
          ("Bề ngang đông–tây", round((E - W) * 111.32 * KLAT, 1), "km", "xấp xỉ, tại vĩ độ 16,05"),  # (tiếp)
          ("Bề dọc bắc–nam", round((N - S_) * 110.6, 1), "km", "xấp xỉ"),  # (tiếp)
          ("Tổng diện tích 56 xã/phường", round(cm.dien_tich_km2.sum(), 1), "km²", "tính trên UTM 49N từ " + nguon),  # (tiếp)
          ("Số quận/huyện", cm.quan_huyen.nunique(), "đơn vị", "; ".join(f"{d} ({(cm.quan_huyen == d).sum()})" for d in DISTRICTS)),  # (tiếp)
          ("Số xã/phường", len(cm), "đơn vị", f"{n_p} phường, {n_x} xã – bản đồ hành chính trước 01/01/2025"),  # (tiếp)
          ("Số trạm y tế", len(st), "trạm", "mỗi xã/phường một trạm (A13)")]  # (tiếp)
    save_csv(pd.DataFrame(pv, columns=["chi_so", "gia_tri", "don_vi", "ghi_chu"]), bang("pham_vi_da_nang_cu.csv"), S, note=f"pham vi 56 xa/phuong theo {nguon}")  # ghi CSV
    fig = plt.figure(figsize=(12.4, 7.0), dpi=200, facecolor=SURF)  # khung hình lớn
    gs = fig.add_gridspec(2, 2, width_ratios=[1.75, 1], height_ratios=[1, 1.15], left=0.045, right=0.985, top=0.885, bottom=0.085, wspace=0.05, hspace=0.12)  # bố cục
    ax, zx, ky = fig.add_subplot(gs[:, 0]), fig.add_subplot(gs[0, 1]), fig.add_subplot(gs[1, 1])  # bản đồ chính, phóng to, bảng chú giải
    ZOOM = (108.168, 108.262, 16.022, 16.098)  # khung phóng to lõi đô thị
    quan_g = {d: unary_union(list(cm.loc[cm.quan_huyen == d, "geom"])) for d in DISTRICTS}  # hình học từng quận

    def ve(a, lw, chinh, fs, ms):  # vẽ bản đồ hành chính lên khung a
        for _, r in cm.iterrows():  # tô từng xã theo màu quận
            a.add_collection(PolyCollection(cac_vong(r.geom), facecolors=C_DIST[r.quan_huyen], edgecolors="white", linewidths=lw))  # đa giác
        for gq in quan_g.values():  # viền quận
            for v in cac_vong(gq):  # từng vòng
                a.plot(v[:, 0], v[:, 1], color=INK, lw=0.9, alpha=.75)  # đường viền đậm
        a.scatter(st.lon, st.lat, s=ms, c=INK, edgecolor="white", linewidth=0.8, zorder=6)  # trạm
        for _, r in cm.iterrows():  # số thứ tự xã
            trong = ZOOM[0] <= r.lon_tam <= ZOOM[1] and ZOOM[2] <= r.lat_tam <= ZOOM[3]  # xã nằm trong khung phóng to
            if not (chinh and trong):  # bản đồ chính: xã trong khung phóng to chỉ đánh số ở hình phóng to
                a.text(r.lon_tam, r.lat_tam, str(r.so_tren_ban_do), ha="center", va="center", fontsize=fs, weight="bold",  # số
                       color=INK if r.quan_huyen == DISTRICTS[3] else "white", zorder=7, clip_on=True)  # (tiếp) màu chữ tương phản
        a.set_aspect(1 / KLAT); a.set_facecolor(SURF)  # tỷ lệ, nền
    ve(ax, 0.5, True, 6.4, 13)  # bản đồ chính
    ax.set_xlim(W - 0.02, E + 0.02); ax.set_ylim(S_ - 0.03, N + 0.02); ax.grid(color=GRID, lw=.6); ax.set_axisbelow(True)  # khung, lưới
    ax.xaxis.set_major_formatter(lambda v, _: vn(v, 2) + "°Đ"); ax.yaxis.set_major_formatter(lambda v, _: vn(v, 2) + "°B"); ax.tick_params(labelsize=7)  # nhãn toạ độ
    ax.add_patch(Rectangle((ZOOM[0], ZOOM[2]), ZOOM[1] - ZOOM[0], ZOOM[3] - ZOOM[2], fill=False, ec=INK, lw=1, ls=(0, (4, 2)), zorder=7))  # khung phóng to
    thanh_ty_le(ax, W + 0.005, S_ - 0.018)  # thước tỷ lệ
    ax.legend(handles=[Line2D([], [], marker="o", ls="", mfc=INK, mec="white", ms=5, label="Trạm y tế xã/phường (56)"),  # chú giải ký hiệu
                       Line2D([], [], color=INK, lw=0.9, label="Ranh giới quận/huyện")], loc="lower right", fontsize=7)  # (tiếp)
    ve(zx, 0.7, False, 6.4, 7)  # hình phóng to
    zx.set_xlim(ZOOM[0], ZOOM[1]); zx.set_ylim(ZOOM[2], ZOOM[3]); zx.set_xticks([]); zx.set_yticks([])  # khung phóng to
    zx.set_title("Lõi đô thị (phóng to)", loc="left", fontsize=8.5, color=INK)  # tiêu đề
    ky.axis("off"); ky.set_xlim(0, 1); ky.set_ylim(0, 1)  # khung chú giải dạng chữ
    ky.text(0, 1.0, "Quận/huyện (màu) và số trên bản đồ → xã/phường", fontsize=8.2, color=INK, va="top", weight="bold")  # tiêu đề chú giải
    yy, xx = 0.925, 0.0  # vị trí chữ
    for d in DISTRICTS:  # từng quận
        sub = cm[cm.quan_huyen == d]  # xã của quận
        if yy - (0.05 + 0.034 * int(np.ceil(len(sub) / 2))) < -0.03:  # hết chỗ cột trái
            yy, xx = 0.925, 0.5  # sang cột phải
        ky.add_patch(Rectangle((xx, yy - 0.026), 0.022, 0.026, fc=C_DIST[d], ec="none", transform=ky.transAxes, clip_on=False))  # ô màu
        ky.text(xx + 0.03, yy - 0.013, f"{d} ({len(sub)})", fontsize=6.9, color=INK, va="center", weight="bold"); yy -= 0.045  # tên quận
        ten = [f"{r.so_tren_ban_do} {r.ten_day_du.replace('Phường ', '').replace('Xã ', '')}" for _, r in sub.iterrows()]  # danh sách xã
        for i in range(0, len(ten), 2):  # hai xã mỗi dòng
            ky.text(xx, yy, "  ·  ".join(ten[i:i + 2]), fontsize=6.0, color=INK2, va="center"); yy -= 0.034  # chữ
        yy -= 0.012  # khoảng cách giữa các quận
    fig.suptitle("Đà Nẵng trước 01/01/2025: 56 xã/phường, 7 quận/huyện và 56 trạm y tế", x=0.045, ha="left", fontsize=12.5, color=INK, weight="bold")  # tiêu đề lớn
    fig.text(0.045, 0.917, f"Phạm vi: {vn(W, 4)}–{vn(E, 4)}° Đông, {vn(S_, 4)}–{vn(N, 4)}° Bắc · {vn(cm.dien_tich_km2.sum(), 1)} km² · {n_p} phường, {n_x} xã", fontsize=8.5, color=INK2)  # phụ đề
    fig.text(0.045, 0.018, f"Ranh giới: {nguon} (chỉ dùng học thuật). Trạm: toạ độ nhóm nhập tay (A13). File: xa_phuong_56_toa_do.csv, pham_vi_da_nang_cu.csv", fontsize=6.3, color=INK2)  # nguồn
    luu(fig, "h8_ban_do_hanh_chinh_truoc_2025.png", "ban do hanh chinh 56 xa/phuong truoc 01/01/2025")  # lưu

    # ---------- h9: trước / sau khi thêm K điểm đề xuất ----------
    sk = sites.sort_values("thu_tu").head(K_MAP)  # K điểm đầu
    K = len(sk)  # số điểm thực có
    xy_o = np.column_stack(utm49n(o.lon.values, o.lat.values))  # toạ độ mét của các ô
    o["dist_sau"] = np.minimum(o.dist_m.values, cKDTree(sk[["x_m", "y_m"]].values).query(xy_o)[0])  # khoảng cách tới trạm/điểm gần nhất sau khi thêm điểm
    tong = o[P].sum()  # tổng dân
    lop_k = {k_: np.digitize(o[c].values, bins, right=True) for k_, c in (("truoc", "dist_m"), ("sau", "dist_sau"))}  # lớp khoảng cách trước / sau
    rows = []  # bảng so sánh
    for i, lb in enumerate(nhan):  # từng lớp
        r = dict(lop_khoang_cach=lb)  # dòng
        for k_ in ("truoc", "sau"):  # trước, sau
            v = o.loc[lop_k[k_] == i, P].sum()  # dân của lớp
            r[f"dan_{k_}"], r[f"ty_le_{k_}_pct"] = round(v), round(100 * v / tong, 2)  # số dân, tỷ lệ
        r["thay_doi_dan"] = r["dan_sau"] - r["dan_truoc"]  # thay đổi
        rows.append(r)  # thêm
    p3 = {k_: 100 * o.loc[o[c] <= R, P].sum() / tong for k_, c in (("truoc", "dist_m"), ("sau", "dist_sau"))}  # % trong 3 km
    xa5 = {k_: o.loc[o[c] > WHITE_KM * 1000, P].sum() for k_, c in (("truoc", "dist_m"), ("sau", "dist_sau"))}  # dân > 5 km
    rows.append(dict(lop_khoang_cach=f"Trong {MAIN_KM:g} km (KPI chính)", dan_truoc=round(tong * p3["truoc"] / 100), ty_le_truoc_pct=round(p3["truoc"], 2),  # dòng KPI chính
                     dan_sau=round(tong * p3["sau"] / 100), ty_le_sau_pct=round(p3["sau"], 2), thay_doi_dan=round(tong * (p3["sau"] - p3["truoc"]) / 100)))  # (tiếp)
    bs = pd.DataFrame(rows); bs.insert(0, "so_diem_de_xuat", K)  # bảng, thêm cột số điểm
    save_csv(bs, bang("ban_do_truoc_sau_de_xuat.csv"), S, note=f"dan {B} theo lop khoang cach, truoc va sau khi them {K} diem")  # ghi CSV
    fig, axs = plt.subplots(1, 2, figsize=(12.4, 6.0), dpi=200, facecolor=SURF)  # hai bản đồ
    fig.subplots_adjust(left=0.01, right=0.99, top=0.83, bottom=0.2, wspace=0.02)  # lề
    for a, k_, ttl in ((axs[0], "truoc", f"TRƯỚC: {len(st)} trạm hiện có"), (axs[1], "sau", f"SAU: {len(st)} trạm + {K} điểm đề xuất")):  # trước, sau
        a.scatter(o.lon, o.lat, c=[C_SEQ[c] for c in lop_k[k_]], s=0.75, marker="s", linewidths=0, rasterized=True)  # ô theo lớp
        ve_ranh_gioi(a, .25); a.scatter(st.lon, st.lat, s=14, c=INK, edgecolor="white", linewidth=0.8, zorder=6)  # viền, trạm
        if k_ == "sau":  # bản đồ sau: vẽ điểm đề xuất
            for _, s_ in sk.iterrows():  # từng điểm
                a.add_patch(Ellipse((s_.lon, s_.lat), 2 * R / (111320 * KLAT), 2 * R / 110600, fill=False, ec=C2, lw=1, ls=(0, (4, 2)), zorder=6))  # vòng 3 km
                a.annotate(f"{int(s_.thu_tu)}  {s_.xa.replace('Xã ', '').replace('Phường ', '')}", (s_.lon, s_.lat), xytext=(7, 6), textcoords="offset points", fontsize=7.2, weight="bold", zorder=8)  # nhãn
            a.scatter(sk.lon, sk.lat, s=95, marker="*", c=C2, edgecolor="white", linewidth=0.9, zorder=7)  # sao
        a.set_xlim(W - 0.015, E + 0.015); a.set_ylim(S_ - 0.02, N + 0.015); lam_dep(a); a.set_facecolor(SURF)  # khung
        hs = [Patch(color=C_SEQ[i], label=f"{nhan[i]}: {vn(100 * o.loc[lop_k[k_] == i, P].sum() / tong, 1)}% dân") for i in range(5)]  # chú giải
        a.legend(handles=hs, loc="upper left", bbox_to_anchor=(0, -0.01), ncol=3, frameon=False, fontsize=7.4, title="Khoảng cách tới trạm/điểm gần nhất", title_fontsize=7.8)  # chú giải
        a.set_title(ttl, loc="left", fontsize=10.5, color=INK, weight="bold", pad=20)  # tiêu đề
        a.text(0, 1.012, f"{vn(p3[k_], 2)}% dân trong {MAIN_KM:g} km  ·  {vn(xa5[k_], 0)} người cách trên {WHITE_KM:g} km", transform=a.transAxes, fontsize=8.6, color=INK2, va="bottom")  # phụ đề
    fig.suptitle(f"Tiếp cận trạm y tế trước và sau khi thêm {K} điểm đề xuất – dân số {B}", x=0.01, ha="left", fontsize=12.5, color=INK, weight="bold")  # tiêu đề lớn
    fig.text(0.01, 0.012, "Khoảng cách đường chim bay. Điểm đề xuất do thuật toán tham lam chọn (A10), chưa xét quỹ đất. File: ban_do_truoc_sau_de_xuat.csv, greedy_sites.csv", fontsize=6.3, color=INK2)  # nguồn
    luu(fig, "h9_truoc_sau_de_xuat.png", f"ban do truoc/sau khi them {K} diem de xuat")  # lưu
    log.info("Da ve 9 hinh; truoc/sau %d diem: 3 km %.2f%% -> %.2f%%", K, p3["truoc"], p3["sau"])  # in

    tu_dien()  # từ điển dữ liệu
    chot_so_lieu()  # kiểm tra sổ và số liệu chốt


def tu_dien():  # từ điển dữ liệu: các bảng của hồ dữ liệu và bảng kết quả chính
    WP, OSM = "WorldPop Global2 R2025A (CC BY 4.0)", "© OpenStreetMap contributors (ODbL 1.0)"  # nhãn nguồn
    D = [  # (tầng, tập dữ liệu, trường, kiểu, ý nghĩa, nguồn, quy tắc chất lượng, mức nhạy cảm)
        ("bronze", "worldpop/pop_100m_raw.csv", "year, row, col, pop_raw", "số", "Giá trị pixel 100 m THÔ từng năm; -99999 là NoData", WP, "NoData/ngoài 56 xã -> excluded (không phải lỗi)", "Thấp (mô hình)"),  # tầng bronze: worldpop/pop_100m_raw.csv
        ("bronze", "worldpop/pop_1km_raw.csv", "year, k1_row, k1_col, bien, gia_tri", "số/chuỗi", "Lưới 1 km: tổng dân (pop) và 7 nhóm tuổi t_60…t_90, 2015–2030 (2025–2030 là số chiếu)", WP, "bien ∈ 8 giá trị; năm ∈ 2015–2030", "Trung bình (nhóm tuổi)"),  # tầng bronze: worldpop/pop_1km_raw.csv
        ("bronze", "co_so_y_te/tyt_raw.csv", "dong_excel, cot1, cot2, cot3", "chuỗi", "Nguyên văn file toạ độ trạm nhập tay (cả dòng tiêu đề, dòng tên quận)", "Nhóm nhập tay, kiểm tra theo congdulieu.vn 12/12/2024 (A13)", "cot3 đúng mẫu 'vĩ độ, kinh độ', trong hộp bao; tên khớp danh mục", "Công khai"),  # tầng bronze: co_so_y_te/tyt_raw.csv
        ("bronze", "co_so_y_te/cskcb_raw.csv", "ma_cs, ten_cs, tuyen, hang, cap, diem, dia_chi, toa_do", "chuỗi", "Nguyên văn danh sách 30 CSKCB của Sở Y tế", "Người dùng cung cấp (A17)", "toạ độ đọc được, trong hộp bao; mã không trùng", "Công khai"),  # tầng bronze: co_so_y_te/cskcb_raw.csv
        ("bronze", "tra_cuu_khong_gian/o_luoi_100m.csv", "row, col, cell_id, lon, lat, x_m, y_m, ma_tram, k1_row, k1_col, elev_m, slope_deg, nut_duong, kc_ra_duong_m", "số/chuỗi", "Mọi pixel 100 m trong hộp bao: xã chứa tâm pixel (rỗng = ngoài 56 xã), ô 1 km, độ cao, nút đường gần nhất", "GADM 4.1 (A12); AWS Terrain Tiles; " + OSM, "khoá không trùng; toạ độ trong hộp bao; mã xã ∈ danh mục", "Công khai"),  # tầng bronze: tra_cuu_khong_gian/o_luoi_100m.csv
        ("bronze", "tra_cuu_khong_gian/duong_nut.csv, duong_canh.csv", "nut, x_m, y_m / i, j, dai_m, phut", "số", "Đồ thị đường xe máy (thành phần liên thông lớn nhất); thời gian theo tốc độ giả định", OSM + " (A6)", "cạnh trỏ tới nút có thật; độ dài > 0", "Công khai"),  # tầng bronze: tra_cuu_khong_gian/duong_nut.csv, duong_canh.csv
        ("silver", "quarantine_records / excluded_records", "nguon, loai, ly_do, khoa, ban_ghi", "chuỗi", "Dòng không vào bảng sạch: LOI_DU_LIEU (lỗi thật) -> quarantine, có nguyên văn ban_ghi; NGOAI_PHAM_VI (NoData, ngoài 56 xã) -> excluded, chỉ khoá tham chiếu (ban_ghi rỗng, tra lại Bronze theo khoa)", "b2_kiem_tra_lam_sach.py", "ly_do có thể nhiều mã nối bằng ';'", "Thấp"),  # tầng silver: quarantine_records / excluded_records
        ("silver", "dim_o_luoi (theo ma_quan)", "cell_id, ma_tram, ma_quan, lon, lat, x_m, y_m, elev_m, slope_deg, nut_duong, kc_ra_duong_m", "số/chuỗi", "Ô 100 m có dân (> 0 ở ít nhất một năm) trong 56 xã", "b3_bien_doi.py", "41 nghìn ô; mỗi ô thuộc đúng 1 xã", "Công khai"),  # tầng silver: dim_o_luoi (theo ma_quan)
        ("silver", "fact_dan_so_o_nam (theo year, ma_quan)", "cell_id, ma_tram, year, pop, pop60, share60", "số", "Dân và dân 60+ từng ô từng năm 2015–2024 (60+ theo tỷ lệ ô 1 km, A3)", WP, "pop ≥ 0; pop60 ≤ pop", "Thấp"),  # tầng silver: fact_dan_so_o_nam (theo year, ma_quan)
        ("silver", f"o_luoi_tiep_can_{B} (theo ma_quan)", "dist_m, tram_gan_nhat, net_dist_m, net_time_min, net_station, kc_cskcb_m, kc_cskcb_duong_m, tg_cskcb_phut, detour, nhom", "số/chuỗi", "Một dòng / ô có dân năm 2024: khoảng cách chim bay và đường đi tới TYT và CSKCB gần nhất", "b3_bien_doi.py", "net_dist_m ≥ dist_m (bất đẳng thức tam giác)", "Thấp"),  # tầng silver: o_luoi_tiep_can_2024
        ("silver", "dim_tram, dim_cskcb, dim_xa", "toạ độ WGS84 + UTM 49N; nhóm địa bàn (A7)", "số/chuỗi", "Danh mục 56 trạm, 30 CSKCB, 56 xã sau làm sạch", "b3_bien_doi.py", "56 trạm; 30 CSKCB", "Công khai"),  # tầng silver: dim_tram, dim_cskcb, dim_xa
        ("gold", "fact_health_access_yearly (theo year)", "ma_tram, year, dan, ty_le_trong_2/3/5km_pct, kc_trung_vi_m, kc_p95_m, dan_tren_5km…", "số", "KPI tiếp cận theo xã x năm 2015–2024 (giữ 56 trạm, A14)", "b4_tong_hop.py", "tỷ lệ 0–100", "Công khai (số tổng hợp)"),  # tầng gold: fact_health_access_yearly (theo year)
        ("gold", "kpi_tong_hop, kpi_theo_nam, kpi_theo_xa, kpi_theo_nhom, kpi_theo_quan, tram_vung_phuc_vu", "chi_so, gia_tri… / cột KPI", "số/chuỗi", "Bảng KPI năm 2024 và theo năm; bản CSV cùng tên *_spark.csv trong outputs/tables", "b4_tong_hop.py", "khớp bộ số 19/11/2026 (doi_soat_spark.csv)", "Công khai (số tổng hợp)"),  # tầng gold: kpi_tong_hop, kpi_theo_nam, kpi_theo_xa, kpi_theo_nhom, kpi_theo_quan, tram_vung_phuc_vu
        ("gold", f"fact_khoang_cach_o_{B}, fact_khoang_cach_tyt_cskcb", "khoảng cách ô -> cơ sở; ma trận 56 TYT x 30 CSKCB", "số", "Ma trận khoảng cách phục vụ bản đồ", "b3_bien_doi, b4_tong_hop", "≥ 0", "Thấp"),  # tầng gold: ma trận khoảng cách
        ("gold", "vung_lom_tiep_can, fact_optimal_facility_placement, kich_ban_nq1251, tyt_cskcb", "xem CSV cùng nội dung", "số/chuỗi", "Vùng lõm (A8, A18), điểm đặt mới (A10), kịch bản gộp NQ 1251, TYT và CSKCB (A15)", "b4_tong_hop.py", "điểm đề xuất chưa xét quỹ đất; danh sách A15 là danh sách rà soát", "Nội bộ – chưa công bố"),  # tầng gold: vung_lom_tiep_can, fact_optimal_facility_placement, kich_ban_nq1251, tyt_cskcb
        ("gold", "fact_access_forecast_2030", "dan_2030, dan_2030_lo80/hi80, ty_le_3km_2030…", "số", "DỰ BÁO 2030 theo 56 xã cũ (file riêng, không trộn với KPI 2015–2024)", "b4_du_bao.py (A9)", "lo80 ≤ dự báo ≤ hi80", "Thấp"),  # tầng gold: fact_access_forecast_2030
        ("outputs", "dan_cu_luoi_100m.csv", "cell_id, lon, lat, ma_tram, nhom, pop_2015…pop_2024, share60_src, share60_2024, pop60_2024, elev_m, slope_deg", "số/chuỗi", "Lưới dân cư 100 m dạng CSV (mở bằng Excel)", WP + " (A2, A3)", "cùng tập ô với dim_o_luoi", "Thấp"),  # tầng outputs: dan_cu_luoi_100m.csv
        ("outputs", "tiep_can_luoi_100m.csv", "dist_m, dist_m_chi_osm, net_dist_m, net_time_min, net_station, snap_cell_m, detour, kc_cskcb_m, kc_cskcb_duong_m, tg_cskcb_phut", "số", "Khoảng cách từng ô (dist_m_chi_osm giữ tên cũ: chỉ dùng trạm có toạ độ thực – A5)", "b5_ghi.py", "đơn vị m, phút", "Thấp"),  # tầng outputs: tiep_can_luoi_100m.csv
        ("outputs", "qa_official_population.csv", "lech_pct, trong_3pct, trong_20pct_A11, canh_bao", "số/chuỗi", "So WorldPop với số liệu NQ 1251: quy tắc ±3% chỉ CẢNH BÁO, ±20% là ngưỡng A11", "b2_kiem_tra_lam_sach.py", "không cách ly vì WorldPop là ước tính mô hình", "Công khai"),  # tầng outputs: qa_official_population.csv
    ]  # hết danh sách
    td = pd.DataFrame(D, columns=["tang", "tap_du_lieu", "ten_truong", "kieu", "y_nghia", "nguon", "quy_tac_chat_luong", "muc_nhay_cam"])  # bảng từ điển
    td = td[["tap_du_lieu", "ten_truong", "kieu", "y_nghia", "nguon", "quy_tac_chat_luong", "muc_nhay_cam", "tang"]]  # giữ thứ tự cột cũ, cột mới "tang" ở cuối
    save_csv(td, bang("tu_dien_du_lieu.csv"), S, note="tu dien bang Bronze / Silver / Gold va bang ket qua chinh")  # ghi CSV


def chot_so_lieu():  # kiểm tra sổ run_manifest và lập bảng số liệu chốt cho báo cáo
    so = pd.read_csv(RUN_MANIFEST, encoding="utf-8")  # đọc toàn bộ sổ
    con = so.file.map(lambda f: (ROOT / f).exists())  # dòng nào còn file thật
    if (~con).any():  # có dòng trỏ tới file đã bị xoá / chuyển đi (ví dụ kết quả của phiên bản cũ)
        log.info("Bo %d dong so run_manifest tro toi file khong con ton tai: %s", int((~con).sum()), ", ".join(so.file[~con].head(10)))  # in
        so[con].to_csv(RUN_MANIFEST, index=False, encoding="utf-8")  # ghi lại sổ chỉ gồm file còn tồn tại
    man = so[con].drop_duplicates("file", keep="last").set_index("file")  # sổ kết quả, mỗi file một dòng
    rows = []  # kết quả kiểm tra
    for p in sorted(list(TAB.rglob("*.csv")) + list(FIG.glob("*.png"))):  # mọi bảng (trong mọi thư mục bước) và hình
        rel = str(p.relative_to(ROOT))  # đường dẫn tương đối
        if p.name in ("so_lieu_chot.csv", "kiem_tra_run_manifest.csv"):  # hai file do chính bước này ghi
            continue  # bỏ qua
        if rel not in man.index:  # chưa đăng ký
            rows.append(dict(file=rel, co_trong_so=False, sha256_khop=None, script="", created_utc="", trang_thai="CẦN XEM – chưa đăng ký")); continue  # ghi lại
        m = man.loc[rel]  # dòng sổ
        khop = sha256(p) == m.sha256  # SHA-256 còn khớp không
        rows.append(dict(file=rel, co_trong_so=True, sha256_khop=khop, script=m.script, created_utc=m.created_utc, trang_thai="ĐẠT" if khop else "CẦN XEM – file đổi sau khi đăng ký"))  # ghi lại
    kt = pd.DataFrame(rows)  # bảng kiểm tra
    save_csv(kt, bang("kiem_tra_run_manifest.csv"), S, note="doi chieu SHA-256 moi file ket qua voi run_manifest.csv")  # ghi CSV
    log.info("Kiem tra run_manifest: %d file, %d DAT, %d CAN XEM", len(kt), int((kt.trang_thai == "ĐẠT").sum()), int((kt.trang_thai != "ĐẠT").sum()))  # in
    tt_file = dict(zip(kt.file, kt.trang_thai))  # trạng thái theo file
    out = []  # các con số chốt

    def chot(nhom, chi_so, gia_tri, don_vi, file, cach_lay):  # thêm một con số kèm nguồn
        rel = file if file.startswith("data/") else str(bang(file).relative_to(ROOT))  # đường dẫn tương đối của file nguồn
        m = man.loc[rel] if rel in man.index else None  # dòng sổ
        out.append(dict(nhom=nhom, chi_so=chi_so, gia_tri=gia_tri, don_vi=don_vi, file=rel, cach_lay=cach_lay,  # dòng
                        script=m.script if m is not None else "", created_utc=m.created_utc if m is not None else "",  # (tiếp) script, giờ ghi
                        sha256_file=m.sha256 if m is not None else "",  # (tiếp) mã SHA-256 lúc ghi
                        trang_thai_file=tt_file.get(rel, "ĐẠT" if rel.startswith("data/reference/") else "CẦN XEM – chưa đăng ký")))  # (tiếp)
    for _, r in doc("kpi_tong_hop_spark.csv").iterrows():  # mọi KPI tổng hợp
        chot(f"KPI {B} ({r.loai})", r.chi_so, r.gia_tri, r.don_vi, "kpi_tong_hop_spark.csv", f"dòng chi_so = '{r.chi_so}'")  # chốt
    g = doc("greedy_before_after.csv").set_index("K")  # trước/sau thêm điểm
    for K in (0, 5):  # K = 0 và 5
        chot("Thêm trạm", f"Tỷ lệ dân trong 3 km với {K} điểm đề xuất", g.loc[K, "ty_le_3km"], "%", "greedy_before_after.csv", f"K = {K}, cột ty_le_3km")  # chốt
    u = doc("ung_vien_gop_tram.csv")  # ứng viên A15
    chot("Bớt trạm (A15)", "TYT có từ 6 CSKCB trong 2 km (ứng viên rà soát)", len(u), "trạm", "ung_vien_gop_tram.csv", "số dòng")  # chốt
    chot("Bớt trạm (A15)", "…trong đó đã bị NQ 1251 gộp", int(u.xa_bi_gop_nq1251.sum()), "trạm", "ung_vien_gop_tram.csv", "xa_bi_gop_nq1251 = True")  # chốt
    chua = u[~u.xa_bi_gop_nq1251].ten_tram.str.replace("Trạm Y tế ", "").tolist()  # chưa gộp
    chot("Bớt trạm (A15)", "…chưa gộp, cần rà soát: " + ", ".join(chua), len(chua), "trạm", "ung_vien_gop_tram.csv", "xa_bi_gop_nq1251 = False")  # chốt
    st = doc("tram_y_te_56.csv")  # bảng trạm
    for _, r in st[st.kc_tram_tam_dan_cu_m > 3000].iterrows():  # trạm cách tâm dân cư > 3 km
        chot("Đặt sai chỗ", f"Khoảng cách {r.ten_tram} – tâm dân cư xã", r.kc_tram_tam_dan_cu_m, "m", "tram_y_te_56.csv", f"ma_tram = {r.ma_tram}")  # chốt
    x = doc("kpi_theo_xa_spark.csv").sort_values("ty_le_trong_3km_pct").iloc[0]  # xã thấp nhất
    chot("Đặt sai chỗ", f"Xã có tỷ lệ dân trong 3 km thấp nhất: {x.ten_day_du}", round(x.ty_le_trong_3km_pct, 2), "%", "kpi_theo_xa_spark.csv", "min ty_le_trong_3km_pct")  # chốt
    chot("Đặt sai chỗ", f"{x.ten_day_du}: tỷ lệ dân trong 3 km theo đường đi", round(x.ty_le_trong_3km_mang_duong_pct, 2), "%", "kpi_theo_xa_spark.csv", f"ma_tram = {x.ma_tram}")  # chốt
    chot("Đặt sai chỗ", "Trạm nằm ngoài ranh giới xã (lệch nhỏ, do ranh giới GADM – không phải bằng chứng)", int((~st.trong_ranh_gioi_xa).sum()), "trạm",  # chốt
         "tram_y_te_56.csv", f"trong_ranh_gioi_xa = False; xa nhất {st.kc_ngoai_ranh_gioi_m.max():.0f} m")  # (tiếp)
    v = doc("tram_vung_phuc_vu_spark.csv")  # vùng phục vụ
    vmin, vmax = v.loc[v.dan_phuc_vu.idxmin()], v.loc[v.dan_phuc_vu.idxmax()]  # ít nhất, nhiều nhất
    chot("Phân bổ tải", f"Dân vùng phục vụ ít nhất: {vmin.ten_tram}", round(vmin.dan_phuc_vu), "người", "tram_vung_phuc_vu_spark.csv", "min dan_phuc_vu")  # chốt
    chot("Phân bổ tải", f"Dân vùng phục vụ nhiều nhất: {vmax.ten_tram}", round(vmax.dan_phuc_vu), "người", "tram_vung_phuc_vu_spark.csv", "max dan_phuc_vu")  # chốt
    chot("Phân bổ tải", "Trung vị dân vùng phục vụ", round(v.dan_phuc_vu.median()), "người", "tram_vung_phuc_vu_spark.csv", "median dan_phuc_vu")  # chốt
    n = doc("kpi_theo_nam_spark.csv").set_index("year")  # KPI theo năm
    for y in (n.index.min(), n.index.max()):  # năm đầu, năm cuối
        chot("Theo năm", f"Tổng dân 56 xã năm {y}", round(n.loc[y, "dan"]), "người", "kpi_theo_nam_spark.csv", f"year = {y}")  # chốt
        chot("Theo năm", f"Tỷ lệ dân trong 3 km năm {y}", round(n.loc[y, "ty_le_trong_3km_pct"], 2), "%", "kpi_theo_nam_spark.csv", f"year = {y}")  # chốt
    a30 = doc("access_2030_total.csv").iloc[0]  # tiếp cận 2030
    chot("Dự báo 2030", "Tỷ lệ dân trong 3 km năm 2030 (dự báo, giữ 56 trạm)", a30.ty_le_3km_2030, "%", "access_2030_total.csv", "cột ty_le_3km_2030")  # chốt
    f30 = doc("forecast_total_2030.csv").set_index("chi_so").gia_tri  # tổng dự báo
    for k_, ten in (("dan_2030", "Tổng dân 2030 (dự báo)"), ("dan_2030_lo80", "…cận dưới khoảng 80%"), ("dan_2030_hi80", "…cận trên khoảng 80%")):  # ba con số
        chot("Dự báo 2030", ten, round(f30[k_]), "người", "forecast_total_2030.csv", f"chi_so = {k_}")  # chốt
    w = doc("white_zones.csv")  # vùng lõm
    tr = w[w.loai_vung.str.startswith("trang")]  # vùng trắng
    chot("Nguyên nhân", "Cụm dân cách trạm trên 5 km", len(tr), "cụm", "white_zones.csv", "loai_vung = trang_>5km")  # chốt
    chot("Nguyên nhân", "…mang cả địa hình và dân cư phân tán", int(tr.nguyen_nhan.str.contains("Địa hình").sum()), "cụm", "white_zones.csv", "nguyen_nhan chứa 'Địa hình' (A8)")  # chốt
    hv = doc("tyt_cskcb_theo_nhom.csv").set_index("nhom_dia_ban").loc["Huyện Hòa Vang"]  # Hòa Vang
    chot("CSKCB không thay TYT", "Hòa Vang: TYT không có CSKCB nào trong 2 km", f"{int(hv.so_tram_khong_co_cskcb_2km)}/{int(hv.so_tram)}", "trạm", "tyt_cskcb_theo_nhom.csv", "nhom_dia_ban = Huyện Hòa Vang")  # chốt
    chot("CSKCB không thay TYT", "Hòa Vang: khoảng cách trung bình tới CSKCB gần nhất", round(hv.kc_cskcb_gan_nhat_tb_m), "m", "tyt_cskcb_theo_nhom.csv", "kc_cskcb_gan_nhat_tb_m")  # chốt
    sd = doc("spark_so_dong_theo_nguon.csv")  # số dòng
    chot("Chất lượng dữ liệu", "Dòng lỗi dữ liệu (quarantine)", int(sd.so_dong_loi_du_lieu.sum()), "dòng", "spark_so_dong_theo_nguon.csv", "tổng so_dong_loi_du_lieu")  # chốt
    chot("Chất lượng dữ liệu", "Dòng ngoài phạm vi (excluded – không phải lỗi)", int(sd.so_dong_ngoai_pham_vi.sum()), "dòng", "spark_so_dong_theo_nguon.csv", "tổng so_dong_ngoai_pham_vi")  # chốt
    ds = doc("doi_soat_spark.csv")  # đối soát
    chot("Chất lượng dữ liệu", "Phép đối soát đạt", f"{int((ds.ket_qua == 'ĐẠT').sum())}/{len(ds)}", "phép", "doi_soat_spark.csv", "ket_qua = ĐẠT")  # chốt
    so = pd.DataFrame(out)  # bảng số liệu chốt
    save_csv(so, bang("so_lieu_chot.csv"), S, note="con so chot cho bao cao kem file nguon va SHA-256")  # ghi CSV
    log.info("Chot %d con so tu %d file; file nguon CAN XEM: %d", len(so), so.file.nunique(), int((so.trang_thai_file != "ĐẠT").sum()))  # in


if __name__ == "__main__":  # chạy trực tiếp
    main()  # gọi hàm chính
