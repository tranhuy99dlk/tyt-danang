# -*- coding: utf-8 -*-
"""08 - Du bao dan so va dan 60+ theo xa den 2030; xa co ty le tiep can giam.

CANH BAO PHUONG PHAP (#Truthmode): chuoi 2015-2030 cua WorldPop R2025A la UOC LUONG MO HINH
(noi suy giua cac moc dieu tra, sau do chieu toi 2030). Backtest duoi day do kha nang mo hinh
TAI TAO chuoi WorldPop, khong phai do sai so so voi dan so thuc. WorldPop 2030 chi la moc doi chieu.

Mo hinh: baseline Naive (giu nam cuoi), Drift (keo dai xu huong dau-cuoi), Tuyen tinh OLS,
         Holt tat dan (ETS A,Ad,N - mo hinh chuan).
Backtest 2 goc: (a) hoc 2015-2021, kiem tra 2022-2024 (h=1..3); (b) hoc 2015-2019, kiem tra 2020-2024 (h=1..5).
Du bao cuoi: hoc 2015-2025, du bao 2026-2030, khoang 80% tu ETS.
Dau ra: forecast_backtest.csv, forecast_commune_2030.csv, access_2030_change.csv, h5, h6.
"""
import json
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from statsmodels.tsa.exponential_smoothing.ets import ETSModel

from common import INTERIM, TAB, FIG, BASE_YEAR, get_logger, save_csv, register

warnings.filterwarnings("ignore")
S = "08_forecast"
log = get_logger(S)
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d9d8d3"
C1, C2 = "#2a78d6", "#eb6834"
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9})


def f_naive(y, h):
    return np.repeat(y[-1], h)


def f_drift(y, h):
    return y[-1] + (y[-1] - y[0]) / (len(y) - 1) * np.arange(1, h + 1)


def f_lin(y, h):
    t = np.arange(len(y))
    b = np.polyfit(t, y, 1)
    return np.polyval(b, np.arange(len(y), len(y) + h))


def f_ets(y, h, pi=False):
    try:
        m = ETSModel(pd.Series(y, dtype=float), error="add", trend="add", damped_trend=True).fit(disp=False)
        if pi:
            fr = m.get_prediction(start=len(y), end=len(y) + h - 1).summary_frame(alpha=0.2)
            return fr["mean"].values, fr["pi_lower"].values, fr["pi_upper"].values
        return m.forecast(h).values
    except Exception:
        out = f_drift(y, h)
        return (out, out, out) if pi else out


MODELS = {"Naive (baseline)": f_naive, "Drift (baseline)": f_drift, "Tuyến tính OLS": f_lin, "Holt tắt dần ETS(A,Ad,N)": f_ets}


def backtest(cs, col):
    rows = []
    for tr_end, te in ((2021, [2022, 2023, 2024]), (2019, [2020, 2021, 2022, 2023, 2024])):
        for ma, d in cs.groupby("ma_tram"):
            d = d.set_index("year")[col]
            y = d.loc[2015:tr_end].values
            act = d.loc[te].values
            for name, f in MODELS.items():
                fc = f(y, len(te))
                for h, (a, p) in enumerate(zip(act, fc), 1):
                    rows.append(dict(goc=f"hoc 2015-{tr_end}", ma_tram=ma, mo_hinh=name, h=h, ape=abs(p - a) / a * 100 if a else np.nan))
    bt = pd.DataFrame(rows)
    summ = bt.groupby(["goc", "mo_hinh"]).ape.agg(MAPE_pct="mean", APE_trung_vi_pct="median", APE_p90_pct=lambda s: s.quantile(.9)).reset_index()
    return summ.round(3)


def main():
    cs = pd.read_csv(TAB / "commune_series.csv", encoding="utf-8-sig")
    comm = pd.DataFrame([f["properties"] for f in json.load(open(INTERIM / "communes_56.geojson", encoding="utf-8"))["features"]])
    bts = []
    for col, lab in (("pop_ds", "Dân số"), ("pop60_ds", "Dân 60+")):
        b = backtest(cs, col)
        b.insert(0, "bien", lab)
        bts.append(b)
    bt = pd.concat(bts)
    save_csv(bt, TAB / "forecast_backtest.csv", S)
    for _, r in bt.iterrows():
        log.info("Backtest %s | %s | %s: MAPE %.3f%%", r.bien, r.goc, r.mo_hinh, r.MAPE_pct)
    # chon mo hinh: MAPE trung binh 2 goc nho nhat (theo tung bien). Neu baseline thang -> dung baseline (#Truthmode)
    choice = bt.groupby(["bien", "mo_hinh"]).MAPE_pct.mean().reset_index().sort_values("MAPE_pct").groupby("bien").first()
    log.info("Mo hinh duoc chon: %s", choice.mo_hinh.to_dict())
    save_csv(choice.reset_index(), TAB / "forecast_model_choice.csv", S)
    # khoang 80% thuc nghiem: phan vi 80 cua APE o h=5 (goc hoc 2015-2019) cua mo hinh duoc chon
    bt_raw = {}
    for col, lab in (("pop_ds", "Dân số"), ("pop60_ds", "Dân 60+")):
        rows_ = []
        for ma, d in cs.groupby("ma_tram"):
            d = d.set_index("year")[col]
            fcv = MODELS[choice.loc[lab, "mo_hinh"]](d.loc[2015:2019].values, 5)
            rows_.append(abs(fcv[-1] - d.loc[2024]) / d.loc[2024] * 100 if d.loc[2024] else np.nan)
        bt_raw[lab] = float(np.nanpercentile(rows_, 80))
    log.info("Bien do khoang 80%% (APE p80, h=5): %s", bt_raw)

    rows = []
    for ma, d in cs.groupby("ma_tram"):
        d = d.set_index("year")
        r = {"ma_tram": ma}
        for col, key, lab in (("pop_ds", "dan", "Dân số"), ("pop60_ds", "dan60", "Dân 60+")):
            y = d.loc[2015:BASE_YEAR, col].values
            h = 2030 - BASE_YEAR
            mean_ets = f_ets(y, h)[-1]
            mean_dr = f_drift(y, h)[-1]
            chosen = MODELS[choice.loc[lab, "mo_hinh"]](y, h)[-1]
            band = bt_raw[lab] / 100
            r[f"{key}_{BASE_YEAR}"] = y[-1]
            r[f"{key}_2030"] = chosen
            r[f"{key}_2030_lo80"], r[f"{key}_2030_hi80"] = chosen * (1 - band), chosen * (1 + band)
            r[f"{key}_2030_ets"] = mean_ets
            r[f"{key}_2030_drift"] = mean_dr
            r[f"{key}_2030_worldpop"] = d.loc[2030, col] if 2030 in d.index else np.nan
        rows.append(r)
    fc = pd.DataFrame(rows)
    fc = comm[["ma_tram", "ten_day_du", "quan_huyen", "loai_dv"]].merge(fc, on="ma_tram")
    fc["tang_dan_pct"] = 100 * (fc.dan_2030 / fc[f"dan_{BASE_YEAR}"] - 1)
    fc["tang_60_pct"] = 100 * (fc.dan60_2030 / fc[f"dan60_{BASE_YEAR}"] - 1)
    fc["lech_du_bao_vs_worldpop_pct"] = 100 * (fc.dan_2030 / fc.dan_2030_worldpop - 1)
    num = [c for c in fc.columns if c.startswith(("dan", "tang", "lech"))]
    fc[num] = fc[num].round(1)
    save_csv(fc, TAB / "forecast_commune_2030.csv", S)
    T = fc[[f"dan_{BASE_YEAR}", "dan_2030", "dan_2030_lo80", "dan_2030_hi80", "dan_2030_ets", "dan_2030_drift", "dan_2030_worldpop",
            f"dan60_{BASE_YEAR}", "dan60_2030", "dan60_2030_lo80", "dan60_2030_hi80", "dan60_2030_ets", "dan60_2030_drift", "dan60_2030_worldpop"]].sum()
    log.info("Tong 56 xa: dan %s -> 2030 %.0f (80%%: %.0f-%.0f; ETS %.0f; WorldPop %.0f); 60+ %.0f -> %.0f (WorldPop %.0f)",
             BASE_YEAR, T.dan_2030, T.dan_2030_lo80, T.dan_2030_hi80, T.dan_2030_ets, T.dan_2030_worldpop, T[f"dan60_{BASE_YEAR}"], T.dan60_2030, T.dan60_2030_worldpop)
    save_csv(T.round(0).rename("gia_tri").reset_index().rename(columns={"index": "chi_so"}), TAB / "forecast_total_2030.csv", S)

    # ---- tiep can 2030: phan bo trong xa theo nhip tang o 1 km (A4), tong xa = du bao ETS (A9) ----
    g = pd.read_parquet(INTERIM / "grid_access.parquet")
    panel = pd.read_parquet(INTERIM / "grid_panel_2015_2030.parquet", columns=["cell_id", "year", "pop_ds", "pop60_ds"])
    p30 = panel[panel.year == 2030].set_index("cell_id")
    g["w30"] = g.cell_id.map(p30.pop_ds)
    g["w60_30"] = g.cell_id.map(p30.pop60_ds)
    for wcol, tcol, out in (("w30", "dan_2030", "pop_2030"), ("w60_30", "dan60_2030", "pop60_2030")):
        s = g.groupby("ma_tram")[wcol].transform("sum")
        g[out] = g[wcol] / s * g.ma_tram.map(fc.set_index("ma_tram")[tcol])
    rows = []
    for ma, d in g.groupby("ma_tram"):
        within = d.dist_m <= 3000
        rows.append(dict(ma_tram=ma,
                         ty_le_3km_2025=100 * d.loc[within, f"pop_{BASE_YEAR}"].sum() / d[f"pop_{BASE_YEAR}"].sum(),
                         ty_le_3km_2030=100 * d.loc[within, "pop_2030"].sum() / d.pop_2030.sum(),
                         ty_le60_3km_2025=100 * d.loc[within, f"pop60_{BASE_YEAR}"].sum() / d[f"pop60_{BASE_YEAR}"].sum(),
                         ty_le60_3km_2030=100 * d.loc[within, "pop60_2030"].sum() / d.pop60_2030.sum(),
                         dan_ngoai_3km_2025=d.loc[~within, f"pop_{BASE_YEAR}"].sum(), dan_ngoai_3km_2030=d.loc[~within, "pop_2030"].sum(),
                         dan_tren_5km_2025=d.loc[d.dist_m > 5000, f"pop_{BASE_YEAR}"].sum(), dan_tren_5km_2030=d.loc[d.dist_m > 5000, "pop_2030"].sum(),
                         dan60_ngoai_3km_2030=d.loc[~within, "pop60_2030"].sum()))
    ac = pd.DataFrame(rows)
    ac["thay_doi_pp"] = ac.ty_le_3km_2030 - ac.ty_le_3km_2025
    ac["thay_doi_60_pp"] = ac.ty_le60_3km_2030 - ac.ty_le60_3km_2025
    ac = comm[["ma_tram", "ten_day_du", "quan_huyen", "loai_dv"]].merge(ac, on="ma_tram").round(2)
    ac["giam_tiep_can"] = ac.thay_doi_pp < -0.05
    save_csv(ac, TAB / "access_2030_change.csv", S)
    tot = dict(ty_le_3km_2025=100 * g.loc[g.dist_m <= 3000, f"pop_{BASE_YEAR}"].sum() / g[f"pop_{BASE_YEAR}"].sum(),
               ty_le_3km_2030=100 * g.loc[g.dist_m <= 3000, "pop_2030"].sum() / g.pop_2030.sum(),
               ty_le60_3km_2030=100 * g.loc[g.dist_m <= 3000, "pop60_2030"].sum() / g.pop60_2030.sum(),
               dan_tren_5km_2030=g.loc[g.dist_m > 5000, "pop_2030"].sum(), dan60_tren_5km_2030=g.loc[g.dist_m > 5000, "pop60_2030"].sum(),
               so_xa_giam=int(ac.giam_tiep_can.sum()))
    save_csv(pd.DataFrame([tot]).round(2), TAB / "access_2030_total.csv", S)
    log.info("Tiep can 3 km toan TP: 2025 %.2f%% -> 2030 %.2f%%; so xa giam: %d", tot["ty_le_3km_2025"], tot["ty_le_3km_2030"], tot["so_xa_giam"])
    gp = INTERIM / "grid_2030.parquet"
    g[["cell_id", "ma_tram", "x_m", "y_m", "pop_2030", "pop60_2030"]].to_parquet(gp, index=False)
    register(gp, S, rows=len(g))

    # ---- hinh 5: tong dan va 60+ toan 56 xa ----
    tt = cs.groupby("year")[["pop_ds", "pop60_ds"]].sum()
    fig, axs = plt.subplots(1, 2, figsize=(7.2, 3.0), dpi=200)
    for ax, col, key, c, ttl in ((axs[0], "pop_ds", "dan", C1, "Dân số 56 xã/phường"), (axs[1], "pop60_ds", "dan60", C2, "Dân 60+")):
        hist = tt.loc[2015:BASE_YEAR, col]
        wp = tt.loc[BASE_YEAR:2030, col]
        ax.plot(hist.index, hist.values / 1e3, color=c, lw=2, label="WorldPop 2015–2025")
        ax.plot(wp.index, wp.values / 1e3, color=c, lw=1.2, ls=":", label="WorldPop chiếu 2026–2030")
        yrs = np.arange(BASE_YEAR, 2031)
        y = hist.values
        mean, lo, hi = f_ets(y, 2030 - BASE_YEAR, pi=True)
        ax.plot(yrs, np.r_[y[-1], mean] / 1e3, color=INK, lw=1.5, ls="--", label="ETS(A,Ad,N) – tổng TP (khoảng 80% ETS)")
        ax.fill_between(yrs[1:], lo / 1e3, hi / 1e3, color=INK, alpha=.12, lw=0)
        sel = fc[f"{key}_2030"].sum()
        ax.scatter([2030], [sel / 1e3], s=24, color=c, edgecolor="white", zorder=5, label="Tổng dự báo theo xã (mô hình được chọn)")
        ax.set_title(ttl + " (nghìn người)", loc="left", fontsize=9, color=INK)
        ax.grid(color=GRID, lw=.5)
        for s_ in ("top", "right"):
            ax.spines[s_].set_visible(False)
    axs[0].legend(frameon=False, fontsize=6.5)
    fig.text(0.01, 0.005, "Nguồn: WorldPop R2025A (mô hình). File: outputs/tables/commune_series.csv, forecast_total_2030.csv", fontsize=6, color=INK2)
    fig.tight_layout()
    f5 = FIG / "h5_du_bao_dan_so.png"
    fig.savefig(f5); plt.close(fig); register(f5, S)

    # ---- hinh 6: xa giam tiep can ----
    sub = ac[ac.giam_tiep_can].sort_values("thay_doi_pp")
    if len(sub):
        fig, ax = plt.subplots(figsize=(7.2, 0.35 * len(sub) + 1.2), dpi=200)
        y = np.arange(len(sub))
        ax.barh(y, sub.thay_doi_pp, color=C2, height=.6)
        for yy, v, a, b in zip(y, sub.thay_doi_pp, sub.ty_le_3km_2025, sub.ty_le_3km_2030):
            ax.text(v - .02, yy, f"{a:.1f}% → {b:.1f}%", va="center", ha="right", fontsize=7, color=INK)
        ax.set_yticks(y, sub.ten_day_du, fontsize=7.5)
        ax.axvline(0, color=INK2, lw=.6)
        ax.set_xlabel("Thay đổi tỷ lệ dân trong 3 km, 2025→2030 (điểm %)")
        ax.set_xlim(min(sub.thay_doi_pp.min() * 1.8, -0.5), 0.1)
        ax.grid(axis="x", color=GRID, lw=.5)
        for s_ in ("top", "right"):
            ax.spines[s_].set_visible(False)
        ax.set_title(f"Xã/phường có tỷ lệ tiếp cận 3 km giảm đến 2030 ({len(sub)})", loc="left", fontsize=10, color=INK)
        fig.text(0.01, 0.005, "Giả định A4, A9. File: outputs/tables/access_2030_change.csv", fontsize=6, color=INK2)
        fig.tight_layout()
        f6 = FIG / "h6_xa_giam_tiep_can_2030.png"
        fig.savefig(f6); plt.close(fig); register(f6, S)


if __name__ == "__main__":
    # NGOAI PHAM VI tu 16/11/2026: du an chi phan tich 2015-2024, khong du bao 2030.
    raise SystemExit("08_forecast.py da ngung dung: du an khong du bao (pham vi 2015-2024).")
