# -*- coding: utf-8 -*-
"""BƯỚC 4 · TỔNG HỢP – phần 4.4 DỰ BÁO 2030 (Bước 08 của luồng dữ liệu), gọi từ b4_tong_hop.py.

Dự báo xử lý riêng, ghi ra bảng riêng, KHÔNG trộn vào KPI các năm phân tích:
  - Chuỗi học 2015–2024 theo xã: hạ cấp từ lưới 1 km giữ phân bố ô 100 m năm 2024 (A4) để chuỗi nhất quán.
  - 4 mô hình: Naive, Drift (hai mô hình cơ sở), tuyến tính OLS, Holt tắt dần ETS(A,Ad,N). Mỗi xã được khớp mô hình
    SONG SONG trên các executor bằng groupBy("ma_tram").applyInPandas(...) – vì vậy các hàm mô hình nằm ở cấp module
    (Spark gửi được file này tới executor; trên YARN file được gửi kèm bằng addPyFile).
  - Backtest 2 gốc (học 2015–2021 kiểm 2022–2024; học 2015–2019 kiểm 2020–2024). Mô hình có MAPE trung bình nhỏ nhất
    được chọn; nếu mô hình cơ sở thắng thì dùng mô hình cơ sở (#Truthmode). Backtest chỉ đo khả năng TÁI TẠO chuỗi
    WorldPop, không đo sai số so với dân số thật.
  - Khoảng 80% = phân vị 80 của sai số backtest ở tầm 5 năm (dự báo xa 6 năm nên có thể hơi hẹp – A9).
  - Tiếp cận 2030: giữ nguyên 56 trạm; trong mỗi xã, dân 2030 phân bố theo nhịp tăng ô 1 km của số chiếu WorldPop
    2030 (A9). Chỉ tính cho 56 xã cũ (người dùng chốt 26/11/2026).
"""
import warnings  # tắt cảnh báo hội tụ của statsmodels

import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu

from common import BASE_YEAR, YEARS, PROJ_YEARS, FORECAST_YEAR, MAIN_KM, WHITE_KM  # tham số dùng chung

S = "b4_du_bao"  # tên phần ghi trong sổ run_manifest
B, FY = BASE_YEAR, FORECAST_YEAR  # năm hiện trạng, năm đích dự báo
R, RW = MAIN_KM * 1000, WHITE_KM * 1000  # 3 km, 5 km (mét)


# ======================= MÔ HÌNH DỰ BÁO (cấp module để Spark gửi được tới executor) =======================
def f_naive(y, h):  # mô hình cơ sở Naive: giữ nguyên giá trị năm cuối
    return np.repeat(y[-1], h)  # lặp lại h lần


def f_drift(y, h):  # mô hình cơ sở Drift: kéo dài đường nối điểm đầu – điểm cuối
    return y[-1] + (y[-1] - y[0]) / (len(y) - 1) * np.arange(1, h + 1)  # năm cuối + độ dốc trung bình x số năm


def f_lin(y, h):  # hồi quy tuyến tính theo thời gian (OLS)
    t = np.arange(len(y))  # trục thời gian 0..n-1
    b = np.polyfit(t, y, 1)  # hệ số đường thẳng bình phương tối thiểu
    return np.polyval(b, np.arange(len(y), len(y) + h))  # giá trị đường thẳng ở các năm tương lai


def f_ets(y, h):  # Holt tắt dần ETS(A,Ad,N) – mô hình chuẩn
    from statsmodels.tsa.exponential_smoothing.ets import ETSModel  # nhập tại chỗ (chạy trên executor)
    warnings.filterwarnings("ignore")  # bỏ cảnh báo hội tụ
    try:  # thử khớp mô hình
        m = ETSModel(pd.Series(y, dtype=float), error="add", trend="add", damped_trend=True).fit(disp=False)  # sai số cộng, xu hướng cộng tắt dần
        return m.forecast(h).values  # dự báo h bước
    except Exception:  # không hội tụ
        return f_drift(y, h)  # dùng Drift thay thế


MO_HINH = {"Naive (baseline)": f_naive, "Drift (baseline)": f_drift, "Tuyến tính OLS": f_lin, "Holt tắt dần ETS(A,Ad,N)": f_ets}  # bảng mô hình
BIEN = (("pop_ds", "dan", "Dân số"), ("pop60_ds", "dan60", "Dân 60+"))  # (cột chuỗi, tiền tố đầu ra, nhãn)
# Bộ tối ưu của ETS rất nhạy: chuỗi chỉ lệch 1e-10 người (do Spark cộng theo thứ tự khác pandas) đã làm dự báo
# của vài xã đổi tới vài trăm người. Làm tròn chuỗi học tới 10^-6 người trước khi khớp để kết quả ổn định, lặp lại được.
LAM_TRON = 6  # số chữ số thập phân giữ lại của chuỗi học


def backtest_mot_xa(pdf):  # chạy trên executor cho MỘT xã: trả sai số tuyệt đối % của mọi (biến, gốc, mô hình, tầm)
    d = pdf.set_index("year").sort_index()  # chuỗi của xã theo năm
    out = []  # các dòng kết quả
    for cot, _, nhan in BIEN:  # dân số và dân 60+
        for hoc_den, kiem in ((2021, [2022, 2023, 2024]), (2019, [2020, 2021, 2022, 2023, 2024])):  # hai gốc backtest
            y = np.round(d.loc[YEARS[0]:hoc_den, cot].values, LAM_TRON)  # phần học, làm tròn để kết quả không phụ thuộc thứ tự cộng
            thuc = np.round(d.loc[kiem, cot].values, LAM_TRON)  # phần kiểm
            for ten, f in MO_HINH.items():  # từng mô hình
                du = f(y, len(kiem))  # dự báo
                for h, (a, p) in enumerate(zip(thuc, du), 1):  # từng tầm dự báo
                    out.append((nhan, f"hoc {YEARS[0]}-{hoc_den}", str(pdf.ma_tram.iloc[0]), ten, h, abs(p - a) / a * 100 if a else np.nan))  # sai số %
    return pd.DataFrame(out, columns=["bien", "goc", "ma_tram", "mo_hinh", "h", "ape"])  # bảng kết quả của xã


def tao_ham_du_bao(chon: dict, bien_do: dict):  # tạo hàm dự báo cuối cho từng xã (đóng gói mô hình đã chọn và biên độ 80%)
    def du_bao_mot_xa(pdf):  # chạy trên executor cho MỘT xã
        d = pdf.set_index("year").sort_index()  # chuỗi theo năm
        r = {"ma_tram": str(pdf.ma_tram.iloc[0])}  # dòng kết quả
        for cot, k, nhan in BIEN:  # dân số và dân 60+
            y = np.round(d.loc[YEARS[0]:B, cot].values, LAM_TRON)  # chuỗi học 2015–2024, làm tròn tới 10^-6 người
            h = FY - B  # tầm dự báo 6 năm
            p = MO_HINH[chon[nhan]](y, h)[-1]  # dự báo của mô hình được chọn cho năm 2030
            r[f"{k}_{B}"] = y[-1]  # giá trị năm cuối
            r[f"{k}_2030"] = p  # dự báo
            r[f"{k}_2030_lo80"], r[f"{k}_2030_hi80"] = p * (1 - bien_do[nhan] / 100), p * (1 + bien_do[nhan] / 100)  # khoảng 80%
            r[f"{k}_2030_ets"] = f_ets(y, h)[-1]  # dự báo ETS để đối chiếu
            r[f"{k}_2030_drift"] = f_drift(y, h)[-1]  # dự báo Drift để đối chiếu
        return pd.DataFrame([r])  # một dòng
    return du_bao_mot_xa  # trả hàm


def chay(nc, d3, g):  # chạy phần 4.4; d3 = kết quả Bước 3 (Silver), g = mọi ô có dân kèm khoảng cách (pandas, từ 4.3)
    F, log = nc.F, nc.log  # hàm cột Spark, bộ ghi log
    o_dan, fact, k1, xa = d3["o_dan"], d3["fact"], d3["k1"], d3["xa"]  # bảng Silver từ Bước 3

    # ================= 1. CHUỖI THEO XÃ (A4) VÀ SỐ CHIẾU WORLDPOP =================
    k1w = (k1.groupBy("year", "k1_row", "k1_col")  # lưới 1 km theo năm (2015–2030)
           .agg(F.sum(F.when(F.col("bien") == "pop", F.col("gia_tri"))).alias("k"),  # tổng dân ô 1 km
                F.sum(F.when(F.col("bien") != "pop", F.col("gia_tri"))).alias("k60")))  # dân 60+ ô 1 km
    kB = k1w.filter(F.col("year") == B).select("k1_row", "k1_col", F.col("k").alias("kB"))  # tổng dân ô 1 km năm 2024
    goc = (o_dan.select("cell_id", "ma_tram", "k1_row", "k1_col")  # mọi ô có dân
           .join(fact.filter(F.col("year") == B).select("cell_id", F.col("pop").alias("pB"), F.col("share60").alias("sB")), "cell_id", "left")  # dân và tỷ lệ 60+ năm 2024
           .join(kB, ["k1_row", "k1_col"], "left").fillna(0, ["pB", "sB", "kB"]))  # gắn ô 1 km năm 2024; thiếu -> 0
    nam = nc.tao_df(pd.DataFrame({"year": YEARS + PROJ_YEARS}))  # bảng các năm 2015–2030
    ds = (goc.crossJoin(F.broadcast(nam))  # mỗi ô x mỗi năm
          .join(k1w, ["year", "k1_row", "k1_col"], "left").fillna(0, ["k", "k60"])  # gắn ô 1 km của năm đó
          .withColumn("pop_ds", F.col("pB") * F.when(F.col("kB") > 0, F.col("k") / F.col("kB")).otherwise(1.0))  # dân hạ cấp = dân 2024 x nhịp tăng ô 1 km
          .withColumn("pop60_ds", F.when(F.col("kB") > 0, F.col("pB") * F.col("k60") / F.col("kB"))  # dân 60+ hạ cấp
                      .otherwise(F.col("pop_ds") * F.col("sB"))))  # ô 1 km năm 2024 = 0 -> dùng tỷ lệ 60+ của ô
    xa_ds = ds.groupBy("ma_tram", "year").agg(F.sum("pop_ds").alias("pop_ds"), F.sum("pop60_ds").alias("pop60_ds"))  # tổng theo xã x năm
    truc = fact.groupBy("ma_tram", "year").agg(F.sum("pop").alias("pop"), F.sum("pop60").alias("pop60"))  # dân trực tiếp từ lưới 100 m
    cs = (truc.join(xa_ds, ["ma_tram", "year"]).withColumn("nguon", F.lit("100m_truc_tiep"))  # chuỗi theo xã 2015–2024
          .withColumn("ty_le_60_ds", F.col("pop60_ds") / F.col("pop_ds")).orderBy("ma_tram", "year").toPandas())  # tỷ lệ 60+ -> pandas
    cs = cs[["ma_tram", "year", "pop", "pop60", "pop_ds", "pop60_ds", "nguon", "ty_le_60_ds"]]  # thứ tự cột cũ
    nc.xuat("commune_series.csv", cs, S, note="chuoi theo xa 2015-2024: truc tiep 100 m va ha cap 1 km (A4)")  # vào hàng chờ (Bước 5 ghi)
    kd = []  # kiểm định A4: hạ cấp so với trực tiếp
    for y in YEARS[:-1]:  # các năm trừ năm gốc 2024
        c = cs[cs.year == y]  # dữ liệu năm y
        ape = (c.pop_ds - c["pop"]).abs() / c["pop"]  # sai số tương đối từng xã
        kd.append(dict(nam=y, mape_xa_pct=round(100 * ape.mean(), 2), ape_max_pct=round(100 * ape.max(), 2),  # dòng kiểm định
                       tong_truc_tiep=round(c["pop"].sum()), tong_ha_cap=round(c.pop_ds.sum())))  # (tiếp)
        nc.qa_them("Lệch tổng", f"Hạ cấp 1 km→100 m năm {y}: MAPE theo xã (%)", round(100 * ape.mean(), 2), 100 * ape.mean() < 5)  # QA
    nc.xuat("validation_downscale_A4.csv", pd.DataFrame(kd), S, note="kiem dinh A4: chuoi ha cap vs truc tiep 100 m")  # vào hàng chờ (Bước 5 ghi)
    nam_du = cs.groupby("ma_tram").year.nunique()  # số năm có dữ liệu của từng xã
    nc.qa_them("Thiếu", f"Xã đủ {len(YEARS)} năm trong chuỗi ({YEARS[0]}–{YEARS[-1]})", int((nam_du == len(YEARS)).sum()), bool((nam_du == len(YEARS)).all()))  # QA
    chieu = ds.filter(F.col("year").isin(PROJ_YEARS)).groupBy("year").agg(F.sum("pop_ds").alias("dan"), F.sum("pop60_ds").alias("dan60")).orderBy("year").toPandas()  # tổng số chiếu
    nc.xuat("forecast_worldpop_chieu.csv", chieu, S, note="tong so CHIEU WorldPop 2025-2030 ha cap ve 56 xa (chi de doi chieu)")  # vào hàng chờ (Bước 5 ghi)
    o30 = ds.filter(F.col("year") == FY).select("cell_id", "ma_tram", F.col("pop_ds").alias("w30"), F.col("pop60_ds").alias("w60_30")).toPandas()  # số chiếu 2030 từng ô
    wp_xa = o30.groupby("ma_tram")[["w30", "w60_30"]].sum()  # số chiếu 2030 theo xã

    # ================= 2. BACKTEST SONG SONG THEO XÃ (applyInPandas) =================
    chuoi = nc.tao_df(cs[["ma_tram", "year", "pop_ds", "pop60_ds"]])  # chuỗi học lên Spark
    sch_bt = "bien string, goc string, ma_tram string, mo_hinh string, h int, ape double"  # schema đầu ra của backtest
    bt_raw = chuoi.groupBy("ma_tram").applyInPandas(backtest_mot_xa, schema=sch_bt).toPandas()  # mỗi xã một tác vụ song song
    bt = (bt_raw.groupby(["bien", "goc", "mo_hinh"]).ape.agg(MAPE_pct="mean", APE_trung_vi_pct="median",  # tổng hợp sai số
                                                             APE_p90_pct=lambda s: s.quantile(.9)).reset_index().round(3))  # (tiếp)
    nc.xuat("forecast_backtest.csv", bt, S, note="backtest chi do kha nang tai tao chuoi WorldPop (#Truthmode)")  # vào hàng chờ (Bước 5 ghi)
    chon = bt.groupby(["bien", "mo_hinh"]).MAPE_pct.mean().reset_index().sort_values("MAPE_pct").groupby("bien").first()  # mô hình MAPE nhỏ nhất mỗi biến
    nc.xuat("forecast_model_choice.csv", chon.reset_index(), S, note="mo hinh co MAPE trung binh 2 goc nho nhat")  # vào hàng chờ (Bước 5 ghi)
    chon_d = chon.mo_hinh.to_dict()  # nhãn biến -> tên mô hình
    bien_do = {nhan: float(np.nanpercentile(bt_raw[(bt_raw.bien == nhan) & (bt_raw.goc == f"hoc {YEARS[0]}-2019") & (bt_raw.h == 5)  # p80 sai số tầm 5 năm
                                                   & (bt_raw.mo_hinh == chon_d[nhan])].ape, 80)) for _, _, nhan in BIEN}  # (tiếp) của mô hình được chọn
    nc.bien_do = bien_do  # nhớ biên độ 80% để đối soát dự báo với bộ số cũ
    log.info("4. Mo hinh duoc chon: %s; bien do khoang 80%%: %s", chon_d, {k: round(v, 3) for k, v in bien_do.items()})  # in

    # ================= 3. DỰ BÁO 2030 THEO XÃ =================
    sch_fc = ", ".join(["ma_tram string"] + [f"{k}_{s} double" for _, k, _ in BIEN for s in (B, "2030", "2030_lo80", "2030_hi80", "2030_ets", "2030_drift")])  # schema đầu ra
    fc = chuoi.groupBy("ma_tram").applyInPandas(tao_ham_du_bao(chon_d, bien_do), schema=sch_fc).toPandas()  # dự báo song song theo xã
    fc["dan_2030_worldpop"] = fc.ma_tram.map(wp_xa.w30)  # số chiếu WorldPop 2030 (chỉ để đối chiếu)
    fc["dan60_2030_worldpop"] = fc.ma_tram.map(wp_xa.w60_30)  # số chiếu 60+
    tt = xa.select("ma_tram", "ten_day_du", "quan_huyen", "loai_dv").toPandas()  # thông tin xã
    fc = tt.merge(fc, on="ma_tram").sort_values("ma_tram")  # gắn thông tin xã
    fc["tang_dan_pct"] = 100 * (fc.dan_2030 / fc[f"dan_{B}"] - 1)  # % tăng dân
    fc["tang_60_pct"] = 100 * (fc.dan60_2030 / fc[f"dan60_{B}"] - 1)  # % tăng dân 60+
    fc["lech_du_bao_vs_worldpop_pct"] = 100 * (fc.dan_2030 / fc.dan_2030_worldpop - 1)  # lệch so với số chiếu
    cot_fc = (["ma_tram", "ten_day_du", "quan_huyen", "loai_dv"]  # thứ tự cột cũ: thông tin xã
              + [f"{k}_{s}" for _, k, _ in BIEN for s in (B, "2030", "2030_lo80", "2030_hi80", "2030_ets", "2030_drift", "2030_worldpop")]  # các cột dự báo
              + ["tang_dan_pct", "tang_60_pct", "lech_du_bao_vs_worldpop_pct"])  # các cột tỷ lệ
    fc = fc[cot_fc]  # sắp cột
    so = [c for c in fc.columns if c.startswith(("dan", "tang", "lech"))]  # các cột số
    fc[so] = fc[so].round(1)  # làm tròn 1 chữ số
    nc.xuat("forecast_commune_2030.csv", fc, S, note="DU BAO 2030 theo 56 xa cu (A9) - khong phai so do")  # vào hàng chờ (Bước 5 ghi)
    tong = fc[[c for c in cot_fc if c.startswith(("dan_", "dan60_"))]].sum()  # tổng 56 xã
    nc.xuat("forecast_total_2030.csv", tong.round(0).rename("gia_tri").reset_index().rename(columns={"index": "chi_so"}), S, note="tong du bao 56 xa")  # vào hàng chờ (Bước 5 ghi)
    log.info("4. Tong 56 xa: dan %d %.0f -> 2030 %.0f (80%%: %.0f-%.0f; WorldPop chieu %.0f)", B, tong[f"dan_{B}"], tong.dan_2030,  # in
             tong.dan_2030_lo80, tong.dan_2030_hi80, tong.dan_2030_worldpop)  # (tiếp)

    # ================= 4. TIẾP CẬN 2030 (giữ 56 trạm, A9) =================
    gg = g.merge(o30[["cell_id", "w30", "w60_30"]], on="cell_id", how="left").fillna({"w30": 0, "w60_30": 0})  # ô + số chiếu 2030
    for wc, tc, out in (("w30", "dan_2030", "pop_2030"), ("w60_30", "dan60_2030", "pop60_2030")):  # dân và dân 60+
        s_ = gg.groupby("ma_tram")[wc].transform("sum")  # tổng số chiếu của xã
        gg[out] = gg[wc] / s_ * gg.ma_tram.map(fc.set_index("ma_tram")[tc])  # phân bổ tổng dự báo của xã theo tỷ trọng ô
    dong = []  # từng xã
    for ma, d in gg.groupby("ma_tram"):  # duyệt xã
        trong = d.dist_m <= R  # ô trong 3 km
        dong.append({"ma_tram": ma, f"ty_le_3km_{B}": 100 * d.loc[trong, "pop"].sum() / d["pop"].sum(),  # % trong 3 km năm 2024
                     "ty_le_3km_2030": 100 * d.loc[trong, "pop_2030"].sum() / d.pop_2030.sum(),  # % năm 2030
                     f"ty_le60_3km_{B}": 100 * d.loc[trong, "pop60"].sum() / d["pop60"].sum(), "ty_le60_3km_2030": 100 * d.loc[trong, "pop60_2030"].sum() / d.pop60_2030.sum(),  # 60+
                     f"dan_ngoai_3km_{B}": d.loc[~trong, "pop"].sum(), "dan_ngoai_3km_2030": d.loc[~trong, "pop_2030"].sum(),  # dân ngoài 3 km
                     f"dan_tren_5km_{B}": d.loc[d.dist_m > RW, "pop"].sum(), "dan_tren_5km_2030": d.loc[d.dist_m > RW, "pop_2030"].sum(),  # dân > 5 km
                     "dan60_ngoai_3km_2030": d.loc[~trong, "pop60_2030"].sum()})  # 60+ ngoài 3 km
    ac = pd.DataFrame(dong)  # bảng theo xã
    ac["thay_doi_pp"] = ac.ty_le_3km_2030 - ac[f"ty_le_3km_{B}"]  # thay đổi (điểm %)
    ac["thay_doi_60_pp"] = ac.ty_le60_3km_2030 - ac[f"ty_le60_3km_{B}"]  # thay đổi 60+
    ac = tt.merge(ac, on="ma_tram").round(2)  # gắn thông tin xã, làm tròn
    ac["giam_tiep_can"] = ac.thay_doi_pp < -0.05  # xã giảm tiếp cận
    nc.xuat("access_2030_change.csv", ac, S, note="tiep can 3 km 2030 (du bao, giu 56 tram, A9)")  # vào hàng chờ (Bước 5 ghi)
    tot = {f"ty_le_3km_{B}": 100 * gg.loc[gg.dist_m <= R, "pop"].sum() / gg["pop"].sum(),  # toàn 56 xã
           "ty_le_3km_2030": 100 * gg.loc[gg.dist_m <= R, "pop_2030"].sum() / gg.pop_2030.sum(),  # (tiếp)
           "ty_le60_3km_2030": 100 * gg.loc[gg.dist_m <= R, "pop60_2030"].sum() / gg.pop60_2030.sum(),  # (tiếp)
           f"dan_tren_5km_{B}": gg.loc[gg.dist_m > RW, "pop"].sum(), "dan_tren_5km_2030": gg.loc[gg.dist_m > RW, "pop_2030"].sum(),  # (tiếp)
           "dan60_tren_5km_2030": gg.loc[gg.dist_m > RW, "pop60_2030"].sum(), "so_xa_giam": int(ac.giam_tiep_can.sum())}  # (tiếp)
    nc.xuat("access_2030_total.csv", pd.DataFrame([tot]).round(2), S, note="tiep can 3 km toan 56 xa 2030 (du bao)")  # vào hàng chờ (Bước 5 ghi)
    nc.gold(nc.tao_df(fc.merge(ac.drop(columns=["ten_day_du", "quan_huyen", "loai_dv"]), on="ma_tram")).coalesce(1),  # Gold dự báo (file riêng) vào hàng chờ (Bước 5 ghi)
           "gold/fact_access_forecast_2030")  # (tiếp)
    log.info("4. Tiep can 3 km: %d %.2f%% -> 2030 %.2f%% (du bao); so xa giam: %d", B, tot[f"ty_le_3km_{B}"], tot["ty_le_3km_2030"], tot["so_xa_giam"])  # in
    return dict(goc=goc)  # trả cho Bước 5 (xuất bảng lưới phục vụ)
