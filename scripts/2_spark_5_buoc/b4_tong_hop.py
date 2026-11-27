# -*- coding: utf-8 -*-
"""BƯỚC 4 · TỔNG HỢP (Aggregate) – khung 5 bước của pipeline Spark (nội dung nghiệp vụ: Bước 06 + 07 + 08 của luồng dữ liệu).

4.1 KPI theo khoá phân tích (MÔ TẢ): groupBy theo năm, xã, nhóm địa bàn, quận và toàn 56 xã (phép biến đổi RỘNG – có
    shuffle): % dân trong 2/3/5 km (chim bay và đường đi), % tới trạm trong 15 phút, % cần trên 20 phút (A18), khoảng cách
    trung vị p50 và đuôi p95 có trọng số dân (hàm cửa sổ cộng dồn – một lần quét cho cả p50 và p95), dân > 5 km, dân 60+,
    chênh lệch phường – xã miền núi, khoảng cách tới CSKCB. KPI theo nhóm viết thêm bằng Spark SQL để đối soát.
4.2 CHẨN ĐOÁN: vùng lõm = cụm ô cách trạm > 5 km, 3–5 km hoặc cần > 20 phút, kèm nguyên nhân địa hình / phân tán (A8).
4.3 KHUYẾN NGHỊ (b4_khuyen_nghi.py): kịch bản gộp trạm NQ 1251, điểm đặt mới (tham lam, A10), TYT cạnh CSKCB (A15, A16).
4.4 DỰ BÁO (b4_du_bao.py): dân số 2030 theo 56 xã cũ (applyInPandas), tiếp cận 2030 giữ 56 trạm (A9).
4.5 ĐỐI SOÁT với bộ số đã chốt 19/11/2026 (tests/tham_chieu_19_11_2026/) trên chính các bảng đang chờ ghi – kết quả
    sai thì biết TRƯỚC khi ghi. (Các phép bảo toàn số dòng, tổng dân, Spark SQL vs DataFrame nằm ngay tại chỗ tính.)
Bước này KHÔNG ghi file CSV hay Gold: mọi bảng kết quả vào hàng chờ của NguCanh, Bước 5 ghi một lần.
"""
import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu trên driver
from scipy import ndimage  # gom cụm ô liền kề (vùng lõm)

import b4_du_bao  # 4.4 dự báo 2030
import b4_khuyen_nghi  # 4.3 khuyến nghị
from common import (ROOT, BASE_YEAR, THRESH_KM, MAIN_KM, WHITE_KM, TIME_MIN, TIME_LOM_MIN, wquantile)  # tham số và hàm dùng chung

S = "b4_tong_hop"  # tên bước ghi trong sổ run_manifest
R, RW = MAIN_KM * 1000, WHITE_KM * 1000  # 3 km và 5 km đổi ra mét
N_PHUONG, N_NUI, N_DB = "Phường", "Xã miền núi (địa hình)", "Xã đồng bằng/ven đô"  # 3 nhóm địa bàn
THAM_CHIEU = ROOT / "tests" / "tham_chieu_19_11_2026"  # bộ kết quả đã chốt của pipeline cũ


def chay(nc, d2, d3):  # chạy Bước 4; d2 = kết quả Bước 2 (bảng sạch, tham chiếu), d3 = kết quả Bước 3 (Silver)
    F, W, spark, log = nc.F, nc.Window, nc.spark, nc.log  # công cụ Spark
    B = BASE_YEAR  # năm hiện trạng
    a, fy, tram, tp, xa = d3["a"], d3["fy"], d3["tram"], d3["tp"], d3["xa"]  # bảng Silver từ Bước 3
    n_tram = len(tp)  # số trạm

    # ================= 4.1 KPI THEO KHOÁ PHÂN TÍCH (groupBy, cửa sổ) – Bước 06 của luồng dữ liệu =================
    def ty_le(dk, w="pop"):  # % trọng số (dân) thoả điều kiện dk
        return F.sum(F.when(dk, F.col(w)).otherwise(0.0)) / F.sum(w) * 100  # tổng dân thoả / tổng dân x 100

    def kpi_co_ban(df, by):  # các KPI theo khoảng cách chim bay
        return df.groupBy(*by).agg(  # gom nhóm (shuffle) rồi tính
            F.sum("pop").alias("dan"), F.sum("pop60").alias("dan_60"),  # tổng dân, dân 60+
            *[ty_le(F.col("dist_m") <= km * 1000).alias(f"ty_le_trong_{km:g}km_pct") for km in THRESH_KM],  # % trong 2, 3, 5 km
            ty_le(F.col("dist_m") <= R, "pop60").alias("ty_le_60_trong_3km_pct"),  # % dân 60+ trong 3 km
            F.sum(F.when(F.col("dist_m") > RW, F.col("pop")).otherwise(0.0)).alias("dan_tren_5km"),  # dân cách trạm > 5 km
            F.sum(F.when(F.col("dist_m") > RW, F.col("pop60")).otherwise(0.0)).alias("dan_60_tren_5km"))  # dân 60+ cách > 5 km

    def kpi_duong(df, by):  # các KPI theo mạng đường (năm 2024)
        return df.groupBy(*by).agg(  # gom nhóm
            ty_le(F.col("net_dist_m") <= R).alias("ty_le_trong_3km_mang_duong_pct"),  # % trong 3 km theo đường
            ty_le(F.col("net_dist_m") <= RW).alias("ty_le_trong_5km_mang_duong_pct"),  # % trong 5 km theo đường
            ty_le(F.col("net_time_min") <= TIME_MIN).alias("ty_le_toi_tram_15_phut_pct"),  # % tới trạm trong 15 phút
            ty_le(F.col("net_time_min") > TIME_LOM_MIN).alias("ty_le_tren_20_phut_pct"),  # % cần trên 20 phút (A18)
            F.sum(F.when(F.col("net_time_min") > TIME_LOM_MIN, F.col("pop")).otherwise(0.0)).alias("dan_tren_20_phut"),  # dân cần trên 20 phút
            ty_le(F.col("kc_cskcb_m") <= R).alias("ty_le_trong_3km_cskcb_pct"),  # % trong 3 km tới CSKCB (chim bay)
            ty_le(F.col("kc_cskcb_duong_m") <= R).alias("ty_le_trong_3km_cskcb_duong_pct"))  # % trong 3 km tới CSKCB (đường đi)

    def phan_vi(df, by, v, **muc):  # các phân vị có trọng số dân của cột v, MỘT lần quét cửa sổ, ví dụ muc = {"kc_trung_vi_m": 0.5, "kc_p95_m": 0.95}
        d = df.filter((F.col("pop") > 0) & F.col(v).isNotNull())  # chỉ ô có dân và có giá trị
        p = W.partitionBy(*by) if by else W.partitionBy(F.lit(1))  # cửa sổ theo nhóm (hoặc toàn bảng)
        d = (d.withColumn("_cw", F.sum("pop").over(p.orderBy(v, "cell_id").rowsBetween(W.unboundedPreceding, 0)))  # dân cộng dồn theo giá trị tăng dần
             .withColumn("_tot", F.sum("pop").over(p)))  # tổng dân của nhóm
        return d.groupBy(*by).agg(*[F.min(F.when(F.col("_cw") >= q * F.col("_tot"), F.col(v))).alias(ten)  # phân vị q = giá trị nhỏ nhất mà dân luỹ kế >= q x tổng (giống common.wquantile)
                                    for ten, q in muc.items()])  # (tiếp) mỗi mức q một cột; tối ưu 27/11/2026: trước đây mỗi mức q một lần quét riêng

    def ve_pd(df, sx):  # chuyển kết quả Spark (nhỏ) về pandas, có sắp xếp
        return df.orderBy(*sx).toPandas()  # sắp rồi gom về driver

    # ---------- các bảng KPI ----------
    # G1: toàn 56 xã theo từng năm 2015–2024 (giữ nguyên 56 trạm – A14)
    g1 = (kpi_co_ban(fy, ["year"]).join(phan_vi(fy, ["year"], "dist_m", kc_trung_vi_m=0.5, kc_p95_m=0.95), "year")  # KPI + trung vị + phân vị 95
          .withColumn("so_tram", F.lit(n_tram)).withColumn("dan_binh_quan_moi_tram", F.col("dan") / n_tram))  # + số trạm, dân bình quân mỗi trạm
    g1 = ve_pd(g1, ["year"])  # về pandas
    g1 = g1[["year", "dan", "dan_60", "ty_le_trong_2km_pct", "ty_le_trong_3km_pct", "ty_le_trong_5km_pct", "ty_le_60_trong_3km_pct",  # giữ thứ tự cột cũ
             "dan_tren_5km", "dan_60_tren_5km", "kc_trung_vi_m", "so_tram", "dan_binh_quan_moi_tram", "kc_p95_m"]]  # (tiếp) cột mới ở cuối
    # G2: xã x năm -> bảng Gold chính fact_health_access_yearly
    g2s = (kpi_co_ban(fy, ["ma_tram", "year"]).join(phan_vi(fy, ["ma_tram", "year"], "dist_m", kc_trung_vi_m=0.5, kc_p95_m=0.95), ["ma_tram", "year"])  # KPI + p50 + p95
           .join(F.broadcast(xa.select("ma_tram", "ten_day_du", "quan_huyen", "nhom", "ma_quan")), "ma_tram"))  # + thông tin xã
    nc.gold(g2s.repartition("year"), "gold/fact_health_access_yearly", "year")  # vào hàng chờ Gold (Bước 5 ghi) chia thư mục theo năm
    g2 = ve_pd(g2s, ["ma_tram", "year"])[["ma_tram", "year", "dan", "dan_60", "ty_le_trong_2km_pct", "ty_le_trong_3km_pct",  # về pandas, giữ thứ tự cột
                                          "ty_le_trong_5km_pct", "ty_le_60_trong_3km_pct", "dan_tren_5km", "dan_60_tren_5km",  # (tiếp)
                                          "kc_trung_vi_m", "ten_day_du", "quan_huyen", "nhom", "kc_p95_m"]]  # (tiếp)
    # G3: theo xã, năm 2024, đủ chỉ số đường đi và CSKCB
    g3 = (kpi_co_ban(a, ["ma_tram"]).join(phan_vi(a, ["ma_tram"], "dist_m", kc_trung_vi_m=0.5, kc_p95_m=0.95), "ma_tram")  # chim bay: KPI + p50 + p95
          .join(kpi_duong(a, ["ma_tram"]), "ma_tram")  # đường đi, thời gian, CSKCB
          .join(phan_vi(a, ["ma_tram"], "net_dist_m", kc_mang_duong_trung_vi_m=0.5), "ma_tram", "left")  # trung vị đường đi
          .join(phan_vi(a.filter("dist_m > 500"), ["ma_tram"], "detour", he_so_vong_trung_vi=0.5), "ma_tram", "left")  # hệ số vòng trung vị (ô > 500 m)
          .join(phan_vi(a, ["ma_tram"], "kc_cskcb_m", kc_cskcb_trung_vi_m=0.5, kc_cskcb_p95_m=0.95), "ma_tram")  # trung vị và p95 tới CSKCB
          .join(F.broadcast(xa), "ma_tram"))  # thông tin xã
    g3 = ve_pd(g3, ["ma_tram"])  # về pandas
    g3 = g3[["ma_tram", "dan", "dan_60", "ty_le_trong_2km_pct", "ty_le_trong_3km_pct", "ty_le_trong_5km_pct", "ty_le_60_trong_3km_pct",  # cột cũ
             "dan_tren_5km", "dan_60_tren_5km", "kc_trung_vi_m", "ty_le_trong_3km_mang_duong_pct", "ty_le_trong_5km_mang_duong_pct",  # (tiếp)
             "ty_le_toi_tram_15_phut_pct", "kc_mang_duong_trung_vi_m", "he_so_vong_trung_vi", "doc_tb_theo_dan", "cao_tb_theo_dan",  # (tiếp)
             "ten_day_du", "quan_huyen", "loai_dv", "mien_nui_dia_hinh", "nhom",  # (tiếp)
             "kc_p95_m", "ty_le_tren_20_phut_pct", "dan_tren_20_phut", "kc_cskcb_trung_vi_m", "kc_cskcb_p95_m",  # cột mới
             "ty_le_trong_3km_cskcb_pct", "ty_le_trong_3km_cskcb_duong_pct"]]  # (tiếp)

    def theo_nhom(by):  # KPI theo một cột phân nhóm (nhóm địa bàn hoặc quận)
        return (kpi_co_ban(a, [by]).join(phan_vi(a, [by], "dist_m", kc_trung_vi_m=0.5, kc_p95_m=0.95), by).join(kpi_duong(a, [by]), by)  # chim bay (p50, p95) + đường đi
                .join(phan_vi(a, [by], "kc_cskcb_m", kc_cskcb_trung_vi_m=0.5), by)  # trung vị tới CSKCB
                .join(a.groupBy(by).agg(F.countDistinct("ma_tram").alias("so_xa")), by))  # số xã trong nhóm
    cot_nhom = ["dan", "dan_60", "ty_le_trong_2km_pct", "ty_le_trong_3km_pct", "ty_le_trong_5km_pct", "ty_le_60_trong_3km_pct",  # cột cũ của bảng nhóm
                "dan_tren_5km", "dan_60_tren_5km", "kc_trung_vi_m", "ty_le_trong_3km_mang_duong_pct", "ty_le_trong_5km_mang_duong_pct",  # (tiếp)
                "ty_le_toi_tram_15_phut_pct", "so_xa", "kc_p95_m", "ty_le_tren_20_phut_pct", "dan_tren_20_phut",  # (tiếp) + cột mới
                "kc_cskcb_trung_vi_m", "ty_le_trong_3km_cskcb_pct"]  # (tiếp)
    g4 = ve_pd(theo_nhom("nhom"), ["nhom"])[["nhom"] + cot_nhom]  # G4: theo nhóm địa bàn
    g4q = ve_pd(theo_nhom("quan_huyen"), ["quan_huyen"])[["quan_huyen"] + cot_nhom]  # G4b: theo quận/huyện
    # G4c: cùng KPI theo nhóm, viết bằng Spark SQL trên bảng tạm (temp view)
    a.createOrReplaceTempView("o_luoi_tiep_can")  # đăng ký bảng tạm tên o_luoi_tiep_can
    sql = f"""
        SELECT nhom,
               SUM(pop)                                                                  AS dan,
               SUM(CASE WHEN dist_m       <= {R} THEN pop ELSE 0 END) / SUM(pop) * 100   AS ty_le_trong_3km_pct,
               SUM(CASE WHEN net_dist_m   <= {R} THEN pop ELSE 0 END) / SUM(pop) * 100   AS ty_le_trong_3km_mang_duong_pct,
               SUM(CASE WHEN net_time_min <= {TIME_MIN} THEN pop ELSE 0 END) / SUM(pop) * 100 AS ty_le_toi_tram_15_phut_pct,
               COUNT(DISTINCT ma_tram)                                                   AS so_xa
        FROM o_luoi_tiep_can
        GROUP BY nhom"""  # câu SQL tính KPI theo nhóm
    g4s = spark.sql(sql)  # chạy SQL (đi qua cùng bộ tối ưu Catalyst như DataFrame API)
    nc.ke_hoach(g4s, f"plan_kpi_sql_{B}.txt", f"KPI theo nhom nam {B} viet bang Spark SQL")  # ghi kế hoạch thực thi
    g4s = ve_pd(g4s, ["nhom"])  # về pandas
    ks = g4.merge(g4s, on="nhom", suffixes=("_df", "_sql"))  # ghép hai cách viết
    lech = max(float((ks[f"{c}_df"] - ks[f"{c}_sql"]).abs().max()) for c in ("ty_le_trong_3km_pct", "ty_le_trong_3km_mang_duong_pct", "ty_le_toi_tram_15_phut_pct"))  # lệch lớn nhất
    nc.kiem("KPI theo nhom: Spark SQL vs DataFrame API, lech lon nhat (diem %)", lech, 0.0, lech <= 1e-9 and (ks.so_xa_df == ks.so_xa_sql).all(),  # đối soát
            f"ke hoach: logs/plan_kpi_sql_{B}.txt")  # (tiếp)
    # G5: vùng phục vụ từng trạm (ô thuộc trạm gần nhất theo đường chim bay)
    g5 = ve_pd(tram.select("ma_tram", "ten_tram", "quan_huyen", "loai_dv")  # danh sách trạm
               .join(a.groupBy(F.col("tram_gan_nhat").alias("ma_tram")).agg(F.sum("pop").alias("dan_phuc_vu"), F.sum("pop60").alias("dan60_phuc_vu"),  # tổng dân phục vụ
                                                                         F.avg("dist_m").alias("kc_tb_m"), F.count("*").alias("so_o_luoi")),  # (tiếp) khoảng cách TB, số ô
                     "ma_tram", "left").fillna(0, ["dan_phuc_vu", "dan60_phuc_vu", "so_o_luoi"]), ["ma_tram"])  # (tiếp) trạm không có ô nào -> 0
    # G6: tổng hợp toàn 56 xã năm 2024
    tong = (kpi_co_ban(a, []).crossJoin(phan_vi(a, [], "dist_m", kc_trung_vi_m=0.5, kc_p95_m=0.95)).crossJoin(kpi_duong(a, []))  # chim bay (p50, p95) + đường đi
            .crossJoin(phan_vi(a.filter("dist_m > 500"), [], "detour", he_so_vong_trung_vi=0.5))  # hệ số vòng
            .crossJoin(phan_vi(a, [], "net_time_min", thoi_gian_trung_vi_phut=0.5))  # thời gian trung vị
            .crossJoin(phan_vi(a, [], "net_dist_m", kc_mang_duong_p95_m=0.95))  # p95 đường đi
            .crossJoin(phan_vi(a, [], "kc_cskcb_m", kc_cskcb_trung_vi_m=0.5, kc_cskcb_p95_m=0.95)))  # CSKCB
    nc.ke_hoach(tong, f"plan_kpi_{B}.txt", f"truy van KPI chinh toan 56 xa nam {B}")  # ghi kế hoạch thực thi
    t = tong.toPandas().iloc[0]  # một dòng kết quả
    nh = g4.set_index("nhom")  # KPI theo nhóm, tra theo tên nhóm
    kq = []  # các dòng KPI tổng hợp

    def k(ten, v, dv, loai, ghi=""):  # thêm một chỉ số
        kq.append(dict(chi_so=ten, gia_tri=v, don_vi=dv, loai=loai, ghi_chu=ghi))  # dòng mới
    k(f"Tổng dân 56 xã/phường năm {B} (WorldPop, ước tính)", round(t.dan), "người", "nền", "A2")  # tổng dân
    k(f"Tổng dân 60+ năm {B}", round(t.dan_60), "người", "nền", "A3")  # dân 60+
    k("Số trạm y tế", n_tram, "trạm", "nền", "toạ độ nhập tay (A13)")  # số trạm
    k("Tỷ lệ dân trong 3 km đường chim bay tới trạm gần nhất", round(t.ty_le_trong_3km_pct, 2), "%", "KPI chính", "ngưỡng 3 km là giả định A1")  # KPI chính
    k("Tỷ lệ dân trong 3 km theo mạng đường", round(t.ty_le_trong_3km_mang_duong_pct, 2), "%", "KPI chính (đường đi)", "A6")  # theo đường
    k("Tỷ lệ dân tới trạm trong 15 phút xe máy", round(t.ty_le_toi_tram_15_phut_pct, 2), "%", "KPI chính (thời gian)", "A6")  # theo thời gian
    k("Hệ số vòng đường trung vị (mạng đường / chim bay, ô cách trạm > 500 m)", round(t.he_so_vong_trung_vi, 3), "lần", "thuận tiện đường đi", "A6")  # hệ số vòng
    k("Thời gian trung vị tới trạm (xe máy)", round(t.thoi_gian_trung_vi_phut, 1), "phút", "thuận tiện đường đi", "A6")  # thời gian trung vị
    k("Tỷ lệ dân trong 2 km", round(t.ty_le_trong_2km_pct, 2), "%", "độ nhạy", "A1")  # độ nhạy 2 km
    k("Tỷ lệ dân trong 5 km", round(t.ty_le_trong_5km_pct, 2), "%", "độ nhạy", "A1")  # độ nhạy 5 km
    k("Dân bình quân mỗi trạm", round(t.dan / n_tram), "người/trạm", "KPI phụ")  # dân bình quân mỗi trạm
    k("Khoảng cách trung vị (theo dân) tới trạm gần nhất", round(t.kc_trung_vi_m), "m", "KPI phụ")  # p50
    k("Dân cách trạm gần nhất trên 5 km (vùng trắng)", round(t.dan_tren_5km), "người", "KPI phụ")  # vùng trắng
    k("Dân 60+ cách trạm trên 5 km", round(t.dan_60_tren_5km), "người", "KPI phụ")  # 60+ vùng trắng
    k("Tỷ lệ dân 60+ trong 3 km", round(t.ty_le_60_trong_3km_pct, 2), "%", "KPI phụ", "A3")  # 60+ trong 3 km
    for g_ in (N_PHUONG, N_DB, N_NUI):  # từng nhóm địa bàn
        if g_ in nh.index:  # nhóm có mặt
            k(f"Tỷ lệ dân trong 3 km – {g_}", round(nh.loc[g_, "ty_le_trong_3km_pct"], 2), "%", "KPI phụ", f"{int(nh.loc[g_, 'so_xa'])} đơn vị; A7")  # % trong 3 km của nhóm
    gap = nh.loc[N_PHUONG, "ty_le_trong_3km_pct"] - nh.loc[N_NUI, "ty_le_trong_3km_pct"] if N_NUI in nh.index else float("nan")  # chênh lệch chim bay
    gap_d = nh.loc[N_PHUONG, "ty_le_trong_3km_mang_duong_pct"] - nh.loc[N_NUI, "ty_le_trong_3km_mang_duong_pct"] if N_NUI in nh.index else float("nan")  # chênh lệch đường đi
    k("Chênh lệch phường – xã miền núi (3 km, chim bay)", round(gap, 2), "điểm %", "KPI phụ", "A7")  # chênh lệch
    k("Chênh lệch phường – xã miền núi (3 km, mạng đường)", round(gap_d, 2), "điểm %", "KPI phụ", "A6, A7")  # chênh lệch
    k("Khoảng cách p95 (theo dân) tới trạm gần nhất", round(t.kc_p95_m), "m", "KPI phụ (mới)", "đuôi phân bố: 5% dân ở xa hơn mức này")  # p95 chim bay
    k("Khoảng cách p95 (theo dân) tới trạm theo mạng đường", round(t.kc_mang_duong_p95_m), "m", "KPI phụ (mới)", "A6")  # p95 đường đi
    k("Tỷ lệ dân cần trên 20 phút xe máy tới trạm (vùng lõm)", round(t.ty_le_tren_20_phut_pct, 2), "%", "KPI phụ (mới)", "A6, A18")  # % trên 20 phút
    k("Dân cần trên 20 phút xe máy tới trạm", round(t.dan_tren_20_phut), "người", "KPI phụ (mới)", "A6, A18")  # dân trên 20 phút
    k("Khoảng cách trung vị (theo dân) tới CSKCB gần nhất", round(t.kc_cskcb_trung_vi_m), "m", "KPI phụ (mới)", "30 CSKCB, sàn dưới (A17)")  # trung vị CSKCB
    k("Khoảng cách p95 (theo dân) tới CSKCB gần nhất", round(t.kc_cskcb_p95_m), "m", "KPI phụ (mới)", "A17")  # p95 CSKCB
    k("Tỷ lệ dân trong 3 km tới CSKCB gần nhất (chim bay)", round(t.ty_le_trong_3km_cskcb_pct, 2), "%", "KPI phụ (mới)", "A17")  # % 3 km CSKCB
    k("Tỷ lệ dân trong 3 km tới CSKCB gần nhất (mạng đường)", round(t.ty_le_trong_3km_cskcb_duong_pct, 2), "%", "KPI phụ (mới)", "A6, A17")  # % 3 km CSKCB đường
    kq = pd.DataFrame(kq)  # bảng KPI tổng hợp
    kq["gia_tri"] = kq.gia_tri.astype(float)  # ép kiểu số thực cho cột giá trị
    for _, r in kq.iterrows():  # in từng KPI
        log.info("4. KPI | %s = %s %s", r.chi_so, r.gia_tri, r.don_vi)  # dòng log

    # vào hàng chờ (Bước 5 ghi) và Gold cho Bước 06
    rd3 = {c: 3 for c in g1.columns if c.endswith("_pct")}  # làm tròn 3 chữ số cho cột %
    nc.xuat("kpi_tong_hop_spark.csv", kq, S, note=f"KPI chinh + phu nam {B} (Spark)")  # tổng hợp
    nc.xuat("kpi_theo_nam_spark.csv", g1.round(rd3).round({"kc_trung_vi_m": 1, "kc_p95_m": 1}), S, note="56 tram giu nguyen qua cac nam (A14)")  # theo năm
    nc.xuat("kpi_theo_xa_nam_spark.csv", g2.round(3), S, note="xa x nam = gold/fact_health_access_yearly (A14)")  # xã x năm
    nc.xuat("kpi_theo_xa_spark.csv", g3.round(3), S, note=f"theo xa nam {B}, kem mang duong va CSKCB")  # theo xã
    nc.xuat("kpi_theo_nhom_spark.csv", g4.round(3), S, note="phuong / xa dong bang / xa mien nui (A7)")  # theo nhóm
    nc.xuat("kpi_theo_nhom_spark_sql.csv", g4s.round(3), S, note="KPI theo nhom viet bang Spark SQL - doi soat voi kpi_theo_nhom_spark.csv")  # SQL
    nc.xuat("kpi_theo_quan_spark.csv", g4q.round(3), S, note="7 quan/huyen truoc 2025")  # theo quận
    nc.xuat("tram_vung_phuc_vu_spark.csv", g5.round(1), S, note="dan phuc vu boi tram gan nhat (chim bay)")  # vùng phục vụ
    for ten, pdf in (("kpi_tong_hop", kq), ("kpi_theo_nam", g1), ("kpi_theo_xa", g3), ("kpi_theo_nhom", g4), ("kpi_theo_quan", g4q), ("tram_vung_phuc_vu", g5)):  # các bảng Gold nhỏ
        nc.gold(nc.tao_df(pdf).coalesce(1), f"gold/{ten}")  # vào hàng chờ Gold (Bước 5 ghi) (1 file vì bảng nhỏ)
    nc.gold(a.select("cell_id", "ma_tram", "ma_quan", "pop", "pop60", "dist_m", "tram_gan_nhat", "net_dist_m", "net_time_min", "net_station",  # ma trận khoảng cách ô -> cơ sở
                    "kc_cskcb_m", "cskcb_gan_nhat", "kc_cskcb_duong_m", "tg_cskcb_phut"), f"gold/fact_khoang_cach_o_{B}", "ma_quan")  # (tiếp)

    # ================= 4.2 CHẨN ĐOÁN: VÙNG LÕM TIẾP CẬN (A8, A18) – Bước 07a =================
    xa_pd = a.filter((F.col("dist_m") > R) | (F.col("net_time_min") > TIME_LOM_MIN)).select(  # chỉ lấy ô cách trạm > 3 km hoặc cần > 20 phút (rất ít)
        "row", "col", "pop", "pop60", "dist_m", "net_dist_m", "net_time_min", "detour", "slope_deg", "elev_m", "ten_day_du", "tram_gan_nhat", "lon", "lat").toPandas()  # (tiếp)
    vung = []  # danh sách cụm
    for nhan, ma, dk in (("trang_>5km", "trang", xa_pd.dist_m > RW), ("xam_3-5km", "xam_3", (xa_pd.dist_m > R) & (xa_pd.dist_m <= RW)),  # 3 loại vùng
                         ("lom_>20phut", "lom20", xa_pd.net_time_min > TIME_LOM_MIN)):  # (tiếp)
        if not dk.any():  # loại này không có ô nào
            continue  # bỏ qua
        g = xa_pd[dk]  # ô thuộc loại này
        M = np.zeros((int(g.row.max()) + 3, int(g.col.max()) + 3), bool)  # lưới logic: True = ô thuộc loại
        M[g.row + 1, g.col + 1] = True  # đánh dấu (lệch 1 ô để có viền)
        nhan_o, _ = ndimage.label(ndimage.binary_dilation(M, iterations=2), structure=np.ones((3, 3)))  # nới 2 ô (200 m) rồi gom vùng liền kề
        g = g.assign(zone=nhan_o[g.row + 1, g.col + 1])  # mã cụm của từng ô
        for z, d in g.groupby("zone"):  # duyệt từng cụm
            w = d["pop"] + 1e-9  # trọng số dân
            hv = wquantile(d.detour, d["pop"], 0.5)  # hệ số vòng trung vị
            mat_do = d["pop"].sum() / len(d)  # dân trung bình mỗi ô
            dia_hinh = (np.average(d.slope_deg.fillna(0), weights=w) > 8) or (np.average(d.elev_m.fillna(0), weights=w) > 150) or (hv == hv and hv > 1.5)  # quy tắc địa hình (A8)
            phan_tan = mat_do < 10 or d["pop"].sum() < 300  # quy tắc dân cư phân tán (A8)
            nn = " + ".join([c for c, f in (("Địa hình", dia_hinh), ("Dân cư phân tán", phan_tan)) if f]) or "Khác (xem tay)"  # nguyên nhân
            ok = np.isfinite(d.net_dist_m)  # ô có khoảng cách đường đi
            vung.append(dict(loai_vung=nhan, ma_vung=f"{ma}{z:03d}", so_o=len(d), dan=round(d["pop"].sum()), dan_60=round(d.pop60.sum()),  # dòng cụm
                             dan_tb_moi_o=round(mat_do, 1), kc_tb_toi_tram_m=round(np.average(d.dist_m, weights=w)),  # (tiếp)
                             kc_mang_duong_tb_m=round(np.average(d.net_dist_m[ok], weights=w[ok])) if ok.any() else np.nan,  # (tiếp)
                             he_so_vong_trung_vi=round(hv, 2) if hv == hv else np.nan,  # (tiếp)
                             do_cao_tb_m=round(np.average(d.elev_m.fillna(0), weights=w)), do_doc_tb_deg=round(np.average(d.slope_deg.fillna(0), weights=w), 1),  # (tiếp)
                             xa=", ".join(sorted(d.ten_day_du.unique())), tram_gan_nhat=", ".join(sorted(d.tram_gan_nhat.unique())),  # (tiếp)
                             nguyen_nhan=nn, lon=d.lon.mean(), lat=d.lat.mean(),  # (tiếp)
                             dan_tren_20_phut=round(d.loc[d.net_time_min > TIME_LOM_MIN, "pop"].sum())))  # (tiếp) cột mới
    vz = pd.DataFrame(vung).sort_values(["loai_vung", "dan"], ascending=[False, False])  # sắp như bản cũ
    nc.xuat("white_zones.csv", vz, S, note="vung trang > 5 km, vung xam 3-5 km, vung lom > 20 phut (A8, A18)")  # vào hàng chờ (Bước 5 ghi)
    nc.gold(nc.tao_df(vz).coalesce(1), "gold/vung_lom_tiep_can")  # vào hàng chờ Gold (Bước 5 ghi)
    for lv, d in vz.groupby("loai_vung"):  # in tóm tắt
        log.info("4. %s: %d cum, %d nguoi; theo nguyen nhan %s", lv, len(d), d.dan.sum(), d.groupby("nguyen_nhan").dan.sum().to_dict())  # dòng log

    # ================= 4.3 KHUYẾN NGHỊ và 4.4 DỰ BÁO =================
    kn = b4_khuyen_nghi.chay(nc, d2, d3, g3, g5)  # kịch bản NQ 1251, điểm đặt mới, TYT – CSKCB
    db = b4_du_bao.chay(nc, d3, kn["g"])  # dự báo 2030 và tiếp cận 2030
    # ================= 4.5 ĐỐI SOÁT VỚI BỘ SỐ ĐÃ CHỐT 19/11/2026 =================
    doi_soat_tham_chieu(nc)  # so trên các bảng đang chờ ghi
    return dict(g=kn["g"], g3=g3, kq=kq, sites=kn["sites"], goc=db["goc"])  # trả cho Bước 5


def doi_soat_tham_chieu(nc):  # so các con số chính với bộ số đã chốt 19/11/2026 của pipeline cũ
    log = nc.log  # bộ ghi log
    if not THAM_CHIEU.exists():  # không có bộ tham chiếu
        log.warning("CAN XEM: khong co %s -> bo qua doi soat voi bo so cu", THAM_CHIEU)  # cảnh báo
        return  # dừng hàm

    def doc(thu_muc, ten):  # đọc một bảng; thu_muc = None nghĩa là bảng mới đang chờ ghi (đọc như đọc file CSV)
        return nc.doc_cho(ten) if thu_muc is None else pd.read_csv(thu_muc / ten, encoding="utf-8-sig")  # bảng mới / bảng đã chốt

    moi_k = doc(None, "kpi_tong_hop_spark.csv").set_index("chi_so").gia_tri  # KPI tổng hợp mới
    cu_k = doc(THAM_CHIEU, "kpi_tong_hop_spark.csv").set_index("chi_so").gia_tri  # KPI tổng hợp cũ
    for ten, v_cu in cu_k.items():  # từng chỉ số cũ
        v_moi = moi_k.get(ten, np.nan)  # chỉ số tương ứng mới
        nguong = 1.0 if abs(v_cu) >= 100 else 0.011  # người/mét: lệch <= 1; tỷ lệ %: lệch <= 0,011
        nc.kiem(f"So voi 19/11/2026 – {ten}", v_moi, v_cu, bool(abs(v_moi - v_cu) <= nguong), f"nguong {nguong}")  # đối soát
    for ten, cot, khoa, nguong in (("kpi_theo_nam_spark.csv", "ty_le_trong_3km_pct", ["year"], 0.001),  # các bảng chi tiết đem so
                                   ("kpi_theo_nam_spark.csv", "dan", ["year"], 1.0),  # (tiếp)
                                   ("kpi_theo_xa_spark.csv", "ty_le_trong_3km_pct", ["ma_tram"], 0.001),  # (tiếp)
                                   ("kpi_theo_xa_spark.csv", "ty_le_trong_3km_mang_duong_pct", ["ma_tram"], 0.001),  # (tiếp)
                                   ("tram_vung_phuc_vu_spark.csv", "dan_phuc_vu", ["ma_tram"], 0.15),  # (tiếp) làm tròn 1 chữ số
                                   ("greedy_before_after.csv", "ty_le_3km", ["K"], 0.011),  # (tiếp)
                                   ("greedy_before_after.csv", "ty_le_3km_mang_duong", ["K"], 0.011),  # (tiếp)
                                   ("access_2030_change.csv", "ty_le_3km_2030", ["ma_tram"], 0.011),  # (tiếp)
                                   ("tyt_cskcb.csv", "cskcb_2km", ["ma_tram"], 0.0),  # (tiếp)
                                   ("tyt_cskcb.csv", "kc_cskcb_gan_nhat_m", ["ma_tram"], 1.0)):  # (tiếp)
        m = doc(None, ten).merge(doc(THAM_CHIEU, ten), on=khoa, suffixes=("_moi", "_cu"))  # ghép mới và cũ theo khoá
        lech = float((m[f"{cot}_moi"] - m[f"{cot}_cu"]).abs().max())  # lệch lớn nhất
        nc.kiem(f"So voi 19/11/2026 – {ten}: {cot}, lech lon nhat", round(lech, 4), 0.0, bool(lech <= nguong), f"{len(m)} dong; nguong {nguong}")  # đối soát
    m = doc(None, "forecast_commune_2030.csv").merge(doc(THAM_CHIEU, "forecast_commune_2030.csv"), on="ma_tram", suffixes=("_moi", "_cu"))  # dự báo mới / cũ
    tuong_doi = float(((m.dan_2030_moi - m.dan_2030_cu).abs() / m.dan_2030_cu * 100).max())  # lệch tương đối lớn nhất (%)
    bd = getattr(nc, "bien_do", {}).get("Dân số", 0.0)  # biên độ khoảng 80% của dự báo dân số (%)
    nc.kiem("So voi 19/11/2026 – forecast_commune_2030.csv: dan_2030, lech tuong doi lon nhat (%)", round(tuong_doi, 3), 0.0, tuong_doi <= bd,  # đối soát
            f"nguong = bien do khoang 80% ({bd:.3f}%); ETS nhay voi sai so lam tron 1e-10 nen khong lap lai dung so cu")  # (tiếp)
    uv_m, uv_c = doc(None, "ung_vien_gop_tram.csv"), doc(THAM_CHIEU, "ung_vien_gop_tram.csv")  # ứng viên A15 mới / cũ
    nc.kiem("So voi 19/11/2026 – danh sach ung vien ra soat gop tram (A15)", len(uv_m), len(uv_c), set(uv_m.ma_tram) == set(uv_c.ma_tram))  # đối soát
    vz_m, vz_c = doc(None, "white_zones.csv"), doc(THAM_CHIEU, "white_zones.csv")  # vùng trắng mới / cũ
    tr_m, tr_c = vz_m[vz_m.loai_vung.str.startswith("trang")], vz_c[vz_c.loai_vung.str.startswith("trang")]  # chỉ vùng trắng
    nc.kiem("So voi 19/11/2026 – so cum vung trang > 5 km", len(tr_m), len(tr_c), len(tr_m) == len(tr_c))  # đối soát
    nc.kiem("So voi 19/11/2026 – cum vung trang mang ca dia hinh", int(tr_m.nguyen_nhan.str.contains("Địa hình").sum()),  # đối soát
            int(tr_c.nguyen_nhan.str.contains("Địa hình").sum()), int(tr_m.nguyen_nhan.str.contains("Địa hình").sum()) == int(tr_c.nguyen_nhan.str.contains("Địa hình").sum()))  # (tiếp)
    log.info("4. Doi soat voi bo so 19/11/2026: xong (xem doi_soat_spark.csv)")  # in
