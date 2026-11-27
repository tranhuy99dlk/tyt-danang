# -*- coding: utf-8 -*-
"""BƯỚC 2 · KIỂM TRA & LÀM SẠCH (Validate & Clean) – khung 5 bước của pipeline Spark.

1. KIỂM TRA từng dòng của các bảng thô (Bước 1 đã đọc) theo danh sách quy tắc:
     - giá trị Null: THIEU_KHOA, THIEU_TOA_DO, THIEU_GIA_TRI, THIEU_NUT_DUONG
     - đúng kiểu dữ liệu: SAI_KIEU_HOAC_SO_COT (dòng rơi vào cột _corrupt_record khi đọc), SAI_DINH_DANG_TOA_DO
     - miền giá trị: NGOAI_HOP_BAO / SAI_TOA_DO_NGOAI_HOP_BAO, QUAN_KHONG_HOP_LE, LOAI_DV_KHONG_HOP_LE, NAM_NGOAI_PHAM_VI,
       BIEN_KHONG_HOP_LE, GIA_TRI_KHONG_DUONG, GIA_TRI_AM_NODATA
     - trùng lặp và toàn vẹn tham chiếu: TRUNG_KHOA, TRUNG_TRAM, MA_XA_KHONG_CO_TRONG_DANH_MUC, CANH_TRO_TOI_NUT_KHONG_CO…
   Một dòng có thể mang nhiều mã lý do (nối bằng ";").
2. TÁCH dòng vi phạm vào MỘT trong hai vùng của tầng Silver:
     - silver/quarantine_records : LỖI DỮ LIỆU thật (loai = LOI_DU_LIEU) – lưu NGUYÊN VĂN dòng (cột ban_ghi, dạng JSON)
                                   để người phụ trách xem và sửa ở nguồn.
     - silver/excluded_records   : dòng NGOÀI PHẠM VI, KHÔNG phải lỗi (loai = NGOAI_PHAM_VI), ví dụ mã NoData của WorldPop,
                                   pixel ngoài 56 xã (#Truthmode). Chỉ lưu KHOÁ THAM CHIẾU (nguon, khoa, ly_do); ban_ghi để
                                   trống vì bản ghi gốc nằm nguyên ở Bronze (chỉ thêm, không sửa) và tra lại được theo khoá.
                                   Lý do (tối ưu 27/11/2026): trước đây vùng này chép lại hơn 3 triệu dòng dạng JSON, chiếm
                                   56 MB / 87 MB của tầng Silver và là phần chậm nhất của bước kiểm tra.
   Dòng không vi phạm quy tắc nào là dòng sạch, trả về cho Bước 3. KHÔNG xoá điểm bất thường (ví dụ ô rất đông dân):
   quy tắc chỉ loại dòng sai kiểu, thiếu khoá, ngoài miền giá trị đã định nghĩa – không loại theo cảm tính.
3. Quy tắc TỔNG DÂN ±3% của thiết kế: so tổng dân WorldPop với số liệu chính thức NQ 1251. Vì WorldPop là ước tính
   mô hình, lệch này KHÔNG phải lỗi dữ liệu -> chỉ CẢNH BÁO ("CẦN XEM") trong qa_official_population.csv, không đưa
   vào quarantine (người dùng chốt 26/11/2026).
"""
import pandas as pd  # bảng dữ liệu trên driver

from common import (BBOX, YEARS, PROJ_YEARS, AGE60_BANDS, DISTRICTS, MA_QUAN, BASE_YEAR,  # tham số dùng chung
                    POP_TOL_CANH_BAO_PCT, POP_TOL_A11_PCT, norm_name)  # (tiếp) ngưỡng và hàm chuẩn hoá tên

S = "b2_kiem_tra_lam_sach"  # tên bước ghi trong sổ run_manifest
RE_LATLON = r"^\s*(-?\d+(?:\.\d+)?)\s*[,;]\s*(-?\d+(?:\.\d+)?)\s*$"  # mẫu chuỗi "vĩ độ, kinh độ"
LOI, NGOAI, SACH = "LOI_DU_LIEU", "NGOAI_PHAM_VI", "SACH"  # hai loại dòng bị loại và nhãn dòng sạch
MA_NGOAI_PHAM_VI = ("GIA_TRI_AM_NODATA", "NGOAI_56_XA", "NAM_NGOAI_PHAM_VI", "KHONG_THUOC_O_CO_DAN")  # mã lý do KHÔNG phải lỗi


def chay(nc, d1):  # chạy Bước 2; nc là đối tượng NguCanh, d1 = kết quả Bước 1 (bảng thô)
    F, T, W, log = nc.F, nc.T, nc.Window, nc.log  # lấy các công cụ từ ngữ cảnh cho gọn
    D, St = T.DoubleType(), T.StringType()  # kiểu số thực, chuỗi
    CORRUPT, tho = d1["CORRUPT"], d1["tho"]  # tên cột dòng hỏng; các bảng thô đã nạp bộ nhớ đệm
    gan_cache = []  # các bảng đã gắn lý do (giữ trong bộ nhớ), giải phóng ở cuối bước
    tong_hop = []  # số dòng theo (loại, nguồn, lý do) – thay cho một lần đếm lại toàn bộ vùng loại bỏ

    def danh_so_trung(df, khoa):  # đánh số thứ tự bản ghi trong mỗi nhóm khoá: số > 1 là bản trùng
        return df.withColumn("_dup", F.row_number().over(W.partitionBy(*khoa).orderBy(F.lit(1))))  # hàm cửa sổ row_number

    def tach(df, ten, cot_goc, cot_khoa, quy_tac):  # áp quy tắc, tách dòng sạch và dòng bị loại
        gan = df.withColumn("_ly_do", F.concat_ws(";", *[F.when(dk, F.lit(ma)) for ma, dk in quy_tac]))  # nối mã mọi quy tắc bị vi phạm
        gan = gan.withColumn("_loai", F.when(F.col("_ly_do") == "", F.lit(SACH))  # không vi phạm gì -> SẠCH
                             .when(F.exists(F.split("_ly_do", ";"), lambda x: ~x.isin(*MA_NGOAI_PHAM_VI)), F.lit(LOI))  # có mã lỗi thật -> LỖI
                             .otherwise(F.lit(NGOAI))).cache()  # chỉ mang mã ngoài phạm vi -> NGOÀI PHẠM VI; giữ trong bộ nhớ
        gan_cache.append(gan)  # nhớ để giải phóng sau
        dem = gan.groupBy("_loai", "_ly_do").count().toPandas()  # MỘT lần quét: số dòng theo (loại, lý do)
        dem.columns = ["loai", "ly_do", "so_dong"]  # đặt tên cột dễ dùng
        n_tho = int(dem.so_dong.sum())  # tổng số dòng thô
        n_loi = int(dem.loc[dem.loai == LOI, "so_dong"].sum())  # số dòng lỗi dữ liệu
        n_ngoai = int(dem.loc[dem.loai == NGOAI, "so_dong"].sum())  # số dòng ngoài phạm vi
        for r in dem[dem.loai != SACH].itertuples():  # từng nhóm lý do của dòng bị loại
            tong_hop.append(dict(loai=r.loai, nguon=ten, ly_do=r.ly_do, so_dong=int(r.so_dong)))  # ghi vào bảng tổng hợp
        sach = gan.filter(F.col("_loai") == SACH).drop("_ly_do", "_loai", CORRUPT)  # dòng sạch: không vi phạm quy tắc nào
        bo = gan.filter(F.col("_loai") != SACH).select(  # bảng dòng bị loại
            F.lit(ten).alias("nguon"), F.col("_loai").alias("loai"), F.col("_ly_do").alias("ly_do"),  # nguồn, loại, lý do
            F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("")) for k in cot_khoa]).alias("khoa"),  # khoá để tra lại Bronze
            F.to_json(F.struct(*[F.col(c) for c in cot_goc + [CORRUPT]])).alias("ban_ghi"))  # nguyên văn dòng dạng JSON (chỉ tính khi cần)
        nc.so_dong.append(dict(nguon=ten, so_dong_tho=n_tho, so_dong_hop_le=n_tho - n_loi - n_ngoai,  # ghi sổ số dòng
                               so_dong_cach_ly=n_loi + n_ngoai, so_dong_loi_du_lieu=n_loi, so_dong_ngoai_pham_vi=n_ngoai))  # (tiếp)
        nc.loai_bo.append(bo)  # nhớ bảng dòng bị loại để ghi một lần ở cuối bước
        log.info("2. %-16s tho %8d | sach %8d | loi du lieu %5d | ngoai pham vi %8d", ten, n_tho, n_tho - n_loi - n_ngoai, n_loi, n_ngoai)  # in
        return sach, gan  # trả bảng sạch và bảng đã gắn lý do (dùng cho đối soát)

    sai_kieu = F.col(CORRUPT).isNotNull()  # điều kiện: dòng không đọc được theo schema
    W_, S_, E_, N_ = BBOX  # tách hộp bao thành Tây, Nam, Đông, Bắc

    def trong_hop_bao(lon, lat):  # điều kiện toạ độ nằm trong hộp bao Đà Nẵng cũ
        return F.col(lon).between(W_, E_) & F.col(lat).between(S_, N_)  # cả kinh độ và vĩ độ trong khoảng

    # ---------- 2.1 Danh mục 56 trạm / xã ----------
    dm_tho = tho["dm_56_tram"]  # bảng thô từ Bước 1
    dm, _ = tach(danh_so_trung(dm_tho, ["ma_tram"]), "dm_56_tram", dm_tho.columns[:-1], ["ma_tram"], [  # tách theo quy tắc
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("THIEU_KHOA", F.col("ma_tram").isNull() | F.col("ten_xa_phuong").isNull()),  # thiếu mã hoặc tên xã (quy tắc toàn vẹn khoá)
        ("TRUNG_KHOA", F.col("_dup") > 1),  # trùng mã xã (quy tắc toàn vẹn khoá)
        ("QUAN_KHONG_HOP_LE", ~F.col("quan_huyen").isin(*DISTRICTS)),  # quận/huyện không thuộc 7 quận/huyện cũ
        ("LOAI_DV_KHONG_HOP_LE", ~F.col("loai_dv").isin("Phường", "Xã"))])  # loại đơn vị không phải Phường/Xã
    ban_do_quan = F.create_map(*[F.lit(x) for kv in MA_QUAN.items() for x in kv])  # bảng tra tên quận -> mã quận không dấu
    dm = dm.drop("_dup").withColumn("ma_quan", ban_do_quan[F.col("quan_huyen")]).cache()  # thêm cột ma_quan (district_id)
    ma_dm = F.broadcast(dm.select(F.col("ma_tram").alias("_ma_dm")))  # danh sách mã hợp lệ, gửi tới mọi executor (broadcast)

    # ---------- 2.2 Pixel 100 m (bảng tra cứu không gian) ----------
    o_tho = tho["o_luoi_100m"]  # bảng thô
    o_luoi, _ = tach(danh_so_trung(o_tho, ["row", "col"]).join(ma_dm, F.col("ma_tram") == F.col("_ma_dm"), "left"),  # nối danh mục để kiểm mã xã
                     "o_luoi_100m", o_tho.columns[:-1], ["row", "col"], [  # (tiếp) tên nguồn, cột gốc, khoá
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("THIEU_KHOA", F.col("row").isNull() | F.col("col").isNull() | F.col("cell_id").isNull()),  # thiếu khoá
        ("THIEU_TOA_DO", F.col("lon").isNull() | F.col("lat").isNull() | F.col("x_m").isNull() | F.col("y_m").isNull()),  # thiếu toạ độ
        ("SAI_TOA_DO_NGOAI_HOP_BAO", ~trong_hop_bao("lon", "lat")),  # toạ độ ngoài hộp bao
        ("THIEU_NUT_DUONG", F.col("nut_duong").isNull() | F.col("kc_ra_duong_m").isNull()),  # chưa gắn được nút đường
        ("TRUNG_KHOA", F.col("_dup") > 1),  # trùng pixel
        ("NGOAI_56_XA", F.col("ma_tram").isNull()),  # pixel ngoài 56 xã -> ngoài phạm vi, không phải lỗi
        ("MA_XA_KHONG_CO_TRONG_DANH_MUC", F.col("ma_tram").isNotNull() & F.col("_ma_dm").isNull())])  # mã xã lạ
    o_luoi = o_luoi.drop("_dup", "_ma_dm").cache()  # bỏ cột phụ, giữ trong bộ nhớ

    # ---------- 2.3 Dân số 100 m x năm ----------
    pop_tho = tho["pop_100m_raw"]  # 3 triệu dòng pixel x năm
    moi_o = F.broadcast(o_tho.dropDuplicates(["row", "col"]).select("row", "col", F.lit(1).alias("_co_o"), F.col("ma_tram").alias("_ma")))  # mọi pixel có trong bảng tra cứu
    o_sach = F.broadcast(o_luoi.select("row", "col", F.lit(1).alias("_o_ok")))  # pixel đã qua kiểm tra
    pop, pop_gan = tach(danh_so_trung(pop_tho, ["year", "row", "col"]).join(moi_o, ["row", "col"], "left").join(o_sach, ["row", "col"], "left"),  # nối để biết pixel thuộc đâu
                        "pop_100m_raw", pop_tho.columns[:-1], ["year", "row", "col"], [  # (tiếp)
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("THIEU_KHOA", F.col("year").isNull() | F.col("row").isNull() | F.col("col").isNull()),  # thiếu khoá
        ("NAM_NGOAI_PHAM_VI", ~F.col("year").between(YEARS[0], YEARS[-1])),  # năm ngoài 2015–2024 -> ngoài phạm vi
        ("THIEU_GIA_TRI", F.col("pop_raw").isNull() & ~sai_kieu),  # ô giá trị rỗng
        ("GIA_TRI_AM_NODATA", F.col("pop_raw") < 0),  # mã NoData -99999 của WorldPop -> ngoài phạm vi, không phải lỗi
        ("TRUNG_KHOA", F.col("_dup") > 1),  # trùng khoá
        ("KHONG_CO_O_LUOI", F.col("_co_o").isNull()),  # pixel không có trong bảng tra cứu
        ("NGOAI_56_XA", F.col("_co_o").isNotNull() & F.col("_ma").isNull()),  # pixel ngoài 56 xã
        ("O_LUOI_BI_CACH_LY", F.col("_ma").isNotNull() & F.col("_o_ok").isNull())])  # pixel thuộc ô bị loại vì lỗi
    pop = pop.select("year", "row", "col", F.col("pop_raw").alias("pop")).cache()  # giữ cột cần thiết, đổi tên pop_raw -> pop

    # ---------- 2.4 Lưới 1 km: tổng dân + 7 nhóm tuổi (2015–2024 và số chiếu 2025–2030) ----------
    bien_ok = ["pop"] + [f"t_{b}" for b in AGE60_BANDS]  # 8 tên biến hợp lệ
    k1_tho = tho["pop_1km_raw"]  # lưới 1 km thô
    k1, _ = tach(danh_so_trung(k1_tho, ["year", "k1_row", "k1_col", "bien"]), "pop_1km_raw", k1_tho.columns[:-1],  # tách theo quy tắc
                 ["year", "k1_row", "k1_col", "bien"], [  # (tiếp) khoá
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("THIEU_KHOA", F.col("year").isNull() | F.col("k1_row").isNull() | F.col("k1_col").isNull() | F.col("bien").isNull()),  # thiếu khoá
        ("NAM_NGOAI_PHAM_VI", ~F.col("year").between(YEARS[0], PROJ_YEARS[-1])),  # năm ngoài 2015–2030
        ("BIEN_KHONG_HOP_LE", ~F.col("bien").isin(bien_ok)),  # tên biến lạ
        ("THIEU_GIA_TRI", F.col("gia_tri").isNull() & ~sai_kieu),  # thiếu giá trị
        ("GIA_TRI_AM_NODATA", F.col("gia_tri") < 0),  # NoData -> ngoài phạm vi
        ("TRUNG_KHOA", F.col("_dup") > 1)])  # trùng khoá
    k1 = k1.drop("_dup").cache()  # bỏ cột phụ

    # ---------- 2.5 Toạ độ trạm y tế nhập tay ----------
    f_chuan = F.udf(norm_name, St)  # hàm chuẩn hoá tên (Python) dùng được trong Spark (UDF)
    tyt_tho = tho["tyt_raw"]  # nguyên văn file Excel
    khoa_dm = F.broadcast(dm.select(f_chuan("ten_xa_phuong").alias("_k"), "ma_tram", "ten_tram", "quan_huyen", "loai_dv", "ten_day_du"))  # danh mục theo tên chuẩn hoá
    la_quan = F.lower(F.col("cot1")).rlike(r"^\s*(quận|huyện)\s")  # dòng nhóm "Quận ..."/"Huyện ..." trong Excel
    tyt_p = (tyt_tho.withColumn("lat", F.regexp_extract("cot3", RE_LATLON, 1).cast(D))  # tách vĩ độ từ chuỗi "lat, lon"
             .withColumn("lon", F.regexp_extract("cot3", RE_LATLON, 2).cast(D))  # tách kinh độ
             .withColumn("_quan_excel", F.last(F.when(la_quan, F.trim("cot1")), ignorenulls=True)  # quận của dòng nhóm gần nhất phía trên
                         .over(W.orderBy("dong_excel").rowsBetween(W.unboundedPreceding, 0)))  # (tiếp) hàm cửa sổ luỹ kế theo thứ tự dòng
             .withColumn("_k", f_chuan("cot2")).join(khoa_dm, "_k", "left"))  # chuẩn hoá tên xã và nối với danh mục
    tyt_p = tyt_p.withColumn("_dup", F.when(F.col("ma_tram").isNotNull(),  # đánh số trùng chỉ cho dòng đã khớp mã trạm
                                            F.row_number().over(W.partitionBy("ma_tram").orderBy("dong_excel"))).otherwise(F.lit(1)))  # (tiếp) dòng trên được giữ
    la_ban_ghi = F.col("cot2").isNotNull() & F.col("cot3").isNotNull()  # dòng có tên xã và ô toạ độ = bản ghi trạm
    tyt, _ = tach(tyt_p, "tyt_raw", tyt_tho.columns[:-1], ["dong_excel"], [  # tách theo quy tắc
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("KHONG_PHAI_BAN_GHI_TRAM", ~la_ban_ghi),  # dòng tiêu đề / dòng tên quận
        ("SAI_DINH_DANG_TOA_DO", la_ban_ghi & F.col("lat").isNull()),  # ô toạ độ không đúng mẫu "lat, lon"
        ("NGOAI_HOP_BAO", F.col("lat").isNotNull() & ~trong_hop_bao("lon", "lat")),  # toạ độ ngoài hộp bao (quy tắc toạ độ của thiết kế)
        ("TEN_KHONG_KHOP_DANH_MUC", la_ban_ghi & F.col("lat").isNotNull() & F.col("ma_tram").isNull()),  # tên xã lạ
        ("TRUNG_TRAM", F.col("_dup") > 1)])  # một trạm xuất hiện hai lần
    lech_quan = tyt.filter(f_chuan("_quan_excel") != f_chuan("quan_huyen")).select("dong_excel", "cot2", "_quan_excel").collect()  # dòng nằm dưới nhóm quận khác danh mục
    for r in lech_quan:  # báo từng dòng (chỉ cảnh báo, không loại)
        log.warning("CAN XEM dong %d: %s nam duoi '%s' trong file nhap tay, danh muc ghi quan khac", r.dong_excel, r.cot2, r._quan_excel)  # in cảnh báo
    tyt = tyt.select("ma_tram", "ten_tram", "quan_huyen", "loai_dv", "ten_day_du", "lon", "lat", "dong_excel").cache()  # giữ cột cần thiết

    # ---------- 2.6 Danh sách cơ sở khám chữa bệnh ----------
    cs_tho = tho["cskcb_raw"]  # nguyên văn
    cs_p = (cs_tho.withColumn("lat", F.regexp_extract("toa_do", RE_LATLON, 1).cast(D))  # tách vĩ độ
            .withColumn("lon", F.regexp_extract("toa_do", RE_LATLON, 2).cast(D)))  # tách kinh độ
    cs_p = cs_p.withColumn("_dup", F.when(F.col("ma_cs").isNotNull(), F.row_number().over(W.partitionBy("ma_cs").orderBy("dong_excel"))).otherwise(F.lit(1)))  # đánh số trùng mã cơ sở
    cskcb, _ = tach(cs_p, "cskcb_raw", cs_tho.columns[:-1], ["dong_excel"], [  # tách theo quy tắc
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("THIEU_KHOA", F.col("ten_cs").isNull()),  # thiếu tên cơ sở
        ("THIEU_TOA_DO", F.col("toa_do").isNull()),  # thiếu ô toạ độ
        ("SAI_DINH_DANG_TOA_DO", F.col("toa_do").isNotNull() & F.col("lat").isNull()),  # toạ độ sai mẫu
        ("NGOAI_HOP_BAO", F.col("lat").isNotNull() & ~trong_hop_bao("lon", "lat")),  # ngoài hộp bao
        ("TRUNG_KHOA", F.col("_dup") > 1)])  # trùng mã cơ sở
    cskcb = cskcb.select("ma_cs", "ten_cs", "tuyen", "hang", "cap", F.col("diem").cast(D).alias("diem"), "dia_chi", "lat", "lon").cache()  # cột cần thiết

    # ---------- 2.7 Đồ thị đường ----------
    nut_tho = tho["duong_nut"]  # nút
    nut, _ = tach(danh_so_trung(nut_tho, ["nut"]), "duong_nut", nut_tho.columns[:-1], ["nut"], [  # tách
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("THIEU_KHOA", F.col("nut").isNull()),  # thiếu mã nút
        ("THIEU_TOA_DO", F.col("x_m").isNull() | F.col("y_m").isNull()),  # thiếu toạ độ
        ("TRUNG_KHOA", F.col("_dup") > 1)])  # trùng nút
    nut = nut.drop("_dup").cache()  # bỏ cột phụ
    nut_ok = F.broadcast(nut.select("nut"))  # danh sách nút hợp lệ
    canh_tho = tho["duong_canh"]  # cạnh
    canh_p = (canh_tho.join(nut_ok.withColumnRenamed("nut", "_ni"), F.col("i") == F.col("_ni"), "left")  # kiểm nút đầu có tồn tại
              .join(nut_ok.withColumnRenamed("nut", "_nj"), F.col("j") == F.col("_nj"), "left"))  # kiểm nút cuối có tồn tại
    canh, _ = tach(canh_p, "duong_canh", canh_tho.columns[:-1], ["i", "j"], [  # tách
        ("SAI_KIEU_HOAC_SO_COT", sai_kieu),  # đọc không được
        ("THIEU_KHOA", F.col("i").isNull() | F.col("j").isNull()),  # thiếu nút
        ("CANH_TRO_TOI_NUT_KHONG_CO", F.col("i").isNotNull() & F.col("j").isNotNull() & (F.col("_ni").isNull() | F.col("_nj").isNull())),  # cạnh trỏ tới nút lạ
        ("GIA_TRI_KHONG_DUONG", ~((F.col("dai_m") > 0) & (F.col("phut") > 0)))])  # độ dài / thời gian không dương
    canh = canh.select("i", "j", "dai_m", "phut").cache()  # cột cần thiết

    # ---------- 2.8 Bảng tham chiếu nhỏ (chỉ kiểm tra có khoá) ----------
    def kiem_tham_chieu(ten, khoa):  # tách dòng thiếu khoá của một bảng tham chiếu
        d = d1["tham_chieu"][ten]  # bảng đã đọc ở Bước 1 (mọi cột là chuỗi)
        sach, _ = tach(d, ten, d.columns[:-1], [khoa], [("THIEU_KHOA", F.col(khoa).isNull())])  # quy tắc duy nhất: có khoá
        return sach.toPandas()  # bảng nhỏ -> đưa về pandas trên driver
    nq_ds = kiem_tham_chieu("dan_so_nq1251", "doi_tuong")  # số liệu dân số chính thức theo NQ 1251
    nq_sx = kiem_tham_chieu("nq1251_sap_xep", "don_vi_moi")  # danh sách đơn vị sắp xếp theo NQ 1251
    tt30 = kiem_tham_chieu("tt30_goi_dich_vu", "ten_goi_dich_vu")  # 15 gói dịch vụ theo TT 30/2024/TT-BYT

    # ---------- 2.9 Ghi bảng cách ly: quarantine (lỗi thật, nguyên văn) và excluded (ngoài phạm vi, khoá tham chiếu) ----------
    tat_ca = nc.loai_bo[0]  # bảng dòng bị loại của nguồn đầu tiên
    for b in nc.loai_bo[1:]:  # các nguồn còn lại
        tat_ca = tat_ca.unionByName(b)  # ghép theo tên cột (KHÔNG cache: cột JSON chỉ tính cho dòng thật sự cần)
    nc.ghi(tat_ca.filter(F.col("loai") == LOI), "silver/quarantine_records", "nguon", "static")  # lỗi thật -> quarantine, giữ nguyên văn
    nc.ghi(tat_ca.filter(F.col("loai") == NGOAI).withColumn("ban_ghi", F.lit(None).cast(St)),  # ngoài phạm vi -> excluded, bỏ bản sao JSON
           "silver/excluded_records", "nguon", "static")  # (tiếp) chia thư mục theo nguồn
    so = pd.DataFrame(nc.so_dong)  # bảng số dòng theo nguồn
    n_q = nc.doc("silver/quarantine_records", tat_ca.schema).count()  # đọc lại để đếm (theo schema: vẫn đọc được khi rỗng)
    n_e = nc.doc("silver/excluded_records", tat_ca.schema).count()  # đọc lại vùng excluded
    nc.kiem("So dong quarantine doc lai = tong loi du lieu cac nguon", n_q, int(so.so_dong_loi_du_lieu.sum()), n_q == int(so.so_dong_loi_du_lieu.sum()))  # đối soát
    nc.kiem("So dong excluded doc lai = tong ngoai pham vi cac nguon", n_e, int(so.so_dong_ngoai_pham_vi.sum()), n_e == int(so.so_dong_ngoai_pham_vi.sum()),  # đối soát
            "NoData cua WorldPop, ngoai 56 xa - khong phai loi")  # (tiếp) ghi chú
    so["ty_le_cach_ly_pct"] = (100 * so.so_dong_cach_ly / so.so_dong_tho).round(3)  # tỷ lệ dòng không vào bảng sạch
    so["ty_le_loi_du_lieu_pct"] = (100 * so.so_dong_loi_du_lieu / so.so_dong_tho).round(3)  # tỷ lệ lỗi thật
    nc.xuat("spark_so_dong_theo_nguon.csv", so, S, note="so dong tho / sach / loi du lieu / ngoai pham vi tung nguon Bronze")  # vào hàng chờ
    th = pd.DataFrame(tong_hop).sort_values(["loai", "nguon", "ly_do"]).reset_index(drop=True)  # tổng hợp theo lý do (từ các lần đếm ở trên)
    nc.xuat("spark_quarantine_tong_hop.csv", th, S, note="dong bi loai theo loai (LOI_DU_LIEU / NGOAI_PHAM_VI), nguon, ly do")  # vào hàng chờ
    mau = None  # bảng dòng mẫu
    for r in th.itertuples():  # từng nhóm (loại, nguồn, lý do): lấy 10 dòng có khoá nhỏ nhất
        m = (tat_ca.filter((F.col("loai") == r.loai) & (F.col("nguon") == r.nguon) & (F.col("ly_do") == r.ly_do))  # dòng của nhóm
             .orderBy("khoa").limit(10))  # 10 dòng đầu theo khoá (Spark giữ top-10, không sắp xếp toàn bộ)
        mau = m if mau is None else mau.unionByName(m)  # ghép các nhóm
    mau = mau.orderBy("loai", "nguon", "ly_do", "khoa").toPandas()  # sắp và đưa về pandas
    nc.xuat("spark_quarantine_mau.csv", mau, S, note="toi da 10 dong mau moi (loai, nguon, ly do)")  # vào hàng chờ

    # ---------- 2.10 Bảo toàn tổng dân (đối soát) ----------
    t = pop_gan.filter("pop_raw >= 0").groupBy((F.col("_ly_do") == "").alias("sach")).agg(F.sum("pop_raw").alias("s")).toPandas()  # tổng dân pixel >= 0 theo sạch / không sạch
    s_sach, s_bo = float(t.loc[t.sach, "s"].sum()), float(t.loc[~t.sach, "s"].sum())  # tổng dân dòng sạch, dòng bị loại
    s_tho = float(pop_tho.filter("pop_raw >= 0").agg(F.sum("pop_raw")).first()[0])  # tổng dân thô (pixel >= 0, mọi năm)
    nc.kiem("Tong dan tho (pixel >= 0) = sach + bi loai", round(s_sach + s_bo), round(s_tho), abs(s_sach + s_bo - s_tho) <= 1e-6 * s_tho,  # đối soát
            f"sach {s_sach:.0f}; ngoai 56 xa {s_bo:.0f}")  # (tiếp) ghi chú

    # ---------- 2.11 Quy tắc tổng dân ±3% so với số liệu chính thức NQ 1251 (chỉ cảnh báo) ----------
    xa_nam = (pop.filter(F.col("year").isin(BASE_YEAR - 1, BASE_YEAR)).join(o_luoi.select("row", "col", "ma_tram"), ["row", "col"])  # dân 2023, 2024 nối với xã
              .groupBy("ma_tram", "year").agg(F.sum("pop").alias("pop")).toPandas())  # tổng theo xã x năm -> pandas
    k2m = {norm_name(r.ten_xa_phuong): r.ma_tram for r in dm.select("ten_xa_phuong", "ma_tram").collect()}  # tên chuẩn hoá -> mã xã
    dong = []  # các dòng so sánh
    for nam in (BASE_YEAR - 1, BASE_YEAR):  # hai năm gần mốc số liệu NQ 1251
        cy = xa_nam[xa_nam.year == nam].set_index("ma_tram")["pop"]  # dân theo xã của năm đó
        for _, r in nq_ds.iterrows():  # từng đơn vị / nhóm có số liệu chính thức
            mas = [k2m.get(norm_name(x)) for x in str(r.thanh_phan).split(";")]  # các xã thành phần
            if None in mas:  # có xã không khớp danh mục
                continue  # bỏ qua dòng này
            wp = float(cy.reindex(mas).sum())  # tổng dân WorldPop của các xã thành phần
            ct = float(r.dan_so)  # dân số chính thức
            lech = 100 * (wp / ct - 1)  # độ lệch phần trăm
            dong.append(dict(nam_worldpop=nam, doi_tuong=r.doi_tuong, loai=r.loai, dan_chinh_thuc=round(ct), dan_worldpop=round(wp),  # dòng so sánh
                             lech_pct=round(lech, 1), nguon=r.nguon_url,  # (tiếp) độ lệch, nguồn
                             trong_3pct=abs(lech) <= POP_TOL_CANH_BAO_PCT, trong_20pct_A11=abs(lech) <= POP_TOL_A11_PCT,  # (tiếp) đạt ngưỡng nào
                             canh_bao="" if abs(lech) <= POP_TOL_CANH_BAO_PCT else "CẦN XEM (chỉ cảnh báo, không cách ly)"))  # (tiếp) nhãn cảnh báo
    oc = pd.DataFrame(dong)  # bảng so sánh
    nc.xuat("qa_official_population.csv", oc, S, note="quy tac +-3% chi canh bao (WorldPop la uoc tinh mo hinh); moc so lieu NQ 1251 chua xac dinh nam")  # vào hàng chờ
    for nam, d in oc.groupby("nam_worldpop"):  # tổng hợp theo năm
        n3 = int(d.trong_3pct.sum())  # số bản ghi trong ±3%
        nc.qa_them("Lệch tổng", f"WorldPop {nam}: bản ghi lệch số liệu NQ 1251 trong ±3% (quy tắc thiết kế)", f"{n3}/{len(d)}", n3 == len(d),  # dòng QA
                   "chỉ cảnh báo, không cách ly: WorldPop là ước tính mô hình (#Truthmode)")  # (tiếp) ghi chú
        nc.qa_them("Lệch tổng", f"WorldPop {nam} so với số liệu NQ 1251 ({len(d)} đơn vị/nhóm): MAPE (%)", round(d.lech_pct.abs().mean(), 1),  # dòng QA ±20%
                   d.lech_pct.abs().mean() < POP_TOL_A11_PCT, "ngưỡng chấp nhận ±20% (A11)")  # (tiếp)
        log.info("2. WorldPop %d vs NQ 1251: %d/%d ban ghi trong +-3%%, MAPE %.1f%%", nam, n3, len(d), d.lech_pct.abs().mean())  # in

    # ---------- 2.12 Giải phóng bộ nhớ đệm của bảng thô (bảng sạch vẫn giữ) ----------
    for df in (dm, o_luoi, pop, k1, tyt, cskcb, nut, canh):  # bảo đảm mọi bảng sạch đã nạp bộ nhớ đệm trước khi bỏ bảng thô
        df.count()  # hành động nhỏ trên bảng đã cache: nạp nốt phần chưa nạp
    for df in list(tho.values()) + gan_cache:  # bảng thô (Bước 1) và bảng đã gắn lý do
        df.unpersist()  # giải phóng: các bước sau chỉ dùng bảng sạch -> bớt RAM (quan trọng khi chạy YARN với executor 1 GB)

    return dict(dm=dm, o_luoi=o_luoi, pop=pop, k1=k1, tyt=tyt, cskcb=cskcb, nut=nut, canh=canh,  # trả các bảng sạch cho Bước 3
                nq_ds=nq_ds, nq_sx=nq_sx, tt30=tt30, s_sach=s_sach, ranh_gioi=d1["ranh_gioi"],  # (tiếp) bảng tham chiếu, tổng dân sạch, ranh giới
                n_pop_sach=int(so.set_index("nguon").loc["pop_100m_raw", "so_dong_hop_le"]))  # (tiếp) số dòng dân số sạch
