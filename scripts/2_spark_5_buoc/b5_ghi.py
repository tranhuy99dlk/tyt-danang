# -*- coding: utf-8 -*-
"""BƯỚC 5 · GHI (Write / Export) – khung 5 bước của pipeline Spark.

5.1 Ghi tầng Gold: mọi bảng kết quả phân tích Bước 4 đã gửi vào hàng chờ, dạng Parquet nén Snappy; bảng lớn chia thư mục
    (fact_health_access_yearly theo year, fact_khoang_cach_o_2024 theo ma_quan), bảng nhỏ ghi 1 file.
5.2 Ghi bảng CSV phục vụ báo cáo / Excel (outputs/tables/<thư mục bước>/): mỗi file đăng ký vào outputs/run_manifest.csv
    (script tạo ra, số dòng, SHA-256) để truy vết.
5.3 Xuất hai bảng lưới phục vụ người dùng Excel / GIS: dan_cu_luoi_100m.csv, tiep_can_luoi_100m.csv.
5.4 Đo định dạng lưu trữ: cùng một truy vấn KPI trên Parquet phân vùng và trên CSV – bằng chứng cho lựa chọn Parquet.
5.5 Kiểm tra ghi: mọi bảng Parquet có cờ _SUCCESS (Parquet không có giao dịch); ghi qa_summary.csv và doi_soat_spark.csv.
Tầng Silver đã được ghi ở Bước 2 (bảng cách ly) và Bước 3 (dữ liệu sạch, ghi rồi đọc lại làm điểm lưu) – xem lý do ở
docs/KIEN_TRUC.md. Không làm dashboard: bài toán ra quyết định theo năm, ít người dùng (lý do ở docs/BAO_CAO_7_GIAI_DOAN.md,
Giai đoạn 6); bảng CSV + hình + Gold là đủ, Gold đọc được bằng mọi công cụ BI nếu sau này cần dashboard.
"""
import re  # biểu thức chính quy (đọc kế hoạch thực thi)

import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu

from common import bang, LOGS, BASE_YEAR, YEARS, MAIN_KM, norm_name, register, save_csv  # tham số và hàm dùng chung

S = "b5_ghi"  # tên bước ghi trong sổ run_manifest
B = BASE_YEAR  # năm hiện trạng
R = MAIN_KM * 1000  # 3 km (mét)


def chay(nc, d2, d3, d4):  # chạy Bước 5; d2/d3/d4 = kết quả Bước 2/3/4
    log = nc.log  # bộ ghi log
    # ================= 5.1 GHI TẦNG GOLD =================
    for df, ten, part in nc.gold_cho:  # từng bảng Gold trong hàng chờ
        nc.ghi(df, ten, part)  # ghi Parquet nén Snappy (chia thư mục nếu có cột phân vùng)
    log.info("5. Ghi %d bang Gold (Parquet, Snappy)", len(nc.gold_cho))  # in
    # ================= 5.2 GHI BẢNG CSV =================
    for ten, (pdf, script, note) in nc.csv_cho.items():  # từng bảng CSV trong hàng chờ
        save_csv(pdf, bang(ten), script, note=note)  # ghi CSV (utf-8-sig cho Excel) và đăng ký vào run_manifest.csv
    log.info("5. Ghi %d bang CSV vao outputs/tables/", len(nc.csv_cho))  # in
    # ================= 5.3 XUẤT BẢNG LƯỚI PHỤC VỤ =================
    xuat_luoi(nc, d3, d4["goc"])  # hai bảng lưới 100 m cho Excel / GIS
    # ================= 5.4 ĐO PARQUET PHÂN VÙNG SO VỚI CSV =================
    do_dinh_dang(nc)  # xem hàm bên dưới
    # ================= 5.5 KIỂM TRA GHI VÀ TỔNG KẾT =================
    n_ok = sum(nc.co_success(p) for p in nc.da_ghi)  # số bảng Parquet có cờ _SUCCESS
    nc.kiem("So bang Parquet co _SUCCESS = so bang da ghi", n_ok, len(nc.da_ghi), n_ok == len(nc.da_ghi), "Parquet khong co giao dich")  # đối soát
    tong_ket(nc, d2)  # ghi qa_summary.csv và doi_soat_spark.csv


def xuat_luoi(nc, d3, goc):  # xuất lưới 100 m ra CSV (đọc bằng Excel); goc = ô có dân kèm ô 1 km năm 2024 (từ 4.4)
    F = nc.F  # hàm cột Spark
    fact, o_dan, xa, a = d3["fact"], d3["o_dan"], d3["xa"], d3["a"]  # bảng Silver từ Bước 3
    pv = fact.groupBy("cell_id").pivot("year", YEARS).agg(F.first("pop"))  # xoay bảng: mỗi năm một cột dân số
    for y in YEARS:  # đổi tên cột năm
        pv = pv.withColumnRenamed(str(y), f"pop_{y}")  # "2015" -> "pop_2015"
    lw = (o_dan.select("cell_id", "lon", "lat", "ma_tram", "ten_day_du", "quan_huyen", "loai_dv", "elev_m", "slope_deg", "k1_row", "k1_col")  # thông tin ô
          .join(F.broadcast(xa.select("ma_tram", "nhom")), "ma_tram").join(pv, "cell_id", "left")  # + nhóm, dân các năm
          .join(goc.select("cell_id", "kB", "sB"), "cell_id", "left")  # + ô 1 km năm 2024
          .withColumn("share60_src", F.when(F.col("kB") > 0, "1km").otherwise("xa_tb"))  # nguồn tỷ lệ 60+
          .withColumnRenamed("sB", f"share60_{B}").withColumn(f"pop60_{B}", F.coalesce(F.col(f"pop_{B}"), F.lit(0.0)) * F.col(f"share60_{B}"))  # tỷ lệ và dân 60+
          .fillna(0, [f"pop_{y}" for y in YEARS]).orderBy("cell_id").toPandas())  # năm thiếu -> 0; về pandas
    cot1 = ["cell_id", "lon", "lat", "ma_tram", "ten_day_du", "quan_huyen", "loai_dv", "nhom", *[f"pop_{y}" for y in YEARS],  # cột file dân cư (giữ tên cũ)
            "share60_src", f"share60_{B}", f"pop60_{B}", "elev_m", "slope_deg"]  # (tiếp)
    lam_tron = {"lon": 6, "lat": 6, **{f"pop_{y}": 6 for y in YEARS}, f"share60_{B}": 6, f"pop60_{B}": 6, "elev_m": 1, "slope_deg": 2}  # số chữ số giữ lại
    save_csv(lw[cot1].round(lam_tron), bang("dan_cu_luoi_100m.csv"), S, note="luoi dan cu 100 m tu raster WorldPop (A2, A3)")  # ghi CSV
    tc = a.select("cell_id", "ma_tram", F.col("pop").alias(f"pop_{B}"), F.col("pop60").alias(f"pop60_{B}"), "dist_m", "tram_gan_nhat",  # cột file tiếp cận
                  F.col("dist_m_chi_thuc").alias("dist_m_chi_osm"), "net_dist_m", "net_time_min", "net_station", "snap_cell_m", "detour",  # (tiếp) giữ tên cột cũ
                  "kc_cskcb_m", "cskcb_gan_nhat", "kc_cskcb_duong_m", "tg_cskcb_phut").orderBy("cell_id").toPandas()  # (tiếp) cột mới
    save_csv(tc.round({"dist_m": 1, "dist_m_chi_osm": 1, "net_dist_m": 1, "net_time_min": 2, "snap_cell_m": 1, "detour": 3,  # ghi CSV
                       "kc_cskcb_m": 1, "kc_cskcb_duong_m": 1, "tg_cskcb_phut": 2}), bang("tiep_can_luoi_100m.csv"), S,  # (tiếp)
             note="khoang cach chim bay + mang duong moi o toi TYT va CSKCB (A1, A5, A6)")  # (tiếp)


def do_dinh_dang(nc):  # cùng một truy vấn KPI trên 4 cách lưu: đếm file, byte, dòng mà bước quét thực sự đọc
    F, T, spark, log = nc.F, nc.T, nc.spark, nc.log  # công cụ Spark
    spark.catalog.clearCache()  # xoá mọi bộ nhớ đệm: nếu không, Spark đọc bản trong bộ nhớ (từ Bước 3) thay vì quét file
    p_pq = f"{nc.goc}/silver/fact_o_nam_khoang_cach"  # bảng sự kiện Parquet chia thư mục year/ma_quan
    fact = nc.doc("silver/fact_o_nam_khoang_cach")  # đọc bảng sự kiện
    xa = nc.doc("silver/dim_xa").select("ma_tram", "nhom")  # bảng xã nhỏ
    p_csv, p_csvp = f"{nc.goc}/thu_nghiem/csv_phang", f"{nc.goc}/thu_nghiem/csv_phan_vung"  # hai bản sao CSV
    fact.write.mode("overwrite").option("header", True).csv(p_csv)  # bản sao CSV một thư mục phẳng (giống Bronze)
    fact.write.mode("overwrite").option("header", True).partitionBy("year").csv(p_csvp)  # bản sao CSV chia thư mục theo năm
    sch = fact.schema  # schema tường minh (không suy kiểu)
    fact_csv = spark.read.option("header", True).schema(T.StructType([f for f in sch.fields if f.name not in ("year", "ma_quan")] +  # đọc CSV phẳng
                                                                     [sch["year"], sch["ma_quan"]])).csv(p_csv)  # (tiếp) thứ tự cột của file CSV
    fact_csvp = spark.read.option("header", True).schema(T.StructType([f for f in sch.fields if f.name not in ("year", "ma_quan")] + [sch["ma_quan"]])).csv(p_csvp)  # year lấy từ tên thư mục

    def truy_van(df):  # truy vấn nghiệp vụ: % dân trong 3 km theo nhóm, năm 2024
        return (df.filter((F.col("year") == B) & (F.col("pop") > 0))  # lọc (phép biến đổi hẹp)
                .join(F.broadcast(xa), "ma_tram")  # broadcast hash join với bảng xã nhỏ
                .groupBy("nhom")  # gom nhóm (phép biến đổi rộng)
                .agg((F.sum(F.when(F.col("dist_m") <= R, F.col("pop")).otherwise(0.0)) / F.sum("pop") * 100).alias("ty_le_trong_3km_pct")))  # tỷ lệ

    def ke_hoach(df):  # lấy chuỗi kế hoạch thực thi
        import contextlib, io  # noqa: E401  chuyển hướng màn hình
        buf = io.StringIO()  # bộ đệm
        with contextlib.redirect_stdout(buf):  # hứng chữ in ra
            df.explain("formatted")  # in kế hoạch
        return buf.getvalue()  # trả chuỗi

    def dung_luong(p):  # tổng dung lượng và số file dữ liệu (part-*) của một thư mục
        it = nc._fs(p).listFiles(nc._Path(p), True)  # duyệt đệ quy
        s = n = 0  # tổng byte, số file
        while it.hasNext():  # từng file
            f = it.next()  # thông tin file
            if f.getPath().getName().startswith("part-"):  # chỉ file dữ liệu
                s += int(f.getLen()); n += 1  # cộng dồn
        return s, n  # trả kết quả

    gold = pd.read_csv(bang("kpi_theo_nhom_spark.csv"), encoding="utf-8-sig").set_index("nhom")["ty_le_trong_3km_pct"]  # kết quả chuẩn (làm tròn 3 chữ số)
    n_sau_loc = fact.filter((F.col("year") == B) & (F.col("pop") > 0)).count()  # số dòng sau lọc
    pa = [("P1", "Parquet chia thư mục theo năm và quận, đọc thẳng", "Parquet", True, fact, p_pq),  # 4 phương án
          ("P2", "CSV một thư mục phẳng (như Bronze)", "CSV", False, fact_csv, p_csv),  # (tiếp)
          ("P3", "CSV chia thư mục theo năm", "CSV", True, fact_csvp, p_csvp),  # (tiếp)
          ("P4", "Parquet chia thư mục, nhưng cache() trước khi lọc", "Parquet", True, None, p_pq)]  # (tiếp) chạy cuối vì cache ảnh hưởng truy vấn khác
    rows, plans, n_cache, df_cache = [], [], None, None  # kết quả; kế hoạch; số dòng nạp cache
    for ma, mt, dd, pv, df, path in pa:  # từng phương án
        if df is None:  # P4: nạp bộ nhớ đệm (lần nạp quét cả 10 năm)
            df_cache = nc.doc("silver/fact_o_nam_khoang_cach").cache(); n_cache = df_cache.count(); df = df_cache  # nạp cache
        q = truy_van(df)  # truy vấn
        plan = ke_hoach(q)  # kế hoạch (AQE bật)
        plans.append(f"{'=' * 100}\n# {ma} - {mt}\n{'=' * 100}\n{plan}")  # lưu kế hoạch
        spark.conf.set("spark.sql.adaptive.enabled", "false")  # tắt AQE để đọc chỉ số trên kế hoạch vật lý cuối
        q2 = truy_van(df)  # dựng lại truy vấn
        res = q2.toPandas().set_index("nhom")["ty_le_trong_3km_pct"]  # chạy và lấy kết quả
        la = q2._jdf.queryExecution().executedPlan().collectLeaves()  # các nút lá (bước quét) của kế hoạch
        so_file = byte_q = dong_q = None  # chỉ số quét
        for i in range(la.size()):  # duyệt nút lá
            nd = la.apply(i)  # nút
            try:  # bỏ qua nút quét bảng xã nhỏ
                if "dim_xa" in str(nd.metadata()):  # nút đọc dim_xa
                    continue  # bỏ qua
            except Exception:  # nút đọc bộ nhớ đệm không có metadata
                pass  # tiếp tục
            m = nd.metrics()  # chỉ số của nút

            def gt(k):  # lấy một chỉ số nếu có
                return int(m.apply(k).value()) if m.contains(k) else None  # giá trị hoặc rỗng
            if str(nd.nodeName()).startswith(("Scan", "InMemoryTableScan")):  # nút quét bảng sự kiện
                so_file, byte_q, dong_q = gt("numFiles"), gt("filesSize"), gt("numOutputRows"); break  # số file, byte, dòng ra
        spark.conf.set("spark.sql.adaptive.enabled", "true")  # bật lại AQE
        dl, nf = dung_luong(path)  # dung lượng cả bảng
        lech = float((res - gold.reindex(res.index)).abs().max())  # lệch so với bảng chuẩn
        pf = re.search(r"PartitionFilters: \[([^\]]+)\]", plan)  # bộ lọc phân vùng KHÔNG rỗng trong kế hoạch ("[]" = không lọc)
        pu = re.search(r"PushedFilters: \[([^\]]+)\]", plan)  # bộ lọc đẩy xuống nguồn, không rỗng
        cay = plan.split("\n\n")[0]  # cây kế hoạch
        join = next((j for j in ("BroadcastHashJoin", "SortMergeJoin", "BroadcastNestedLoopJoin") if j in cay), "khong co")  # kiểu join
        rows.append(dict(phuong_an=ma, mo_ta=mt, dinh_dang=dd, chia_thu_muc_theo_nam=pv, dung_luong_ca_10_nam_byte=dl, so_file_ca_10_nam=nf,  # dòng kết quả
                         so_file_quet=so_file, dung_luong_file_quet_byte=byte_q, so_dong_ra_tu_buoc_quet=dong_q, so_dong_sau_loc=n_sau_loc,  # (tiếp)
                         partition_filters=f"[{pf.group(1)}]" if pf else "", pushed_filters=f"[{pu.group(1)}]" if pu else "", loai_join=join,  # (tiếp)
                         so_exchange=len(re.findall(r"^\(\d+\) Exchange", plan, flags=re.M)), doc_qua_bo_nho_dem="InMemoryTableScan" in cay,  # (tiếp)
                         ty_le_3km_phuong=round(float(res.get("Phường", np.nan)), 6), ty_le_3km_dong_bang=round(float(res.get("Xã đồng bằng/ven đô", np.nan)), 6),  # (tiếp)
                         ty_le_3km_mien_nui=round(float(res.get("Xã miền núi (địa hình)", np.nan)), 6),  # (tiếp)
                         lech_voi_kpi_theo_nhom_spark=round(lech, 6), khop_kpi_theo_nhom_spark=lech <= 0.001))  # (tiếp)
    if df_cache is not None:  # giải phóng bộ nhớ đệm
        df_cache.unpersist()  # xoá cache
    out = pd.DataFrame(rows)  # bảng so sánh
    for c in ("so_file_quet", "dung_luong_file_quet_byte", "so_dong_ra_tu_buoc_quet"):  # cột có thể rỗng
        out[c] = out[c].astype("Int64")  # kiểu số nguyên cho phép rỗng
    b1 = out.loc[out.phuong_an == "P1", "dung_luong_file_quet_byte"].iloc[0]  # byte quét của P1
    out["byte_quet_gap_bao_nhieu_lan_P1"] = (out.dung_luong_file_quet_byte / b1).round(2)  # so với P1
    out["ghi_chu"] = np.where(out.phuong_an == "P4", f"Đọc từ bộ nhớ đệm; lần nạp cache() trước đó đã quét cả bảng ({n_cache} dòng)", "")  # ghi chú P4
    save_csv(out, bang("so_sanh_parquet_csv.csv"), S, note=f"cung truy van KPI 3 km theo nhom nam {B} tren 4 cach luu")  # ghi CSV
    f = LOGS / f"plan_pruning_{B}.txt"  # file kế hoạch
    f.write_text(f"# Ke hoach thuc thi cua cung mot truy van tren 4 cach luu, Spark {spark.version}\n\n" + "\n".join(plans), encoding="utf-8")  # ghi
    register(f, S, rows=sum(p.count("\n") for p in plans), note="explain formatted cua 4 phuong an luu tru")  # đăng ký
    nc.kiem("4 cach luu cho cung ket qua va khop kpi_theo_nhom_spark.csv", int(out.khop_kpi_theo_nhom_spark.sum()), 4, bool(out.khop_kpi_theo_nhom_spark.all()))  # đối soát
    nc._fs(f"{nc.goc}/thu_nghiem").delete(nc._Path(f"{nc.goc}/thu_nghiem"), True)  # xoá hai bản sao CSV thử nghiệm (~88 MB) sau khi đo xong
    for r in rows:  # in tóm tắt
        log.info("5. %s %-48s | file quet %s | byte %s | PartitionFilters=%s | join=%s", r["phuong_an"], r["mo_ta"], r["so_file_quet"],  # dòng log
                 r["dung_luong_file_quet_byte"], "co" if r["partition_filters"] else "KHONG", r["loai_join"])  # (tiếp)


def tong_ket(nc, d2):  # ghi qa_summary.csv và doi_soat_spark.csv (cuối Bước 5); d2 = kết quả Bước 2
    qb = pd.read_csv(bang("qa_boundaries.csv"), encoding="utf-8-sig")  # QA ranh giới (bước 1c)
    nc.qa_them("Thiếu", "Danh sách trạm đủ 56 dòng", int(d2["dm"].count()), d2["dm"].count() == 56)  # QA
    nc.qa_them("Thiếu", "Xã có ranh giới", int((qb.nguon_dung != "THIEU").sum()), bool((qb.nguon_dung != "THIEU").all()))  # QA
    ten = [norm_name(r.ten_xa_phuong) for r in d2["dm"].select("ten_xa_phuong").collect()]  # tên xã chuẩn hoá
    nc.qa_them("Trùng", "Tên xã/phường trùng sau chuẩn hoá", len(ten) - len(set(ten)), len(ten) == len(set(ten)))  # QA
    if qb.iou_osm_gadm.notna().any():  # có IoU
        nc.qa_them("Sai toạ độ", "Ranh giới OSM vs GADM: IoU trung vị", round(qb.iou_osm_gadm.median(), 3), qb.iou_osm_gadm.median() > 0.8)  # QA
        nc.qa_them("Sai toạ độ", "Xã có IoU OSM-GADM < 0,7", int((qb.iou_osm_gadm < 0.7).sum()), int((qb.iou_osm_gadm < 0.7).sum()) == 0,  # QA
                   ", ".join(qb.loc[qb.iou_osm_gadm < 0.7, "ten_xa_phuong"]))  # (tiếp)
    sd = pd.DataFrame(nc.so_dong)  # số dòng theo nguồn
    nc.qa_them("Lỗi dữ liệu", "Dòng lỗi dữ liệu thật (silver/quarantine_records)", int(sd.so_dong_loi_du_lieu.sum()), True,  # QA
               "; ".join(f"{r.nguon}: {r.so_dong_loi_du_lieu}" for r in sd.itertuples() if r.so_dong_loi_du_lieu))  # (tiếp)
    thu_tu = {"Thiếu": 0, "Trùng": 1, "Sai toạ độ": 2, "Lệch tổng": 3, "Lỗi dữ liệu": 4}  # thứ tự nhóm
    qa = pd.DataFrame(nc.qa)  # bảng QA
    qa = qa.iloc[qa.nhom.map(thu_tu).argsort(kind="stable")]  # sắp theo nhóm, giữ thứ tự trong nhóm
    save_csv(qa, bang("qa_summary.csv"), S, note="tong hop kiem tra chat luong")  # ghi CSV
    ds = pd.DataFrame(nc.doi_soat)  # bảng đối soát
    save_csv(ds, bang("doi_soat_spark.csv"), S, note="bao toan so dong/tong dan, Spark SQL vs DataFrame, so voi bo so 19/11/2026")  # ghi CSV
    nc.log.info("QA: %d kiem tra, %d CAN XEM", len(qa), int((qa.dat == "CẦN XEM").sum()))  # in
    for r in ds[ds.ket_qua != "ĐẠT"].itertuples():  # in từng phép lệch
        nc.log.warning("LECH | %s | moi=%s doi_chieu=%s", r.phep_doi_soat, r.spark, r.pandas_hoac_nguon)  # dòng log
    nc.log.info("Doi soat: %d phep, %d LECH.", len(ds), int((ds.ket_qua == "LỆCH").sum()))  # dòng tổng kết


def ghi_thoi_gian(nc):  # ghi thời gian từng bước (gọi sau khi Bước 5 kết thúc để có đủ 5 dòng)
    tg = pd.DataFrame(nc.thoi_gian)  # bảng thời gian
    tg["ty_le_thoi_gian_pct"] = (100 * tg.thoi_gian_giay / tg.thoi_gian_giay.sum()).round(1)  # mỗi bước chiếm bao nhiêu %
    save_csv(tg, bang("thoi_gian_5_buoc.csv"), S, note=f"thoi gian 5 buoc Spark, master={nc.spark.sparkContext.master}")  # ghi CSV
    for r in tg.itertuples():  # in tóm tắt
        nc.log.info("Buoc %d %-22s %6.1f giay (%4.1f%%), %d job Spark", r.buoc, r.ten_buoc, r.thoi_gian_giay, r.ty_le_thoi_gian_pct, r.so_job_spark)  # dòng log
