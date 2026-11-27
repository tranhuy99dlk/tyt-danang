# -*- coding: utf-8 -*-
"""BƯỚC 1 · ĐỌC (Ingest / Read) – khung 5 bước của pipeline Spark.

Đọc mọi bảng của tầng Bronze (lần nạp mới nhất của từng nguồn) với LƯỢC ĐỒ (schema) KHAI BÁO TƯỜNG MINH:
  - 8 bảng CSV chính: khai báo StructType cho từng cột (tên, kiểu). Spark KHÔNG tự đoán kiểu (không inferSchema),
    chế độ PERMISSIVE: dòng sai kiểu hoặc sai số cột không làm hỏng lần đọc mà rơi vào cột `_corrupt_record`
    để Bước 2 tách ra bảng cách ly.
  - 3 bảng tham chiếu nhỏ (số liệu NQ 1251, danh sách sắp xếp, gói dịch vụ TT 30): đọc mọi cột là chuỗi.
  - 1 file GeoJSON ranh giới 56 xã: đọc nguyên văn (wholetext) rồi phân tích JSON trên driver.
Bước này chỉ ĐỌC và ĐẾM, không sửa giá trị nào. Mỗi bảng được nạp vào bộ nhớ đệm (cache) để Bước 2 không phải đọc
lại file. Danh mục những gì đã đọc ghi ở 1_du_lieu_nguon/spark_bang_da_doc.csv (qua hàng chờ, Bước 5 ghi).

Vì sao chỉ có nguồn file: dữ liệu của bài toán là file mở (raster WorldPop, OSM, Excel nhập tay) đã được phần thu thập
(scripts/1_thu_thap, Python) chuyển đổi thành CSV/GeoJSON ở Bronze. Không có nguồn Database hay luồng sự kiện (Event) thật; cách gắn thêm
các nguồn đó vào cùng khung này mô tả ở docs/BAO_CAO_7_GIAI_DOAN.md (Giai đoạn 2).
"""
import json  # phân tích nội dung GeoJSON

import pandas as pd  # bảng dữ liệu trên driver (danh mục bảng đã đọc)

S = "b1_doc"  # tên bước ghi trong sổ run_manifest
CORRUPT = "_corrupt_record"  # tên cột chứa nguyên văn dòng không đọc được theo schema

BANG_CSV = [  # 8 bảng CSV chính: (nguồn = thư mục source_system, tên bảng)
    ("danh_muc", "dm_56_tram"),  # danh mục 56 trạm / xã
    ("tra_cuu_khong_gian", "o_luoi_100m"),  # bảng tra cứu pixel 100 m (xã, độ cao, nút đường)
    ("worldpop", "pop_100m_raw"),  # dân số lưới 100 m x 10 năm (khoảng 3 triệu dòng)
    ("worldpop", "pop_1km_raw"),  # lưới 1 km: tổng dân + 7 nhóm tuổi 60+
    ("co_so_y_te", "tyt_raw"),  # toạ độ trạm y tế nhập tay (nguyên văn Excel)
    ("co_so_y_te", "cskcb_raw"),  # danh sách cơ sở khám chữa bệnh (nguyên văn Excel)
    ("tra_cuu_khong_gian", "duong_nut"),  # nút đồ thị đường OSM
    ("tra_cuu_khong_gian", "duong_canh"),  # cạnh đồ thị đường OSM
]  # hết danh sách
BANG_THAM_CHIEU = ["dan_so_nq1251", "nq1251_sap_xep", "tt30_goi_dich_vu"]  # 3 bảng tham chiếu nhỏ (nguồn danh_muc)


def khai_bao_schema(T):  # schema khai báo cho từng bảng CSV chính – KHÔNG để Spark tự đoán kiểu
    I, D, St = T.IntegerType(), T.DoubleType(), T.StringType()  # ba kiểu dữ liệu hay dùng: số nguyên, số thực, chuỗi

    def schema(*cot):  # tạo schema từ danh sách (tên cột, kiểu) và thêm cột _corrupt_record
        return T.StructType([T.StructField(n, t, True) for n, t in cot] + [T.StructField(CORRUPT, St, True)])  # mọi cột cho phép rỗng

    return {  # bảng schema
        ("danh_muc", "dm_56_tram"): schema(("ma_tram", St), ("quan_huyen", St), ("loai_dv", St), ("ten_xa_phuong", St),  # danh mục 56 trạm
                                           ("ten_day_du", St), ("ten_tram", St), ("nguon", St)),  # (tiếp)
        ("tra_cuu_khong_gian", "o_luoi_100m"): schema(("row", I), ("col", I), ("cell_id", St), ("lon", D), ("lat", D),  # pixel 100 m
                                                      ("x_m", D), ("y_m", D), ("ma_tram", St), ("k1_row", I), ("k1_col", I),  # (tiếp)
                                                      ("elev_m", D), ("slope_deg", D), ("nut_duong", I), ("kc_ra_duong_m", D)),  # (tiếp)
        ("worldpop", "pop_100m_raw"): schema(("year", I), ("row", I), ("col", I), ("pop_raw", D)),  # dân số 100 m thô
        ("worldpop", "pop_1km_raw"): schema(("year", I), ("k1_row", I), ("k1_col", I), ("bien", St), ("gia_tri", D)),  # lưới 1 km thô
        ("co_so_y_te", "tyt_raw"): schema(("dong_excel", I), ("cot1", St), ("cot2", St), ("cot3", St)),  # toạ độ trạm nhập tay
        ("co_so_y_te", "cskcb_raw"): schema(("dong_excel", I), ("stt", St), ("ma_cs", St), ("ten_cs", St), ("tuyen", St),  # danh sách CSKCB
                                            ("hang", St), ("cap", St), ("diem", St), ("dia_chi", St), ("toa_do", St)),  # (tiếp)
        ("tra_cuu_khong_gian", "duong_nut"): schema(("nut", I), ("x_m", D), ("y_m", D)),  # nút đồ thị đường
        ("tra_cuu_khong_gian", "duong_canh"): schema(("i", I), ("j", I), ("dai_m", D), ("phut", D)),  # cạnh đồ thị đường
    }  # hết bảng schema


def chay(nc):  # chạy Bước 1; nc là đối tượng NguCanh (spark_chung.py)
    F, T, spark, log = nc.F, nc.T, nc.spark, nc.log  # lấy các công cụ từ ngữ cảnh cho gọn
    SCHEMA = khai_bao_schema(T)  # schema của 8 bảng CSV chính
    tho, danh_muc = {}, []  # bảng thô đã đọc; danh mục những gì đã đọc

    def ngay_nap(p):  # lấy ngày nạp từ đường dẫn .../ingestion_date=YYYY-MM-DD/...
        return p.split("ingestion_date=")[1].split("/")[0]  # phần giữa "ingestion_date=" và dấu "/" kế tiếp

    for nguon, ten in BANG_CSV:  # từng bảng CSV chính
        p = nc.bronze(nguon, f"{ten}.csv")  # đường dẫn file của lần nạp mới nhất
        df = (spark.read.option("header", True)  # dòng đầu là tiêu đề
              .option("mode", "PERMISSIVE")  # dòng hỏng không làm dừng, mà đưa vào cột _corrupt_record
              .option("columnNameOfCorruptRecord", CORRUPT)  # tên cột hứng dòng hỏng
              .schema(SCHEMA[(nguon, ten)])  # schema khai báo
              .csv(p)  # đọc file CSV
              .cache())  # giữ trong bộ nhớ: Bước 2 dùng lại nhiều lần mà không đọc lại file
        n = df.count()  # hành động (action) đầu tiên: Spark đọc thật, nạp bộ nhớ đệm và đếm dòng
        tho[ten] = df  # lưu bảng thô
        danh_muc.append(dict(nguon=nguon, bang=ten, dinh_dang="CSV", ngay_nap=ngay_nap(p),  # dòng danh mục
                             so_cot_schema=len(SCHEMA[(nguon, ten)].fields) - 1, kieu_doc="schema khai báo, PERMISSIVE + _corrupt_record",  # (tiếp)
                             so_dong_doc=n))  # (tiếp) số dòng đọc được
        log.info("1. %-18s %-15s %9d dong (%d cot theo schema)", nguon, ten, n, len(SCHEMA[(nguon, ten)].fields) - 1)  # in

    tham_chieu = {}  # bảng tham chiếu nhỏ
    for ten in BANG_THAM_CHIEU:  # từng bảng tham chiếu
        p = nc.bronze("danh_muc", f"{ten}.csv")  # đường dẫn
        df = spark.read.option("header", True).csv(p).withColumn(CORRUPT, F.lit(None).cast(T.StringType()))  # không suy kiểu: mọi cột là chuỗi; thêm cột rỗng cho đồng bộ
        tham_chieu[ten] = df  # lưu
        danh_muc.append(dict(nguon="danh_muc", bang=ten, dinh_dang="CSV", ngay_nap=ngay_nap(p), so_cot_schema=len(df.columns) - 1,  # dòng danh mục
                             kieu_doc="mọi cột là chuỗi (bảng tham chiếu nhỏ)", so_dong_doc=df.count()))  # (tiếp)

    p = nc.bronze("tra_cuu_khong_gian", "ranh_gioi_56_xa.geojson")  # file ranh giới
    ranh_gioi = json.loads(spark.read.text(p, wholetext=True).first()[0])  # đọc cả file thành một chuỗi rồi phân tích JSON trên driver
    danh_muc.append(dict(nguon="tra_cuu_khong_gian", bang="ranh_gioi_56_xa", dinh_dang="GeoJSON", ngay_nap=ngay_nap(p),  # dòng danh mục
                         so_cot_schema=None, kieu_doc="nguyên văn (wholetext), phân tích JSON trên driver",  # (tiếp)
                         so_dong_doc=len(ranh_gioi["features"])))  # (tiếp) số đa giác
    log.info("1. Doc %d bang Bronze (%d dong) + ranh gioi %d xa", len(danh_muc) - 1, sum(d["so_dong_doc"] for d in danh_muc[:-1]),  # in tổng
             len(ranh_gioi["features"]))  # (tiếp)

    dm = pd.DataFrame(danh_muc)  # bảng danh mục
    dm["so_cot_schema"] = dm["so_cot_schema"].astype("Int64")  # số nguyên cho phép rỗng (GeoJSON không có số cột)
    nc.xuat("spark_bang_da_doc.csv", dm, S, note="Buoc 1: bang Bronze da doc, schema, so dong")  # danh mục vào hàng chờ
    return dict(tho=tho, tham_chieu=tham_chieu, ranh_gioi=ranh_gioi, CORRUPT=CORRUPT)  # trả cho Bước 2
