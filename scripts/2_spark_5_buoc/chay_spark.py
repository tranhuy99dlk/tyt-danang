# -*- coding: utf-8 -*-
"""PIPELINE SPARK (PySpark) – KHUNG 5 BƯỚC KỸ THUẬT, phụ trách Bước 02 → 08 của luồng dữ liệu:

  [1. ĐỌC] -> [2. KIỂM TRA & LÀM SẠCH] -> [3. BIẾN ĐỔI] -> [4. TỔNG HỢP] -> [5. GHI]

  Bước 1 · Đọc                b1_doc.py               đọc Bronze (CSV, GeoJSON) với schema khai báo tường minh
  Bước 2 · Kiểm tra & làm sạch b2_kiem_tra_lam_sach.py Null / kiểu / miền giá trị / trùng; tách quarantine và excluded
  Bước 3 · Biến đổi           b3_bien_doi.py          chuẩn hoá (UTM 49N, dân 60+), nối đa nguồn (broadcast, Dijkstra),
                                                      cột nghiệp vụ; ghi Silver làm điểm lưu
  Bước 4 · Tổng hợp           b4_tong_hop.py          KPI groupBy / cửa sổ, vùng lõm, khuyến nghị (b4_khuyen_nghi.py),
                                                      dự báo 2030 (b4_du_bao.py), đối soát với bộ số đã chốt
  Bước 5 · Ghi                b5_ghi.py               ghi Gold (Parquet phân vùng), bảng CSV phục vụ, đo Parquet/CSV,
                                                      kiểm tra _SUCCESS, qa_summary.csv, doi_soat_spark.csv

Tầng lưu trữ (Medallion): data/lake/bronze -> data/lake/silver -> data/lake/gold (hoặc trên HDFS nếu đặt HDFS_URI).
Bảng CSV cho báo cáo: outputs/tables/. Nhật ký: logs/spark_5_buoc_<giờ>.log. Thời gian từng bước: 7_kiem_chung/thoi_gian_5_buoc.csv.

Chạy trên máy (sau 1b, 1c):   python scripts/2_spark_5_buoc/chay_spark.py      (hoặc F5 trong VS Code)
Chạy trên cụm Docker có HDFS:   xem docs/HUONG_DAN_CHAY.md, Phần B (có Docker).
"""
import sys  # danh sách thư mục Python tìm module
from pathlib import Path  # làm việc với đường dẫn
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "chung"))  # thêm scripts/chung/ để import được common.py, hinh_hoc.py
import b1_doc  # Bước 1 · Đọc
import b2_kiem_tra_lam_sach  # Bước 2 · Kiểm tra & làm sạch
import b3_bien_doi  # Bước 3 · Biến đổi
import b4_tong_hop  # Bước 4 · Tổng hợp
import b5_ghi  # Bước 5 · Ghi
from spark_chung import NguCanh  # ngữ cảnh chạy (SparkSession + hàm đọc/ghi + đo thời gian)


def main():  # hàm chính
    nc = NguCanh("spark_5_buoc")  # tạo phiên Spark và ngữ cảnh
    try:  # bảo đảm luôn dừng Spark dù có lỗi
        with nc.buoc(1, "Doc", "doc Bronze voi schema tuong minh"):  # Bước 1
            d1 = b1_doc.chay(nc)  # bảng thô
        with nc.buoc(2, "Kiem tra & lam sach", "Null, kieu, mien gia tri, trung; quarantine / excluded"):  # Bước 2
            d2 = b2_kiem_tra_lam_sach.chay(nc, d1)  # bảng sạch
        with nc.buoc(3, "Bien doi", "chuan hoa, noi da nguon, cot nghiep vu -> Silver"):  # Bước 3
            d3 = b3_bien_doi.chay(nc, d2)  # bảng Silver
        with nc.buoc(4, "Tong hop", "KPI groupBy/cua so, vung lom, khuyen nghi, du bao, doi soat"):  # Bước 4
            d4 = b4_tong_hop.chay(nc, d2, d3)  # kết quả phân tích (đang trong hàng chờ)
        with nc.buoc(5, "Ghi", "Gold Parquet phan vung, bang CSV, kiem tra ghi"):  # Bước 5
            b5_ghi.chay(nc, d2, d3, d4)  # ghi mọi kết quả
        b5_ghi.ghi_thoi_gian(nc)  # ghi bảng thời gian 5 bước (sau khi Bước 5 kết thúc)
    finally:  # luôn chạy
        nc.dung()  # dừng Spark


if __name__ == "__main__":  # chạy trực tiếp
    main()  # gọi hàm chính
