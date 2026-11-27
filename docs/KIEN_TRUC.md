# Tài liệu kiến trúc

Dành cho người cần hiểu dự án được ghép từ những phần nào, dữ liệu đi qua đâu và vì sao thiết kế như vậy.
Cách cài đặt và chạy (không Docker / có Docker): [HUONG_DAN_CHAY.md](HUONG_DAN_CHAY.md). Demo và giới thiệu sản phẩm: [HUONG_DAN_DEMO.md](HUONG_DAN_DEMO.md).

Bản này mô tả pipeline **tinh gọn ngày 26/11/2026**, gồm 8 bước logic (nội dung nghiệp vụ) và kiến trúc lưu trữ Medallion. Từ 27/11/2026,
pipeline Spark được tổ chức theo **khung 5 bước kỹ thuật** Đọc → Kiểm tra & làm sạch → Biến đổi → Tổng hợp → Ghi (thay cho 4 giai đoạn cũ),
và báo cáo trình bày theo **7 giai đoạn vòng đời phân tích dữ liệu lớn**: [BAO_CAO_7_GIAI_DOAN.md](BAO_CAO_7_GIAI_DOAN.md).
Bản cũ 17 bước (pandas tính một lần, Spark tính lại một lần nữa rồi đối soát hai bên) đã xoá khỏi thư mục; vẫn xem được trong lịch sử git,
commit `aa15f50` (ví dụ `git show aa15f50:scripts/06_kpi.py`). Mười bảng kết quả đã chốt ngày 19/11/2026 mà pipeline mới đem ra đối soát
được giữ ở `tests/tham_chieu_19_11_2026/`.

## 1. Bối cảnh và mục tiêu

Đồ án học phần MIS5007 – Dữ liệu lớn và Ứng dụng. Người đọc kết quả là ban lãnh đạo Sở Y tế.
Từ 01/01/2025 Đà Nẵng sắp xếp đơn vị hành chính cấp xã (NQ 1251/NQ-UBTVQH15). Dự án đo mức tiếp cận trạm y tế (TYT) theo bản đồ
**trước** sắp xếp: 56 xã/phường, mỗi xã một trạm, 10 mốc năm 2015–2024 (năm hiện trạng 2024), đặt cạnh 30 cơ sở khám chữa bệnh (CSKCB).

Hệ thống phải trả lời được:

1. Dữ liệu có đủ tin cậy không (thiếu, trùng, sai toạ độ, lệch tổng dân số)?
2. Bao nhiêu phần trăm dân sống trong 3 km tới trạm gần nhất, theo đường chim bay và theo đường đi?
3. Chỉ số phụ: dân bình quân mỗi trạm, khoảng cách trung vị (p50) và đuôi (p95), dân ở xa trên 5 km, người 60+, chênh lệch phường – xã miền núi, khoảng cách tới CSKCB.
4. Nên đặt thêm điểm ở đâu, có trạm nào nên rà soát gộp không.
5. Đến 2030 (dự báo, xử lý riêng) mức tiếp cận thay đổi thế nào nếu giữ nguyên 56 trạm.

## 2. Sơ đồ tổng thể

```
 NGUỒN (data/raw, data/reference)      1 · THU THẬP (scripts/1_thu_thap)      TẦNG BRONZE (data/lake/bronze hoặc HDFS)
┌──────────────────────────────┐   ┌──────────────────────────────────┐   ┌──────────────────────────────────────────────┐
│ WorldPop R2025A .tif         │──►│ 1a_tai_du_lieu.sh  tải + SHA-256 │   │ source_system=<nguồn>/ingestion_date=<ngày>/ │
│ OpenStreetMap .json          │   │ 1b_chuyen_doi_bronze.py   .tif/.xlsx→CSV│──►│  worldpop/   pop_100m_raw, pop_1km_raw       │
│ GADM 4.1, AWS Terrain        │   │   (giữ nguyên giá trị, cả NoData) │   │  co_so_y_te/ tyt_raw, cskcb_raw              │
│ location-tyt.xlsx (56 TYT)   │   │ 1c_tra_cuu_khong_gian.py         │──►│  danh_muc/   dm_56_tram, NQ 1251, TT 30      │
│ danh-sach-cskcb.xlsx (30 CS) │   │   ranh giới, ô→xã, độ cao, đồ thị │   │  ranh_gioi/  GADM Đà Nẵng (GeoJSON)          │
│ danh mục 56 trạm, NQ 1251    │   └──────────────────────────────────┘   │  tra_cuu_khong_gian/ o_luoi_100m, duong_*    │
└──────────────────────────────┘                                          └───────────────────────┬──────────────────────┘
                                                                                                  │
                     PIPELINE SPARK  2_spark_5_buoc/chay_spark.py (PySpark, local[*] trên máy hoặc YARN trên cụm)
┌──────────────────────────────────────────────────────────────────────────────────────────────────▼──────────────────────┐
│ 1 · ĐỌC             b1_doc: Bronze (CSV, GeoJSON) với schema khai báo, PERMISSIVE + _corrupt_record, cache + đếm dòng  │
│ 2 · KIỂM TRA & LÀM SẠCH  b2: Null / kiểu / miền giá trị / trùng → quarantine_records (lỗi thật) / excluded_records (khoá)   │
│ 3 · BIẾN ĐỔI        b3: chuẩn hoá ô/dân/60+, UTM 49N → broadcast join ô × 56 TYT, × 30 CSKCB → Dijkstra → Silver           │
│ 4 · TỔNG HỢP        b4: KPI groupBy + phân vị p50/p95 + Spark SQL, vùng lõm, khuyến nghị, dự báo (applyInPandas), đối soát │
│ 5 · GHI             b5: Gold Parquet phân vùng + bảng CSV phục vụ, đo Parquet/CSV, kiểm tra _SUCCESS, thời gian 5 bước    │
└───────────────┬──────────────────────────────────────────────┬───────────────────────────────────────────────────────────┘
                ▼                                              ▼
   TẦNG SILVER (Parquet, phân vùng)                 TẦNG GOLD (Parquet)                         3 · BÁO CÁO (3_bao_cao)   
   dim_o_luoi (ma_quan), fact_dan_so_o_nam          fact_health_access_yearly (year)            bao_cao.py: hình h1–h9,
   (year, ma_quan), fact_o_nam_khoang_cach,         kpi_*, tram_vung_phuc_vu, vung_lom_tiep_can, từ điển dữ liệu, kiểm tra
   o_luoi_tiep_can_2024, dim_tram, dim_cskcb,       fact_optimal_facility_placement,            SHA-256, so_lieu_chot.csv
   dim_xa, quarantine_records, excluded_records     fact_access_forecast_2030, ma trận khoảng cách
                                       + bản CSV cho báo cáo trong outputs/tables/
```

Python chỉ làm phần Spark không có sẵn: đọc GeoTIFF và Excel, xét điểm nằm trong đa giác, dựng đồ thị đường, vẽ hình.
Mọi phép kiểm tra, làm sạch, join, tổng hợp và dự báo chạy trong Spark. Thuật toán tổ hợp (Dijkstra, tham lam, gom cụm) chạy
trên driver bằng scipy với dữ liệu mà Spark đã thu gọn.

## 3. Luồng dữ liệu theo 8 bước nghiệp vụ của bản thiết kế và nơi xử lý

Tám bước dưới đây là cách bản thiết kế ban đầu chia nội dung nghiệp vụ; code và thư mục kết quả đặt tên theo vòng đời
(1 thu thập → 2 Spark 5 bước → 3 báo cáo) và theo nhóm nội dung của báo cáo. Cột "Chạy ở" cho biết bước nghiệp vụ nằm ở đâu.

| Bước | Nội dung | Chạy ở | Code | Đọc | Ghi |
|---|---|---|---|---|---|
| 01 | Thu nhận: tải nguồn; đổi định dạng sang CSV giữ nguyên giá trị; bảng tra cứu không gian | Python | `1a_tai_du_lieu.sh`, `1b_chuyen_doi_bronze.py`, `1c_tra_cuu_khong_gian.py` | Internet, `data/raw/`, `data/reference/` | `data/raw/`, Bronze, `1_du_lieu_nguon/bronze_danh_muc.csv`, `1_du_lieu_nguon/qa_boundaries.csv`, `1_du_lieu_nguon/qa_network.csv` |
| 02 | Kiểm tra chất lượng, tách lỗi | Spark Bước 1 + 2 | `b1_doc.py`, `b2_kiem_tra_lam_sach.py` | Bronze | `silver/quarantine_records`, `silver/excluded_records`, `spark_*.csv`, `2_chat_luong/qa_official_population.csv` |
| 03 | Chuẩn hoá không gian: ô có dân trong 56 xã, dân 60+ (A3), toạ độ UTM 49N, trạm, CSKCB, phân loại xã (A7) | Spark Bước 3 | `b3_bien_doi.py` | bảng sạch của Bước 2 | `silver/dim_o_luoi`, `silver/fact_dan_so_o_nam`, `silver/dim_tram`, `silver/dim_cskcb`, `silver/dim_xa`, `2_chat_luong/tram_y_te_56.csv`, `2_chat_luong/cskcb_danang.csv`, `2_chat_luong/qa_stations.csv`, `2_chat_luong/qa_grid.csv` |
| 04 | Khoảng cách đường chim bay tới 56 TYT và 30 CSKCB, mọi năm 2015–2024 | Spark Bước 3 | `b3_bien_doi.py` (`gan_nhat`) | silver | `silver/fact_khoang_cach_chim_bay`, `silver/fact_o_nam_khoang_cach` |
| 05 | Khoảng cách và thời gian theo mạng đường OSM, năm 2024 (A6) | Spark Bước 3 (Dijkstra trên driver) | `b3_bien_doi.py` (`DoThi`) | Bronze đồ thị đường, silver | `silver/fact_khoang_cach_duong_di`, `silver/o_luoi_tiep_can_2024` |
| 06 | Tổng hợp KPI | Spark Bước 4 (tính), Bước 5 (ghi) | `b4_tong_hop.py` | silver | `gold/kpi_*`, `gold/fact_health_access_yearly`, `kpi_*_spark.csv`, `3_mo_ta_kpi/tram_vung_phuc_vu_spark.csv`, `logs/plan_kpi_*.txt` |
| 07 | Khuyến nghị: vùng lõm (A8, A18), điểm đặt mới (A10), kịch bản NQ 1251, TYT – CSKCB (A15) | Spark Bước 4 + driver, Bước 5 (ghi) | `b4_tong_hop.py` (vùng lõm), `b4_khuyen_nghi.py` | silver | `4_chan_doan/white_zones.csv`, `greedy_*.csv`, `5_khuyen_nghi/scenario_nq1251.csv`, `tyt_cskcb*.csv`, `5_khuyen_nghi/ung_vien_gop_tram.csv`, `doi_soat_nq1251_*.csv`, gold tương ứng |
| 08 | Dự báo 2030 theo 56 xã cũ (A9), file riêng | Spark Bước 4 (`applyInPandas`), Bước 5 (ghi) | `b4_du_bao.py` | silver, Bronze 1 km 2025–2030 | `forecast_*.csv`, `access_2030_*.csv`, `6_du_bao/commune_series.csv`, `gold/fact_access_forecast_2030` |
| sau 08 | Báo cáo: hình, bản đồ, từ điển, kiểm tra sổ, số liệu chốt | Python | `bao_cao.py` | `outputs/tables/` | `outputs/figures/h1–h9`, `7_kiem_chung/pham_vi_da_nang_cu.csv`, `1_du_lieu_nguon/xa_phuong_56_toa_do.csv`, `5_khuyen_nghi/ban_do_truoc_sau_de_xuat.csv`, `7_kiem_chung/tu_dien_du_lieu.csv`, `7_kiem_chung/kiem_tra_run_manifest.csv`, `7_kiem_chung/so_lieu_chot.csv` |

Tên thư mục và tên file (người dùng chốt 27/11/2026): code nằm ở `scripts/1_thu_thap/` (thu thập, giai đoạn 2 của vòng đời),
`scripts/2_spark_5_buoc/` (mỗi file `bN_…` là một bước của khung 5 bước kỹ thuật – mục 5), `scripts/3_bao_cao/` (trực quan hoá, giai đoạn 6);
bảng kết quả nằm ở `outputs/tables/<thư mục nội dung>/`: `1_du_lieu_nguon`, `2_chat_luong`, `3_mo_ta_kpi`, `4_chan_doan`, `5_khuyen_nghi`,
`6_du_bao`, `7_kiem_chung`, `luoi_100m` (danh sách đầy đủ: `THU_MUC_BANG` trong `scripts/chung/common.py`).

Phần thu thập chạy bảng tra cứu không gian (một phần của Bước 03 và 05) **trước** pipeline Spark vì Spark cần bảng "ô nào thuộc xã nào"
để kiểm tra. Bảng này vẫn được Spark Bước 2 kiểm tra như mọi dữ liệu thô khác (khoá trùng, toạ độ ngoài hộp bao, mã xã lạ).

## 4. Kiến trúc lưu trữ Medallion

| Tầng | Trên máy | Trên HDFS | Định dạng | Phân vùng | Nội dung | Ai ghi |
|---|---|---|---|---|---|---|
| Nguồn | `data/raw/`, `data/reference/` | – | TIFF, JSON, XLSX, CSV | – | File gốc tải về hoặc nhóm cung cấp; không sửa, không xoá; `data/manifest_raw.csv` ghi URL, SHA-256, giờ tải, giấy phép | `1a`, nhóm |
| Bronze | `data/lake/bronze/` | `/mis5007/bronze` | CSV, GeoJSON | `source_system=<nguồn>/ingestion_date=<ngày>` | Bản ghi thô giữ nguyên giá trị (cả NoData, dòng tiêu đề Excel); chỉ thêm, không sửa; Spark đọc lần nạp mới nhất | `1b`, `1c` |
| Silver | `data/lake/silver/` | `/mis5007/silver` | Parquet (Snappy) | `fact_dan_so_o_nam`, `fact_o_nam_khoang_cach`: `year`, `ma_quan`; `dim_o_luoi`, `o_luoi_tiep_can_2024`: `ma_quan`; `quarantine_records`, `excluded_records`: `nguon` | Dữ liệu đã kiểm tra, làm sạch, chuẩn hoá; vùng lỗi (nguyên văn) và vùng ngoài phạm vi (khoá tham chiếu) | Spark Bước 2, 3 |
| Gold | `data/lake/gold/` | `/mis5007/gold` | Parquet (Snappy) | `fact_health_access_yearly`: `year`; `fact_khoang_cach_o_2024`: `ma_quan`; bảng nhỏ: 1 file | KPI, ma trận khoảng cách, vùng lõm, điểm đặt mới, dự báo 2030 (file riêng) | Spark Bước 5 (tính ở Bước 4) |
| Kết quả báo cáo | `outputs/tables/<thư mục nội dung>/`, `outputs/figures/` | – | CSV, PNG | thư mục `1_du_lieu_nguon`, `2_chat_luong`, `3_mo_ta_kpi`, `4_chan_doan`, `5_khuyen_nghi`, `6_du_bao`, `7_kiem_chung`, `luoi_100m` | Bản CSV của Gold và các bảng QA, hình | Spark Bước 5, `09` |

`ma_quan` là mã quận không dấu (`hai_chau`, `thanh_khe`, `son_tra`, `ngu_hanh_son`, `lien_chieu`, `cam_le`, `hoa_vang`) –
tên thư mục phân vùng không nên có dấu tiếng Việt. Không đặt `HDFS_URI` thì ba tầng nằm trên máy; đặt `HDFS_URI` thì đọc và ghi trên HDFS.

## 5. Pipeline Spark – khung 5 bước kỹ thuật

```
[1. ĐỌC] ➔ [2. KIỂM TRA & LÀM SẠCH] ➔ [3. BIẾN ĐỔI] ➔ [4. TỔNG HỢP] ➔ [5. GHI]
```

| Bước kỹ thuật | File | Nội dung nghiệp vụ (8 bước) | Ghi gì |
|---|---|---|---|
| 1 · Đọc | `b1_doc.py` | đầu vào của Bước 02 | không ghi (danh mục `1_du_lieu_nguon/spark_bang_da_doc.csv` vào hàng chờ) |
| 2 · Kiểm tra & làm sạch | `b2_kiem_tra_lam_sach.py` | Bước 02 | `silver/quarantine_records`, `silver/excluded_records` (đúng khung: tách bản ghi lỗi vào bảng cách ly) |
| 3 · Biến đổi | `b3_bien_doi.py` | Bước 03, 04, 05 | các bảng Silver (ghi rồi đọc lại làm điểm lưu) |
| 4 · Tổng hợp | `b4_tong_hop.py`, `b4_khuyen_nghi.py`, `b4_du_bao.py` | Bước 06, 07, 08 | không ghi: mọi bảng vào hàng chờ |
| 5 · Ghi | `b5_ghi.py` | ghi kết quả | Gold, mọi bảng CSV, `so_sanh_parquet_csv.csv`, `qa_summary.csv`, `doi_soat_spark.csv`, `thoi_gian_5_buoc.csv` |

`chay_spark.py` chạy 5 bước nối tiếp; mỗi bước nằm trong khối `with nc.buoc(...)` để in tiêu đề `===== BUOC n` và đo thời gian. Bước 1–4
gửi bảng CSV vào hàng chờ bằng `nc.xuat(...)` và bảng Gold bằng `nc.gold(...)`; Bước 5 ghi một lần. Nhờ vậy đối soát với bộ số đã chốt
(cuối Bước 4) chạy **trước** khi ghi. Tầng Silver không dồn về Bước 5: bảng cách ly ghi ở Bước 2 như khung quy định, bảng sạch ghi cuối Bước 3
để Bước 4 đọc từ Parquet đã nén thay vì tính lại cả chuỗi từ 3 triệu dòng CSV, và để còn dữ liệu sạch nếu chạy hỏng giữa chừng.

### Bước 1 · Đọc

Mỗi bảng Bronze đọc bằng schema khai báo (`StructType`), không suy kiểu, chế độ `PERMISSIVE`: dòng sai kiểu hoặc sai số cột rơi vào
`_corrupt_record`. Bảng được `cache()` rồi `count()` ngay (Spark đọc thật một lần), Bước 2 dùng lại bản trong bộ nhớ. Ba bảng tham chiếu
nhỏ đọc mọi cột là chuỗi; ranh giới GeoJSON đọc nguyên văn rồi phân tích trên driver. Lần chạy 27/11/2026: 11 bảng CSV, 4.252.689 dòng
và 56 đa giác (`1_du_lieu_nguon/spark_bang_da_doc.csv`).

### Bước 2 · Kiểm tra & làm sạch (Bước 02 của luồng dữ liệu)

Hàm `tach()` áp danh sách quy tắc `(mã lý do, điều kiện vi phạm)`: Null, kiểu, miền giá trị, trùng, toàn vẹn tham chiếu. Mỗi nguồn được
đếm **một lần** theo (loại, lý do); bảng `2_chat_luong/spark_quarantine_tong_hop.csv` lấy thẳng từ các lần đếm này. Dòng vi phạm mang
`nguon`, `loai`, `ly_do` (nhiều mã nối bằng `;`), `khoa`, `ban_ghi`:

- `loai = NGOAI_PHAM_VI` → `silver/excluded_records`: dòng **chỉ** mang mã trong `MA_NGOAI_PHAM_VI` – không phải lỗi (#Truthmode).
  Từ 27/11/2026 chỉ lưu **khoá tham chiếu** (`ban_ghi` để trống): bản gốc nằm nguyên ở Bronze (chỉ thêm, không sửa) và tra lại được theo
  `nguon` + `khoa`. Trước đó vùng này chép lại hơn 3 triệu dòng dạng JSON (56 MB, 64% tầng Silver).
- `loai = LOI_DU_LIEU` → `silver/quarantine_records`: dòng có ít nhất một mã lỗi; giữ **nguyên văn** JSON để người phụ trách sửa ở nguồn.
- Bảng mẫu `2_chat_luong/spark_quarantine_mau.csv` lấy 10 dòng có khoá nhỏ nhất mỗi nhóm bằng `orderBy(...).limit(10)` (Spark chỉ giữ top-10),
  thay cho hàm cửa sổ trên toàn bộ 3 triệu dòng; kết quả giống từng byte với cách cũ.

| Mã lý do | Ý nghĩa | Loại | Áp cho |
|---|---|---|---|
| `GIA_TRI_AM_NODATA` | Mã NoData −99999 của WorldPop (ô không có dân) | ngoài phạm vi | dân số 100 m, 1 km |
| `NGOAI_56_XA` | Pixel không thuộc 56 xã | ngoài phạm vi | pixel, dân số |
| `NAM_NGOAI_PHAM_VI` | Năm ngoài 2015–2024 (100 m) hoặc 2015–2030 (1 km) | ngoài phạm vi | dân số |
| `SAI_KIEU_HOAC_SO_COT`, `THIEU_KHOA`, `TRUNG_KHOA`, `THIEU_GIA_TRI` | Đọc không được, thiếu/trùng khoá, ô rỗng | lỗi | mọi nguồn |
| `QUAN_KHONG_HOP_LE`, `LOAI_DV_KHONG_HOP_LE` | Quận không thuộc 7 quận/huyện; loại đơn vị không phải Phường/Xã | lỗi | danh mục 56 xã |
| `SAI_TOA_DO_NGOAI_HOP_BAO`, `NGOAI_HOP_BAO`, `THIEU_TOA_DO`, `SAI_DINH_DANG_TOA_DO` | Toạ độ ngoài hộp bao Đà Nẵng cũ, thiếu, sai mẫu "vĩ độ, kinh độ" | lỗi | pixel, TYT, CSKCB |
| `MA_XA_KHONG_CO_TRONG_DANH_MUC`, `KHONG_CO_O_LUOI`, `O_LUOI_BI_CACH_LY`, `THIEU_NUT_DUONG` | Tham chiếu tới mã/ô không hợp lệ | lỗi | pixel, dân số |
| `KHONG_PHAI_BAN_GHI_TRAM`, `TEN_KHONG_KHOP_DANH_MUC`, `TRUNG_TRAM` | Dòng tiêu đề/tên quận, tên xã lạ, trùng trạm | lỗi | TYT |
| `BIEN_KHONG_HOP_LE` | Tên biến không phải `pop` hay `t_60` … `t_90` | lỗi | lưới 1 km |
| `CANH_TRO_TOI_NUT_KHONG_CO`, `GIA_TRI_KHONG_DUONG` | Cạnh đồ thị trỏ tới nút lạ, độ dài/thời gian không dương | lỗi | đồ thị đường |

Lần chạy 26/11/2026 (`2_chat_luong/spark_quarantine_tong_hop.csv`): quarantine có **9 dòng**, đều ở file toạ độ nhập tay (8 dòng tiêu đề/tên quận,
1 dòng tiêu đề "Column1…" không đúng mẫu toạ độ – không có bản ghi trạm nào sai; 56/56 trạm sạch). excluded có 3.030.871 dòng:
NoData của WorldPop và pixel ngoài 56 xã. Quy tắc dòng nhóm quận trong Excel khác danh mục chỉ ghi `CAN XEM` vào log.

**Quy tắc tổng dân ±3% của thiết kế** được áp ở đây nhưng chỉ **cảnh báo**: WorldPop là ước tính mô hình, lệch so với số liệu
chính thức không phải lỗi dữ liệu. Chỉ 2/17 bản ghi mỗi năm 2023, 2024 nằm trong ±3% (`2_chat_luong/qa_official_population.csv`, cột `trong_3pct`);
ngưỡng chấp nhận cũ ±20% (A11) vẫn báo cáo song song.

### Bước 3 · Biến đổi (Bước 03 + 04 + 05 của luồng dữ liệu)

| Mã | Phép biến đổi | Loại | Ý nghĩa nghiệp vụ |
|---|---|---|---|
| T1 | Gộp 7 nhóm tuổi của ô 1 km, nối vào ô 100 m | rộng (groupBy) + join | Dân 60+ từng ô (A3) |
| T2 | Nối chéo ô có dân × 56 TYT và × 30 CSKCB bằng `F.broadcast`, giữ `min(struct(d, mã))` | broadcast join + groupBy | Khoảng cách chim bay tới cơ sở gần nhất |
| T3 | Dijkstra nhiều nguồn trên driver (scipy), đưa chi phí từng nút lên Spark, join ô theo `nut_duong` | join | Khoảng cách, thời gian theo đường (A6); hệ số vòng |
| T4 | Trung bình độ dốc, độ cao theo dân của từng xã | groupBy | Phường / xã đồng bằng / xã miền núi (A7) |

**Broadcast trong kế hoạch thực thi** (`logs/plan_khoang_cach_2024.txt`): bảng 56 trạm được gửi tới mọi executor (`BroadcastExchange`) và nối
bằng `BroadcastNestedLoopJoin Cross BuildRight`; bảng ô 41 nghìn dòng không bị shuffle ở bước nối. Bản thiết kế ghi "Broadcast Hash Join",
nhưng phép "tìm trạm gần nhất" không có khoá bằng nhau để băm (mỗi ô phải so với cả 56 trạm), nên Spark dùng vòng lặp lồng trên bảng đã broadcast –
cùng lợi ích (không shuffle bảng lớn), khác tên thuật toán. `BroadcastHashJoin` thật xuất hiện ở các phép nối có khoá, ví dụ nối bảng sự kiện với
`dim_xa` theo `ma_tram` (`logs/plan_pruning_2024.txt`). Bước `groupBy("cell_id")` sau đó mới có shuffle (`Exchange`).

Toạ độ đổi sang UTM 49N bằng công thức Krüger viết thuần numpy (`hinh_hoc.utm49n`) để chạy được cả trong container không có pyproj;
đối soát với pyproj lệch 0,0000 m (`7_kiem_chung/doi_soat_spark.csv`). Kiểm tra trạm nằm trong ranh giới xã dùng thuật toán bắn tia (`hinh_hoc.trong_da_giac`).

### Bước 4 · Tổng hợp (Bước 06 + 07 + 08 của luồng dữ liệu)

- KPI tính bằng `groupBy` (phép biến đổi rộng, có shuffle; `spark.sql.shuffle.partitions = 16`). Phân vị có trọng số dân (p50, p95)
  tính bằng hàm cửa sổ cộng dồn: giá trị nhỏ nhất mà dân luỹ kế ≥ q × tổng dân – cùng định nghĩa với `common.wquantile`. Từ 27/11/2026
  p50 và p95 của cùng một biến tính trong **một** lần quét cửa sổ (`phan_vi(df, by, v, kc_trung_vi_m=0.5, kc_p95_m=0.95)`) thay vì hai lần.
- KPI theo nhóm viết thêm một lần bằng Spark SQL trên temp view và đối soát với DataFrame API (lệch 0).
- Vùng lõm: Spark lọc ô cách trạm > 3 km hoặc cần > 20 phút (vài nghìn ô), driver gom cụm bằng `scipy.ndimage`.
- Điểm đặt mới: Spark chọn ứng viên (ô đông dân nhất mỗi khối 3 × 3 ô bằng `row_number` trên cửa sổ), driver chạy tham lam bao phủ tối đa.
- TYT – CSKCB: ma trận 56 × 30 bằng broadcast join, ghi vào `gold/fact_khoang_cach_tyt_cskcb`.

- Dự báo (`b4_du_bao.py`): chuỗi học 2015–2024 theo xã: hạ cấp từ lưới 1 km, giữ phân bố ô 100 m năm 2024 (A4).
- `groupBy("ma_tram").applyInPandas(...)` khớp 4 mô hình cho từng xã song song trên các executor; backtest 2 gốc; mô hình có MAPE
  trung bình nhỏ nhất được chọn (lần này ETS cho cả dân số và dân 60+, `6_du_bao/forecast_model_choice.csv`).
- Kết quả dự báo vào hàng chờ, Bước 5 ghi riêng `gold/fact_access_forecast_2030` và các file `forecast_*.csv`, `access_2030_*.csv`.
- Cuối Bước 4: đối soát với bộ số đã chốt 19/11/2026 trên chính các bảng đang chờ ghi (`nc.doc_cho` đọc bảng như đọc file CSV).

### Bước 5 · Ghi

1. Ghi mọi bảng Gold trong hàng chờ (Parquet Snappy; bảng lớn chia thư mục `year` / `ma_quan`, bảng nhỏ 1 file).
2. Ghi mọi bảng CSV (`save_csv`: utf-8-sig cho Excel, đăng ký `run_manifest.csv` với số dòng và SHA-256).
3. Xuất hai bảng lưới phục vụ Excel / GIS: `dan_cu_luoi_100m.csv`, `tiep_can_luoi_100m.csv`.
4. Đo cùng một truy vấn trên Parquet phân vùng và CSV (`7_kiem_chung/so_sanh_parquet_csv.csv`).
5. Kiểm tra mọi bảng Parquet có `_SUCCESS`; ghi `qa_summary.csv`, `doi_soat_spark.csv`; sau cùng ghi `7_kiem_chung/thoi_gian_5_buoc.csv`.

### Ghi và đọc an toàn

Mọi bảng Silver được ghi rồi **đọc lại** trước khi dùng tiếp (`NguCanh.luu`). Parquet không có giao dịch: mỗi lần đọc kiểm tra file
`_SUCCESS` trước và dừng nếu thiếu (`NguCanh.doc`). Mặc định `partitionOverwriteMode = static` (ghi đè cả bảng).

### Đối soát – 48 phép (`7_kiem_chung/doi_soat_spark.csv`)

- Bảo toàn: số dòng quarantine/excluded đọc lại = tổng theo nguồn; tổng dân thô = sạch + bị loại; dòng bảng sự kiện đọc lại = trước khi ghi;
  mọi bảng Parquet đã ghi có `_SUCCESS`.
- Hai cách tính độc lập: khoảng cách broadcast join vs KDTree; Spark SQL vs DataFrame API; UTM tự viết vs pyproj; đường đi ≥ chim bay.
- So với bộ số đã chốt 19/11/2026 (34 phép): mọi KPI tổng hợp, KPI theo năm, theo xã, vùng phục vụ, điểm đề xuất, tiếp cận 2030,
  ứng viên A15, vùng trắng. Lần chạy 27/11/2026 (pipeline 5 bước): 48/48 `ĐẠT`.

## 6. Đối chiếu với bản thiết kế "Luồng dữ liệu – Kiến trúc lưu trữ – Spark pipeline"

### 6.1. Từng yêu cầu của thiết kế nằm ở đâu trong code

Trạng thái: **Đúng** = làm như thiết kế; **Điều chỉnh** = làm khác vì dữ liệu hoặc vì người dùng chốt (lý do ở 6.2); **Bổ sung** = thiết kế không ghi nhưng cần để kết quả đáng tin.

| Yêu cầu trong thiết kế | Nằm ở | Trạng thái |
|---|---|---|
| Bước 01 – nạp dữ liệu đa nguồn, không đổi định dạng gốc | `1a_tai_du_lieu.sh` (file gốc vào `data/raw/`, ghi SHA-256), `1b_chuyen_doi_bronze.py` (giá trị giữ nguyên, kể cả NoData) | Đúng; Spark không đọc được TIFF/Excel nên Bronze là CSV giữ nguyên giá trị |
| Bước 02 – quy tắc toạ độ trong hộp bao | `b2_kiem_tra_lam_sach.py`: `NGOAI_HOP_BAO`, `SAI_TOA_DO_NGOAI_HOP_BAO` | Đúng (hộp bao điều chỉnh) |
| Bước 02 – quy tắc tổng dân ±3% | `b2_kiem_tra_lam_sach.py` mục 2.11, `2_chat_luong/qa_official_population.csv` | Điều chỉnh: cảnh báo, không cách ly |
| Bước 02 – toàn vẹn khoá xã (không rỗng, không trùng) | `b2_kiem_tra_lam_sach.py`: `THIEU_KHOA`, `TRUNG_KHOA`, `MA_XA_KHONG_CO_TRONG_DANH_MUC` | Đúng |
| Bước 02 – bảng cách ly | `silver/quarantine_records` (+ `silver/excluded_records` cho dòng ngoài phạm vi) | Đúng + Bổ sung (tách lỗi thật với NoData) |
| Bước 03 – phủ ranh giới lên lưới 100 m, bỏ ô ngoài thành phố / dân = 0 | `1c_tra_cuu_khong_gian.py` (điểm trong đa giác), `b3_bien_doi.py` (`co_dan`, `dim_o_luoi`) | Đúng |
| Bước 03 – đổi WGS84 sang mét UTM | `hinh_hoc.utm49n`, cột `x_m`, `y_m` | Điều chỉnh: UTM 49N |
| Bước 04 – khoảng cách chim bay tới 56 TYT và 30 CSKCB, 10 mốc năm | `b3_bien_doi.py` (`gan_nhat`), `silver/fact_o_nam_khoang_cach` | Đúng; khoảng cách tới CSKCB chỉ tính một lần vì vị trí cơ sở không đổi theo năm |
| Bước 05 – đường đi ngắn nhất OSM, năm 2024 | `b3_bien_doi.py` (`DoThi.dijkstra`), `silver/fact_khoang_cach_duong_di` | Điều chỉnh: Dijkstra chạy trên driver |
| Bước 06 – % trong 3 km (chim bay và đường đi), dân bình quân mỗi trạm, p50, p95, > 5 km, 60+, chênh lệch phường – xã miền núi | `b4_tong_hop.py` (`kpi_co_ban`, `kpi_duong`, `phan_vi`), `3_mo_ta_kpi/kpi_tong_hop_spark.csv` | Đúng; p50/p95 tính có trọng số dân (6.2) |
| Bước 07 – vùng lõm (> 5 km hoặc > 20 phút), điểm đặt mới | `b4_tong_hop.py` mục 4.2, `b4_khuyen_nghi.py` mục 4.3b; `4_chan_doan/white_zones.csv`, `5_khuyen_nghi/greedy_sites.csv` | Đúng (20 phút = A18) |
| Bước 08 – dự báo 2030, file riêng | `b4_du_bao.py`, `forecast_*.csv`, `gold/fact_access_forecast_2030` | Điều chỉnh: theo 56 xã cũ |
| Bronze – phân vùng `source_system`, `ingestion_date`, không sửa | `data/lake/bronze/source_system=…/ingestion_date=…/` | Đúng |
| Silver – Parquet phân vùng `year`, `district_id` | `silver/fact_dan_so_o_nam`, `silver/fact_o_nam_khoang_cach` chia `year=…/ma_quan=…` | Đúng (`ma_quan` = district_id không dấu) |
| Gold – Parquet, phân vùng `year`, mã xã cũ / mới | `gold/fact_health_access_yearly` chia `year=…`; mã xã là cột | Điều chỉnh: không chia theo mã xã |
| Gold – PostgreSQL/PostGIS qua JDBC | – | Điều chỉnh: không làm (người dùng chốt) |
| Giai đoạn I – đọc bằng `StructType`, `filter` / `when` / `otherwise`, `df_clean` / `df_quarantine` | `b1_doc.py` (`khai_bao_schema`), `b2_kiem_tra_lam_sach.py` (`tach`) | Đúng (nay là Bước 1 + 2 của khung 5 bước) |
| Giai đoạn II – broadcast bảng TYT/CSKCB, nối với lưới dân số | `b3_bien_doi.py` (`F.broadcast`), `logs/plan_khoang_cach_2024.txt` | Đúng; Spark chọn `BroadcastNestedLoopJoin` (mục 5) |
| Giai đoạn III – `groupBy("year", "commune_id")`, hàm cửa sổ, `percentile_approx` | `b4_tong_hop.py` (`groupBy`, `Window`, `phan_vi`) | Điều chỉnh: phân vị chính xác có trọng số thay cho `percentile_approx` |
| Giai đoạn IV – dự báo, ghi Gold | `b4_du_bao.py` (`applyInPandas`), `b5_ghi.py` (ghi Gold) | Đúng |
| Biến đổi hẹp (ép kiểu, kiểm tra null, đổi toạ độ, lọc) | `withColumn`, `filter`, UDF `utm49n_cap` | Đúng |
| Biến đổi rộng, `spark.sql.shuffle.partitions = 16` | `spark_chung.py` | Đúng |
| Quyết định 1 – chim bay mọi năm, đường đi năm 2024 | `b3_bien_doi.py` | Đúng |
| Quyết định 2 – Parquet nén Snappy, predicate pushdown | `NguCanh.ghi` (Snappy); đo ở `7_kiem_chung/so_sanh_parquet_csv.csv` | Đúng; số đo thật thay cho "70%" |
| Đích ghi `s3a://…/silver/quarantine_records/`, `gold/fact_health_access_yearly/`, `gold/fact_access_forecast_2030/`, `gold/fact_optimal_facility_placement/` | Cùng tên bảng trong `data/lake/` hoặc `hdfs://namenode:9000/mis5007/` | Điều chỉnh vị trí, giữ tên bảng |
| – | Đối soát 48 phép, `7_kiem_chung/doi_soat_spark.csv`; ghi/đọc an toàn `_SUCCESS`; Spark SQL song song DataFrame API | Bổ sung |

### 6.2. Những điểm làm khác thiết kế và lý do

Bản thiết kế có vài điểm không khớp dữ liệu của dự án. Cách xử lý:

| Thiết kế ghi | Dữ liệu cho thấy | Đã làm |
|---|---|---|
| UTM 48N (EPSG:32648) | 56 xã trải 107,82–108,34°Đ (`7_kiem_chung/pham_vi_da_nang_cu.csv`); từ 108°Đ là múi 49, chỉ phía tây Hòa Bắc thuộc múi 48; khoảng cách theo hai múi chênh dưới 1 m trên 3 km | Giữ UTM 49N (EPSG:32649) |
| Hộp bao 15,89–16,21°B; 107,83–108,40°Đ | Ranh giới tới 16,2257°B (Hòa Hiệp Bắc), 107,8167°Đ (Hòa Bắc): hộp bao thiết kế cắt mất một phần hai xã | Giữ hộp bao 15,88–16,24°B; 107,78–108,36°Đ |
| Lệch tổng dân > ±3% → quarantine | 2/17 bản ghi mỗi năm trong ±3%; lệch do mô hình ước tính, không phải lỗi | Kiểm tra ±3% nhưng chỉ cảnh báo (người dùng chốt 26/11/2026) |
| 47 phường, 9 xã miền núi | 45 phường, 11 xã; theo địa hình (A7) có 2 xã miền núi, 9 xã đồng bằng/ven đô | Dùng số của dữ liệu |
| Spark tính đường đi ngắn nhất | Spark không có thuật toán Dijkstra | Dijkstra trên driver (scipy), kết quả join vào Spark |
| Gold phân vùng theo year × commune_id_old × commune_id_new | Mỗi bảng KPI chỉ vài nghìn dòng; chia 560 thư mục sinh nhiều file nhỏ | Gold chia thư mục theo `year`; mã xã là cột |
| Ghi s3a:// và PostgreSQL/PostGIS | Dự án chưa có hai hệ thống này | Hồ dữ liệu trên máy (`data/lake/`) hoặc HDFS (người dùng chốt) |
| Dự báo theo bản đồ sau sáp nhập (47 đơn vị) | – | Chỉ dự báo theo 56 xã cũ (người dùng chốt) |
| Vùng lõm > 20 phút | Dự án dùng 15 phút | Tính cả hai; 20 phút là giả định A18 |
| `percentile_approx(distance, 0.5)` cho p50, p95 | `percentile_approx` tính phân vị theo **ô lưới**, không theo **người**: ô 2 người và ô 2.000 người được tính như nhau | Phân vị chính xác có trọng số dân bằng hàm cửa sổ cộng dồn (`phan_vi`) – "50% dân sống trong X m" đúng nghĩa |
| Broadcast Hash Join cho lưới × TYT | "Trạm gần nhất" không có khoá bằng nhau để băm | Spark tự chọn `BroadcastNestedLoopJoin`, vẫn broadcast bảng nhỏ, không shuffle bảng lớn |
| "Tăng tốc 10–15 lần", "tiết kiệm hơn 70% I/O" | Không có số đo | Thay bằng số đo thật ở mục 7 |

### 6.3. Đối chiếu với khung 5 bước kỹ thuật (27/11/2026)

| Yêu cầu của khung | Nằm ở | Trạng thái |
|---|---|---|
| Bước 1 – đọc dữ liệu thô từ CSV, JSON với schema tường minh | `b1_doc.py`: `StructType` cho 8 bảng, `PERMISSIVE` + `_corrupt_record`; GeoJSON đọc nguyên văn | Đúng |
| Bước 1 – đọc từ Database, Event | – | Không có nguồn này; cách gắn thêm (JDBC chụp vào Bronze, Structured Streaming) ở `BAO_CAO_7_GIAI_DOAN.md` Giai đoạn 2.3 |
| Bước 2 – kiểm tra Null, kiểu, miền giá trị; tách bản ghi lỗi vào bảng cách ly | `b2_kiem_tra_lam_sach.py` (`tach`), `silver/quarantine_records` | Đúng + Bổ sung (`excluded_records` cho dòng ngoài phạm vi, không phải lỗi) |
| Bước 3 – chuẩn hoá, nối đa nguồn, cột nghiệp vụ | `b3_bien_doi.py` (UTM 49N, broadcast join, Dijkstra, `detour`, `nhom`, `pop60`) | Đúng |
| Bước 4 – KPI theo khoá (groupBy, cửa sổ), kiểm tra đối soát | `b4_tong_hop.py` (`kpi_co_ban`, `phan_vi`, Spark SQL), đối soát 19/11 trước khi ghi | Đúng |
| Bước 5 – ghi Parquet phân vùng hoặc bảng phục vụ (Dashboard, Table) | `b5_ghi.py`: Gold Parquet, bảng CSV phục vụ | Đúng; không làm dashboard (lý do ở báo cáo, Giai đoạn 6.3) |
| Thứ tự nối tiếp 1 → 5 | `chay_spark.py` | Điều chỉnh: Silver ghi ở Bước 2 (cách ly) và cuối Bước 3 (điểm lưu) – lý do ở mục 5 |

## 7. Quyết định thiết kế và đánh đổi

| Quyết định | Lý do | Cái giá |
|---|---|---|
| Khoảng cách chim bay cho cả 10 năm; đường đi chỉ cho năm 2024 | Vị trí 56 trạm giữ nguyên qua các năm (A14) nên đường đi không đổi theo năm; tránh lặp Dijkstra | Không có chỉ số đường đi theo từng năm |
| Parquet phân vùng thay CSV ở Silver/Gold | Đo trên cùng truy vấn năm 2024 (`7_kiem_chung/so_sanh_parquet_csv.csv`): Parquet quét 1.196.358 byte; CSV phẳng quét 46.402.204 byte (gấp 38,79 lần); CSV chia thư mục theo năm 4.476.899 byte (3,74 lần). Cắt tỉa phân vùng (`PartitionFilters`) có ở cả Parquet và CSV chia thư mục | Ghi Parquet chậm hơn CSV một chút; cần công cụ đọc Parquet |
| Silver chia thư mục theo `year` và `ma_quan` | Đúng thiết kế (district_id); truy vấn một quận chỉ đọc thư mục của quận đó | Mỗi năm thành 7 file nhỏ (P1 quét 7 file thay vì 1) |
| Python làm phần hình học, Spark làm phần bảng | Spark không có phép điểm-trong-đa-giác, không đọc GeoTIFF | Hai ngôn ngữ xử lý trong một luồng |
| Một đường tính duy nhất trong Spark (bỏ bản pandas trùng) | Tinh gọn: bản cũ phải sửa công thức ở hai nơi | Mất phép đối soát Spark ↔ pandas; thay bằng đối soát với bộ số 19/11 và các phép tính độc lập (KDTree, Spark SQL, pyproj) |
| Broadcast bảng 56 TYT / 30 CSKCB | Bảng nhỏ vài KB: gửi nguyên bản cho mọi executor, bảng ô không bị shuffle | Chỉ dùng được khi một phía rất nhỏ |
| Làm tròn chuỗi học tới 10⁻⁶ người trước khi khớp ETS | Bộ tối ưu ETS rất nhạy: chuỗi lệch 10⁻¹⁰ (thứ tự cộng khác) làm dự báo vài xã đổi tới 238 người | Dự báo theo xã lệch tối đa 0,338% so với bộ số 19/11, nằm trong khoảng 80% (±0,748%) |
| Chạy Spark local trên máy, cụm Docker là tuỳ chọn | Chạy và debug được bằng F5 trong VS Code; toàn bộ pipeline mất khoảng 1 phút | Cần cài Java 17 và pyspark trong `.venv` |
| Ranh giới xã lấy từ GADM 4.1 | OSM mốc 31/12/2024 đã vẽ theo bản đồ sau sáp nhập, chỉ khớp 45/56 xã (A12) | GADM thô hơn và không được phân phối lại (nên `data/lake/` không đưa lên GitHub) |
| Mọi tham số nằm trong `common.py` | Đổi năm hay ngưỡng ở một chỗ | Đổi năm cơ sở làm đổi tên cột (`pop_2024`) |
| Bronze lưu CSV giữ nguyên giá trị (cả NoData, dòng tiêu đề Excel) | Spark không đọc được TIFF/Excel; giữ nguyên giá trị để Spark Bước 2 tự phát hiện và ghi lý do loại – kiểm tra được, không "làm sạch ngầm" | Bronze lớn (pixel × năm: 3 triệu dòng) |
| Tách `excluded_records` khỏi `quarantine_records` | NoData của WorldPop và pixel ngoài 56 xã không phải lỗi; trộn vào quarantine sẽ thành "3 triệu dòng lỗi" – sai sự thật (#Truthmode) | Thêm một vùng lưu trữ |
| Dự báo bằng `groupBy("ma_tram").applyInPandas(...)` | Mô hình ETS (statsmodels) không có trong Spark; `applyInPandas` cho mỗi xã một tác vụ song song trên executor | Executor phải có pandas, statsmodels (trên YARN phải gửi kèm gói Python) |
| Pipeline Spark theo 5 bước kỹ thuật, Bước 1–4 gửi kết quả vào hàng chờ, Bước 5 ghi một lần | Khớp khung 5 bước; đối soát với bộ số đã chốt chạy trước khi ghi; mỗi bước đo được thời gian riêng (`7_kiem_chung/thoi_gian_5_buoc.csv`) | Bảng kết quả giữ trong RAM driver tới Bước 5 (vài MB, không đáng kể) |
| `excluded_records` chỉ lưu khoá tham chiếu | Bản gốc đã nằm ở Bronze; chép lại 3 triệu dòng JSON vừa tốn chỗ vừa chậm. Đo 27/11/2026: 56 MB → 16 MB | Muốn xem nguyên văn một dòng NoData phải tra lại Bronze theo khoá |
| p50 và p95 tính trong một lần quét cửa sổ | Mỗi lần quét cửa sổ là một lần shuffle + sắp xếp; gộp các mức q của cùng một biến giảm số lần quét từ 25 xuống 15 | Không |
| Giải phóng bộ nhớ đệm của bảng thô cuối Bước 2 | Các bước sau chỉ dùng bảng sạch; bớt RAM, quan trọng khi executor YARN chỉ có 1 GB | Phải `count()` các bảng sạch trước khi giải phóng |
| Driver giải bài toán tổ hợp (Dijkstra, tham lam, gom cụm) | Spark thu gọn dữ liệu trước (vài nghìn ô chưa phủ, 5.894 ứng viên); bài toán lặp kiểu tham lam không song song hoá tốt | Phần này chạy trên một máy |

## 8. Hạ tầng Docker (tuỳ chọn – trình diễn HDFS, YARN, JupyterLab)

File cấu hình `docker/docker-compose.yml`, tên dự án compose `mis5007`.

| Container | Image | Cổng trên máy | Dùng để |
|---|---|---|---|
| `mis5007-namenode` | bde2020/hadoop-namenode 3.2.1 | 9871 | HDFS NameNode và giao diện duyệt file |
| `mis5007-datanode` | bde2020/hadoop-datanode 3.2.1 | 9864 | Lưu block dữ liệu |
| `mis5007-resourcemanager` | bde2020/hadoop-resourcemanager 3.2.1 | 8088 | YARN |
| `mis5007-nodemanager` | bde2020/hadoop-nodemanager 3.2.1 | 8042 | YARN |
| `mis5007-spark-notebook` | jupyter/pyspark-notebook | 8888, 4041 | JupyterLab, PySpark, Spark UI |

Thư mục dự án được gắn vào `/home/jovyan/work/mis5007_tram_y_te` của container JupyterLab. Image NodeManager không có Python nên khi
chạy `SPARK_MASTER=yarn` phải gửi kèm một môi trường Python đóng gói (`yarn_python_env.sh`, gồm numpy, pandas, pyarrow, statsmodels cho
UDF và `applyInPandas`).

Trên máy Apple Silicon (kiểm tra 26/11/2026): image `jupyter/pyspark-notebook` chạy bản arm64 (Python 3.11.6, Spark 3.5.0, Java 17); bốn image
bde2020 chỉ có bản **amd64** (Docker chạy qua giả lập; NodeManager dùng Java 1.8, không có Python). Hệ quả cho YARN: gói Python của executor
phải đóng theo amd64 – `yarn_python_env.sh` đóng gói trong container miniforge chạy theo kiến trúc của NodeManager và ghim đúng phiên bản
numpy 1.24.4, pandas 2.0.3, pyarrow 13.0.0, statsmodels 0.14.0 của driver.

Kết quả chạy trên cụm ngày 26/11/2026: pipeline đọc Bronze từ HDFS và ghi Silver/Gold lên HDFS trong 87 giây (`local[*]` trong container),
47/47 phép đối soát `ĐẠT` (phép so với pyproj tự bỏ vì container không có pyproj); `hdfs fsck` báo `HEALTHY`, 0 block thiếu/hỏng
(`logs/hdfs_yarn_cli_sau_*.log`). KPI trùng bản chạy trên máy; tổng dự báo 2030 lệch 11 người do thư viện trong container khác phiên bản.

Kết quả chạy executor trên YARN ngày 26/11/2026 (`SPARK_MASTER=yarn`, client mode: driver trong container JupyterLab, 2 executor × 2 nhân
trên NodeManager, gói Python 247 MB gửi kèm qua `spark.archives`):

| Lần chạy | Bộ nhớ executor | Thời gian | Đối soát | Ứng dụng YARN | Ghi chú | Log |
|---|---|---|---|---|---|---|
| 1 | 1536m | 328 giây | 47/47 `ĐẠT` | `application_1795707941184_0001` FINISHED / SUCCEEDED | 2 executor bị kernel giết (exit 137) vì máy ảo Docker 7,7 GB hết RAM; Spark tự xin executor mới, chạy lại tác vụ | `logs/02_08_spark_20261126T154806Z.log` |
| 2 | 1g | 293 giây | 47/47 `ĐẠT` | `application_1795707941184_0002` FINISHED / SUCCEEDED | Không executor nào bị giết; RAM đỉnh cả cụm 7.387 MiB / 7.934 MiB | `logs/02_08_spark_20261126T155537Z.log` |
| 3 (code 5 bước, 27/11/2026) | 1g | 184 giây | 47/47 `ĐẠT` | `application_1795712053948_0001` FINISHED / SUCCEEDED | Không executor nào bị giết; Silver trên HDFS 42,0 MB (trước 79,9 MB) | `logs/02_08_spark_20261126T165529Z.log` |
| 4 (sau khi đổi tên thư mục, 27/11/2026) | 1g | 171 giây | 47/47 `ĐẠT` | `application_1795740876835_0001` FINISHED / SUCCEEDED | Không executor nào bị giết; HDFS `HEALTHY`, Silver 42,0 MB | `logs/spark_5_buoc_20261127T005707Z.log`, `logs/hdfs_yarn_cli_sau_20261127T010000Z.log` |

Sau lần 1, mặc định `SPARK_EXECUTOR_MEMORY` đổi từ `1536m` thành `1g`. KPI hai lần trùng bản chạy trên máy; tổng dự báo 2030 trên YARN
1.214.509 người, lệch 7 người so với bản trên máy (1.214.516) vì executor dùng thư viện amd64 khác phiên bản – nằm trong dung sai đối soát.
Lần 3 nhanh hơn lần 1–2 nhưng máy và tải nền khác nhau nên không so trực tiếp. YARN chậm hơn chạy trên máy (47–89 giây) vì cụm chỉ có một máy, NodeManager chạy qua giả lập amd64 và dữ liệu nhỏ; mục đích là chứng minh
cùng một code chạy được phân tán, không phải đo tăng tốc. Trạng thái HDFS và YARN sau khi chạy: `logs/hdfs_yarn_cli_sau_20261126T160110Z.log`.

## 9. Truy vết và chất lượng

| Cần biết | Xem ở |
|---|---|
| File nguồn tải từ đâu, lúc nào, có bị đổi không | `data/manifest_raw.csv` |
| Bảng Bronze nào, nạp ngày nào, từ file gốc nào | `outputs/tables/1_du_lieu_nguon/bronze_danh_muc.csv` |
| File kết quả do script nào tạo, bao nhiêu dòng, SHA-256 | `outputs/run_manifest.csv`, `7_kiem_chung/kiem_tra_run_manifest.csv` |
| Một lần chạy đã in ra gì | `logs/<tên bước>_<giờ UTC>.log` |
| Dữ liệu có vấn đề gì | `7_kiem_chung/qa_summary.csv` và các bảng `qa_*.csv` |
| Dòng nào bị loại, vì sao | `2_chat_luong/spark_quarantine_tong_hop.csv`, `2_chat_luong/spark_quarantine_mau.csv`, `silver/quarantine_records`, `silver/excluded_records` |
| Kết quả mới có khớp bộ số đã chốt không | `7_kiem_chung/doi_soat_spark.csv` |
| Spark thực thi truy vấn thế nào | `logs/plan_kpi_2024.txt`, `logs/plan_kpi_sql_2024.txt`, `logs/plan_pruning_2024.txt` |
| Ý nghĩa từng bảng, từng trường | `7_kiem_chung/tu_dien_du_lieu.csv` |
| Con số nào dùng trong báo cáo, lấy từ đâu | `7_kiem_chung/so_lieu_chot.csv` |

## 10. Hạn chế đã biết

- WorldPop là ước tính mô hình, không phải số đếm; lệch khoảng 21% so với số liệu NQ 1251 (`2_chat_luong/qa_official_population.csv`).
- Mạng đường gồm cả cao tốc dù xe máy không được đi; mức ảnh hưởng chưa đo (A6).
- KPI theo năm giữ nguyên vị trí 56 trạm hiện nay (A14).
- "Xã miền núi" phân loại theo địa hình, chưa đối chiếu danh sách chính thức (A7).
- Mạng đường và cơ sở y tế OSM tải ở thời điểm chạy `1a_tai_du_lieu.sh`; tải lại ngày khác có thể cho số liệu đường đi hơi khác.
- Danh sách 30 CSKCB không có phòng khám tư nhỏ nên số đếm CSKCB là sàn dưới (A17).
- Ngưỡng ứng viên gộp trạm chọn sau khi biết NQ 1251 gộp xã nào, nên mức khớp và p-value là lạc quan (A15).
- Phân loại khả năng thay thế của 15 gói dịch vụ là đánh giá của nhóm (A16).
- Dự báo ETS nhạy với sai số làm tròn; đã cố định bằng cách làm tròn chuỗi học, nhưng nên đọc kèm khoảng 80% (A9).
- Pipeline mới đã chạy thử trên macOS 26 (Apple Silicon): trên máy (`local[*]`), trên cụm Docker với HDFS, và với executor trên YARN.
  Cụm YARN chỉ có một NodeManager trên một máy nên không đo được tăng tốc; với máy ảo Docker 7,7 GB RAM, cụm dùng tới 7.387 MiB lúc đỉnh
  (mục 8) – nên tăng RAM cho Docker Desktop nếu chạy YARN thường xuyên.

Bảng giả định đầy đủ A1–A18 nằm trong [CLAUDE.md](../CLAUDE.md).
