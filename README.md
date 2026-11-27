# Tiếp cận trạm y tế xã/phường – Đà Nẵng cũ

Dự án đo mức độ người dân tiếp cận được trạm y tế (TYT) ở 56 xã/phường Đà Nẵng theo bản đồ hành chính **trước** đợt sắp xếp 01/01/2025,
trên 10 mốc năm 2015–2024 (năm hiện trạng 2024), và đặt TYT cạnh 30 cơ sở khám chữa bệnh (CSKCB) khác trên địa bàn.
Đồ án học phần MIS5007 – Dữ liệu lớn và Ứng dụng; người đọc kết quả là ban lãnh đạo Sở Y tế.

## Kết luận chính

**Không tăng trạm đại trà, cũng không giảm trạm đại trà.** Dữ liệu cho thấy đây là bài toán **đặt sai chỗ và phân bổ sai tải**,
không phải bài toán đếm số trạm. Mỗi con số dưới đây dẫn về một file trong `outputs/tables/` (trừ khi ghi khác).

| Nhận định | Số liệu | File |
|---|---|---|
| Thêm trạm đại trà lợi ít | 98,69% dân đã ở trong 3 km; thêm 5 điểm đề xuất chỉ nâng lên 99,53% | `5_khuyen_nghi/greedy_before_after.csv` |
| Bớt trạm đại trà không còn chỗ | 17 TYT có từ 6 CSKCB trong 2 km; 14 trong số đó đã bị NQ 1251 gộp từ 01/01/2025, chỉ còn 3 (Khuê Trung, Thạch Thang, Hòa Thuận Tây) để rà soát | `5_khuyen_nghi/ung_vien_gop_tram.csv` |
| Đặt sai chỗ | TYT xã Hòa Ninh cách tâm dân cư của xã 3.150 m – trạm duy nhất vượt 3 km; Hòa Ninh có tỷ lệ dân trong 3 km thấp nhất 56 xã (42,19%; theo đường đi 34,90%) | `7_kiem_chung/qa_summary.csv`, `2_chat_luong/tram_y_te_56.csv`, `3_mo_ta_kpi/kpi_theo_xa_spark.csv` |
| Phân bổ sai tải | Dân thuộc vùng phục vụ của mỗi trạm từ 3.479 (Hòa Bắc) tới 76.954 người (Hòa Khánh Bắc), chênh 22 lần; trung vị 19.488 | `3_mo_ta_kpi/tram_vung_phuc_vu_spark.csv` |
| Nguyên nhân không phải dân số tăng | Dân tăng từ 1.079.220 (2015) lên 1.177.774 (2024) nhưng tỷ lệ trong 3 km giữ 98,69%; dự báo 2030 vẫn 98,69% | `3_mo_ta_kpi/kpi_theo_nam_spark.csv`, `6_du_bao/access_2030_total.csv` |
| Không chỉ là đường xá | 91 trong 98 cụm dân cách trạm trên 5 km mang đồng thời hai nguyên nhân: địa hình và dân cư phân tán | `4_chan_doan/white_zones.csv` |

**CSKCB khác không thay thế được TYT**, xét ở ba lớp:

| Lớp | Bằng chứng | Nguồn |
|---|---|---|
| Pháp lý – chức năng (quyết định) | Thông tư 30/2024/TT-BYT giao TYT 15 nhóm gói dịch vụ. Theo đánh giá của nhóm, 14/15 là dự phòng hoặc quản lý sức khoẻ theo địa bàn (tiêm chủng, quản lý thai, bệnh không lây nhiễm, hồ sơ sức khoẻ điện tử, giám sát dịch, an toàn thực phẩm, xác định tình trạng nghiện); chỉ y học cổ truyền là cơ sở khác cung cấp tương đương được | `data/reference/tt30_2024_goi_dich_vu.csv` |
| Cấp chuyên môn | Luật KCB 15/2023/QH15 (Điều 104) chia 3 cấp: ban đầu – cơ bản – chuyên sâu. Trong 30 CSKCB: 27 cấp cơ bản, 2 chuyên sâu, 1 ban đầu (16 tuyến huyện, 13 tuyến tỉnh, 1 tuyến trung ương); không có TYT nào. Cơ sở cấp ban đầu duy nhất là một trung tâm bác sĩ gia đình tư nhân ở Cẩm Lệ | `2_chat_luong/cskcb_danang.csv` |
| Địa lý | Ở Hòa Vang, 10/11 TYT không có CSKCB nào trong 2 km, 8/11 không có trong 3 km; CSKCB gần nhất trung bình cách 5.332 m, xa nhất 11.638 m (Hòa Bắc). Ở Hải Châu và Thanh Khê, trung bình 7 CSKCB trong 2 km | `5_khuyen_nghi/tyt_cskcb_theo_nhom.csv`, `5_khuyen_nghi/tyt_cskcb.csv` |

Đọc kèm các giới hạn ở cuối trang: dân số là ước tính mô hình, ngưỡng 3 km và cách phân loại gói dịch vụ là giả định của nhóm.

## Cách hệ thống chạy

Báo cáo trình bày theo **7 giai đoạn của vòng đời phân tích dữ liệu lớn** ([docs/BAO_CAO_7_GIAI_DOAN.md](docs/BAO_CAO_7_GIAI_DOAN.md)).
Dữ liệu lưu theo kiến trúc **Medallion** (Bronze → Silver → Gold) và xử lý bằng **PySpark theo khung 5 bước kỹ thuật**:

```
[1. ĐỌC] ➔ [2. KIỂM TRA & LÀM SẠCH] ➔ [3. BIẾN ĐỔI] ➔ [4. TỔNG HỢP] ➔ [5. GHI]
```

(1) đọc Bronze với schema khai báo; (2) kiểm tra Null / kiểu / miền giá trị / trùng, tách quarantine và excluded; (3) chuẩn hoá, nối đa nguồn
(broadcast join, Dijkstra), tạo cột nghiệp vụ; (4) KPI bằng groupBy / hàm cửa sổ, vùng lõm, khuyến nghị, dự báo 2030, đối soát; (5) ghi Gold Parquet
phân vùng và bảng CSV phục vụ, kiểm tra ghi. Code và kết quả được đặt tên theo vòng đời (1 thu thập → 2 Spark 5 bước → 3 báo cáo).
Chi tiết: [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md). Toàn bộ code có chú thích tiếng Việt cho từng dòng.

## Bắt đầu nhanh

Cần macOS, VS Code, Homebrew và khoảng 5 GB ổ đĩa trống (Docker Desktop chỉ cần khi trình diễn HDFS/YARN).
Kho GitHub đã kèm các file nhóm tự chuẩn bị; cách lấy dự án khi không dùng GitHub: [hướng dẫn chạy](docs/HUONG_DAN_CHAY.md#a2-lấy-thư-mục-dự-án).

```bash
git clone https://github.com/tranhuy99dlk/tyt-danang.git mis5007_tram_y_te && cd mis5007_tram_y_te
brew install uv openjdk@17                                            # công cụ Python và Java 17 cho Spark
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.lock.txt
bash scripts/0_kiem_tra_may.sh                                       # máy đủ điều kiện chưa
bash scripts/1_thu_thap/1a_tai_du_lieu.sh                                       # tải khoảng 1 GB dữ liệu nguồn mở
bash scripts/chay_toan_bo.sh                                          # thu thập → Spark 5 bước → báo cáo (không cần Docker)
```

Trong VS Code: mở thư mục dự án, bấm **Cmd+Shift+B** để chạy toàn bộ, hoặc mở một file chạy được trong `scripts/` (ví dụ `scripts/3_bao_cao/bao_cao.py`) và bấm **F5**.
Chạy đúng thì phần Spark in `Doi chieu NQ 1251: ... KHOP` và `Doi soat: 48 phep, 0 LECH.`; trên máy thử lệnh cuối mất khoảng 1 phút.
Mở `outputs/tables/3_mo_ta_kpi/kpi_tong_hop_spark.csv` để xem KPI chính.

## Tài liệu

| Bạn là | Đọc |
|---|---|
| Người mới, cần cài và chạy từng bước trên VS Code: **không cần Docker** (Phần A) hoặc **có Docker** HDFS/YARN (Phần B); đọc kết quả, đổi tham số, xử lý lỗi | [docs/HUONG_DAN_CHAY.md](docs/HUONG_DAN_CHAY.md) |
| Người chuẩn bị demo và giới thiệu sản phẩm: kịch bản từng bước trên VS Code, câu nói mẫu, câu hỏi thường gặp | [docs/HUONG_DAN_DEMO.md](docs/HUONG_DAN_DEMO.md) |
| Người đọc báo cáo: 7 giai đoạn vòng đời phân tích dữ liệu lớn, pipeline 5 bước, kết quả, số đo tối ưu | [docs/BAO_CAO_7_GIAI_DOAN.md](docs/BAO_CAO_7_GIAI_DOAN.md) |
| Người cần hiểu thiết kế: kiến trúc, pipeline Spark, đối chiếu với bản thiết kế, lý do lựa chọn | [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md) |
| Người sửa code hoặc viết báo cáo: quy tắc làm việc và bảng giả định A1–A18 | [CLAUDE.md](CLAUDE.md) |

## Cấu trúc thư mục

```
scripts/                              code, chia theo vòng đời: 0 kiểm tra → 1 thu thập → 2 Spark 5 bước → 3 báo cáo
  0_kiem_tra_may.sh                   kiểm tra máy trước khi chạy
  1_thu_thap/                         THU THẬP (giai đoạn 2 của vòng đời, Python)
    1a_tai_du_lieu.sh                   tải nguồn mở về data/raw (SHA-256, giấy phép)
    1b_chuyen_doi_bronze.py             chuyển đổi đa nguồn vào Bronze: raster/Excel -> CSV, giữ nguyên giá trị
    1c_tra_cuu_khong_gian.py            ranh giới, ô -> xã, độ cao, đồ thị đường (phần hình học Spark không làm được)
  2_spark_5_buoc/                     PIPELINE PYSPARK 5 BƯỚC: Đọc → Kiểm tra & làm sạch → Biến đổi → Tổng hợp → Ghi
    chay_spark.py                       chạy lần lượt 5 bước, đo thời gian và số job từng bước
    b1_doc.py                           Bước 1 · Đọc: Bronze (CSV, GeoJSON) với schema khai báo tường minh
    b2_kiem_tra_lam_sach.py             Bước 2 · Kiểm tra & làm sạch: Null/kiểu/miền giá trị/trùng; quarantine / excluded
    b3_bien_doi.py                      Bước 3 · Biến đổi: chuẩn hoá, nối đa nguồn (broadcast, Dijkstra), cột nghiệp vụ -> Silver
    b4_tong_hop.py                      Bước 4 · Tổng hợp: KPI groupBy/cửa sổ, vùng lõm, đối soát; gọi b4_khuyen_nghi.py, b4_du_bao.py
    b5_ghi.py                           Bước 5 · Ghi: Gold Parquet phân vùng, bảng CSV, đo Parquet/CSV, kiểm tra _SUCCESS
    spark_chung.py                      khung Spark dùng chung (SparkSession, ghi/đọc Parquet an toàn, hàng chờ kết quả, đo thời gian)
  3_bao_cao/bao_cao.py                BÁO CÁO (giai đoạn 6): hình h1–h9, từ điển dữ liệu, kiểm tra sổ, số liệu chốt
  chung/                              dùng chung: common.py (mọi tham số, bảng THU_MUC_BANG), hinh_hoc.py
  cum_hdfs_yarn/                      tiện ích cụm Docker: hdfs_yarn_cli.sh, yarn_python_env.sh
  chay_toan_bo.sh                     chạy 1b -> 1c -> 2 -> 3 (Cmd+Shift+B trong VS Code)
data/raw/               dữ liệu nguồn tải về + location-tyt.xlsx (toạ độ trạm) + danh-sach-cskcb.xlsx (30 CSKCB)
data/reference/         danh sách 56 trạm, dân số NQ 1251, 8 đơn vị gộp theo NQ 1251, 15 gói dịch vụ TT 30/2024
data/lake/              bronze/ (CSV thô theo nguồn, ngày nạp)  silver/ (Parquet sạch + quarantine/excluded)  gold/ (KPI, dự báo)
outputs/tables/         bảng kết quả CSV, chia theo nhóm nội dung của báo cáo:
  1_du_lieu_nguon/  2_chat_luong/  3_mo_ta_kpi/  4_chan_doan/  5_khuyen_nghi/  6_du_bao/  7_kiem_chung/  luoi_100m/
outputs/figures/        hình h1–h5, h7–h9 (h6 chỉ có khi có xã giảm tiếp cận đến 2030)
outputs/run_manifest.csv  mỗi file kết quả: script tạo ra, số dòng, SHA-256
tests/tham_chieu_19_11_2026/  10 bảng kết quả đã chốt 19/11/2026, pipeline tự đối soát với bộ này
docs/  docker/  notebooks/  .vscode/   tài liệu, cụm HDFS + YARN + JupyterLab, notebook, cấu hình VS Code
logs/                   nhật ký từng lần chạy
```

`data/lake/`, `outputs/` và `logs/` đều tạo lại được bằng `bash scripts/chay_toan_bo.sh`.
`data/raw/location-tyt.xlsx`, `data/raw/danh-sach-cskcb.xlsx` và `data/reference/` thì không; đừng xoá.

## Nguồn dữ liệu và giấy phép

| Nguồn | Dùng cho | Giấy phép / ghi chú |
|---|---|---|
| WorldPop Global2 R2025A | Dân số lưới 100 m, 1 km, nhóm tuổi | CC BY 4.0 – ghi nguồn WorldPop, University of Southampton |
| OpenStreetMap (Overpass API) | Mạng đường, ranh giới đối chiếu | ODbL 1.0 – ghi "© OpenStreetMap contributors"; dữ liệu phái sinh chia sẻ lại phải cùng ODbL |
| GADM 4.1 | Ranh giới 56 xã | Chỉ dùng học thuật/phi thương mại, không phân phối lại |
| AWS Terrain Tiles (Mapzen) | Độ cao, độ dốc | Ghi nguồn |
| Nhóm thực hiện | Toạ độ 56 trạm (nhập tay, kiểm tra theo Cổng dữ liệu Đà Nẵng congdulieu.vn, dữ liệu cập nhật 12/12/2024) | Ghi nguồn cổng dữ liệu; điều kiện dùng lại chưa được xác nhận |
| Danh sách CSKCB của Sở Y tế (người dùng cung cấp) | 30 cơ sở: tuyến, hạng, cấp chuyên môn, toạ độ | Chưa đối chiếu với công bố gốc (A17) |
| Thông tư 30/2024/TT-BYT | 15 nhóm gói dịch vụ của TYT | Văn bản pháp luật; cột phân loại chức năng là đánh giá của nhóm (A16) |
| NQ 1251/NQ-UBTVQH15 và báo chí dẫn lại | 8 đơn vị gộp, số liệu dân số đối chiếu | Xem cột nguồn trong `data/reference/` |

## Giới hạn cần biết trước khi dùng số liệu

- Dân số là ước tính mô hình của WorldPop, không phải số đếm; lệch khoảng 21% so với số liệu NQ 1251 ở các phường trung tâm.
- Ngưỡng 3 km là giả định do nhóm tự nghiên cứu (A1); luôn đọc kèm kết quả 2 km, 5 km, theo đường đi và theo 15 phút.
- Toạ độ trạm do nhóm nhập tay, chưa kiểm chứng thực địa (A13). 8 trạm nằm ngoài ranh giới xã của chính nó, nhưng xa nhất 104 m – mức lệch này nhiều khả năng do ranh giới GADM thô, **không** dùng làm bằng chứng đặt sai chỗ.
- Kết luận "đặt sai chỗ" ở Hòa Ninh dựa trên khoảng cách trạm – tâm dân cư; tác động nếu dời trạm **chưa được tính** (chưa có số).
- Danh sách 30 CSKCB không có phòng khám tư nhỏ nên mọi số đếm CSKCB là sàn dưới; địa chỉ trong danh sách dùng tên đơn vị hành chính sau 2025, tức là danh sách hiện hành chứ không phải ảnh chụp năm 2024 (A17).
- Ngưỡng ứng viên rà soát gộp trạm (≥ 6 CSKCB trong 2 km) được chọn **sau khi** đã biết NQ 1251 gộp xã nào, nên mức khớp 14/17 và p-value trong `5_khuyen_nghi/doi_soat_nq1251_quy_tac.csv` là lạc quan (A15). Danh sách ứng viên là danh sách rà soát, không phải đề nghị đóng trạm.
- Cột "bệnh viện tuyến trên thay thế được" trong `tt30_2024_goi_dich_vu.csv` là đánh giá của nhóm; bản PDF trong `data/reference/` là bản thảo, báo cáo phải dẫn bản chính thức (A16).
- Số liệu sau năm 2024 (`forecast_*.csv`, `access_2030_*.csv`, hình h5) là **dự báo** từ chuỗi 2015–2024, không phải số đo; đọc kèm khoảng 80%.
- Quy tắc "tổng dân lệch số liệu chính thức quá ±3%" của bản thiết kế chỉ được dùng để **cảnh báo**: chỉ 2/17 bản ghi mỗi năm đạt (`2_chat_luong/qa_official_population.csv`) vì WorldPop là ước tính mô hình, không phải lỗi dữ liệu.
- Ngưỡng "vùng lõm" 20 phút xe máy là giả định mới A18; các chỉ số p95, khoảng cách tới CSKCB là chỉ số bổ sung ngày 26/11/2026.

Chi tiết: [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md#10-hạn-chế-đã-biết) và bảng giả định trong [CLAUDE.md](CLAUDE.md).
