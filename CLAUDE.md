# CLAUDE.md – MIS5007: Tiếp cận trạm y tế xã/phường Đà Nẵng cũ

File này Claude Code đọc tự động mỗi khi mở dự án. Giữ ngắn, sửa khi quy ước thay đổi.

## Bối cảnh
- Đồ án nhóm học phần MIS5007 – Dữ liệu lớn và Ứng dụng (thạc sĩ). Người đọc báo cáo: ban lãnh đạo Sở Y tế.
- Câu hỏi quản lý (bối cảnh sắp xếp đơn vị hành chính từ 01/01/2025): bao nhiêu % dân sống trong 3 km tới trạm y tế gần nhất; ai ở ngoài; vì sao; đặt thêm trạm ở đâu.
- **Phạm vi thời gian phân tích: 2015 → 31/12/2024** (10 mốc năm WorldPop 2015–2024, năm hiện trạng 2024). **Dự báo tới 2030** (Bước 08, chuỗi học 2015–2024, kết quả ghi file riêng, không trộn vào KPI các năm phân tích), **chỉ theo 56 xã cũ** (người dùng chốt 26/11/2026).
- Phạm vi: **56 trạm = 56 xã/phường Đà Nẵng cũ, bản đồ hành chính trước 01/01/2025** (45 phường, 11 xã) – danh sách gốc `data/reference/danh_sach_56_tram.csv`.
  Từ 01/01/2025 (NQ 1251/NQ-UBTVQH15) còn 47 đơn vị; từ 01/07/2025 TP mới có 94 đơn vị. Chỉ dùng hai mốc này khi được yêu cầu (kịch bản gộp 56→47 trạm nằm trong `b4_khuyen_nghi.py`).
- Nguồn: S1 WorldPop Global2 R2025A (lưới 100 m, 1 km, nhóm tuổi), S2 OpenStreetMap (ranh giới mốc 31/12/2024, mạng đường), toạ độ 56 trạm người dùng nhập tay (`data/raw/location-tyt.xlsx`), bổ trợ: AWS Terrain Tiles (độ cao), GADM 4.1 (ranh giới), số liệu dân số NQ 1251 (`data/reference/dan_so_chinh_thuc_nq1251.csv`), 8 đơn vị gộp theo NQ 1251 (`data/reference/nq1251_sap_xep.csv`), danh sách CSKCB Sở Y tế (`data/raw/danh-sach-cskcb.xlsx`), gói dịch vụ TYT theo TT 30/2024/TT-BYT (`data/reference/tt30_2024_goi_dich_vu.csv`).
- **Pipeline tinh gọn ngày 26/11/2026** theo bản thiết kế của người dùng: 8 bước logic, lưu trữ Medallion (`data/lake/bronze|silver|gold` trên máy hoặc HDFS – người dùng chốt, không dùng S3/PostGIS), một đường tính duy nhất (bỏ bản pandas tính trùng). Code 17 bước cũ đã xoá khỏi thư mục (xem lại bằng git, commit `aa15f50`); 10 bảng kết quả chốt 19/11/2026 ở `tests/tham_chieu_19_11_2026/` để đối soát. Các điểm thiết kế bị điều chỉnh theo dữ liệu (UTM 49N, hộp bao, quy tắc ±3% chỉ cảnh báo…): `docs/KIEN_TRUC.md` mục 6.
- **Báo cáo theo 7 giai đoạn vòng đời phân tích dữ liệu lớn, pipeline Spark theo khung 5 bước kỹ thuật** (người dùng chốt 27/11/2026): Đọc → Kiểm tra & làm sạch → Biến đổi → Tổng hợp → Ghi (`b1_doc.py` … `b5_ghi.py`, thay cho 4 giai đoạn `gd1`–`gd4` cũ). Báo cáo: `docs/BAO_CAO_7_GIAI_DOAN.md`. Bước 1–4 không ghi CSV/Gold mà gửi vào hàng chờ (`nc.xuat`, `nc.gold`), Bước 5 ghi một lần; Silver vẫn ghi ở Bước 2 (quarantine/excluded) và Bước 3 (điểm lưu). `excluded_records` chỉ lưu khoá tham chiếu (ban_ghi rỗng) từ 27/11/2026. Không làm dashboard (lý do ở báo cáo, Giai đoạn 6) – chỉ làm khi bài toán đổi. Tên thư mục (người dùng chốt 27/11/2026): code `scripts/0_…`, `1_thu_thap/`, `2_spark_5_buoc/`, `3_bao_cao/`; kết quả `outputs/tables/1_du_lieu_nguon … 7_kiem_chung, luoi_100m`; log Spark `logs/spark_5_buoc_*.log` (log trước 27/11 mang tiền tố `02_08_spark_`).
- Người dùng là người mới học code: **mọi dòng code đều có chú thích tiếng Việt** giải thích dòng đó làm gì; code sửa/thêm mới phải giữ quy ước này. Dự án phải chạy được trong VS Code (`.venv`, task, F5).

## Quy tắc bắt buộc
1. **Không bịa số.** Mọi con số trong câu trả lời và báo cáo phải trích một file cụ thể: `outputs/tables/<thư mục nội dung>/*.csv`, `logs/*.log`, `data/manifest_raw.csv`, `outputs/run_manifest.csv`. Chưa có file → nói "chưa có số", không ước đoán, không dùng dữ liệu giả lập. Không dẫn các tỷ lệ "tăng tốc 10–15 lần", "tiết kiệm 70% I/O" trong bản thiết kế ban đầu (chưa từng đo); dẫn `7_kiem_chung/so_sanh_parquet_csv.csv`.
2. Không sửa, không xoá `data/raw/` và `data/reference/`. Không tải dữ liệu ngoài `scripts/1_thu_thap/1a_tai_du_lieu.sh` khi chưa hỏi.
3. Mọi giả định mang mã A1–A18 (bảng dưới). Giả định mới → thêm A19, A20… vào bảng này và báo người dùng.
4. Chạy theo thứ tự `1a_tai_du_lieu.sh` → `chay_toan_bo.sh` (= `1b_chuyen_doi_bronze.py` → `1c_tra_cuu_khong_gian.py` → `2_spark_5_buoc/chay_spark.py` → `bao_cao.py`). Sau pipeline Spark đọc log mới nhất `logs/spark_5_buoc_*.log` (dòng `Doi chieu NQ 1251 … KHOP`, dòng `Doi soat: … LECH`), `7_kiem_chung/doi_soat_spark.csv`, `7_kiem_chung/qa_summary.csv`, các bảng `qa_*.csv`, `7_kiem_chung/so_sanh_parquet_csv.csv`; sau `bao_cao.py` đọc `7_kiem_chung/kiem_tra_run_manifest.csv`, `7_kiem_chung/so_lieu_chot.csv`. Báo mọi dòng `CẦN XEM`, `LỆCH`, `FAIL` trước khi dùng số.
5. Khi sửa code: trong pipeline Spark, Bước 1–4 gửi bảng CSV vào hàng chờ bằng `nc.xuat("ten.csv", df, S, note=…)` và bảng Gold bằng `nc.gold(df, "gold/…")`, Bước 5 (`b5_ghi.py`) ghi qua `save_csv` / `nc.ghi`; ngoài Spark ghi CSV qua `save_csv(df, bang("ten.csv"), S)` / `register` trong `scripts/chung/common.py` để `run_manifest.csv` đủ; Silver ghi qua `nc.ghi` / `nc.luu` của `spark_chung.py` (kiểm tra `_SUCCESS`). Không đổi tên cột đầu ra khi chưa hỏi (vì vậy `luoi_100m/tiep_can_luoi_100m.csv` vẫn giữ tên cột `dist_m_chi_osm`). Comment từng dòng.
6. Giấy phép phải ghi trong báo cáo: WorldPop CC BY 4.0; OSM ODbL 1.0 ("© OpenStreetMap contributors", CSDL phái sinh chia sẻ lại cùng ODbL); GADM chỉ học thuật, không phân phối lại (nên `data/lake/` không đưa lên GitHub); AWS Terrain Tiles ghi nguồn.
7. #Truthmode: WorldPop là **ước tính mô hình**, không phải đếm đầu người. `silver/quarantine_records` chỉ chứa lỗi dữ liệu thật; NoData của nguồn và dòng ngoài phạm vi vào `silver/excluded_records` (cột `loai` = `LOI_DU_LIEU` / `NGOAI_PHAM_VI`); không tạo lỗi giả để minh hoạ và không gọi dòng ngoài phạm vi là "lỗi". Lệch tổng dân so với số liệu chính thức là **cảnh báo**, không phải lỗi. Số sau 2024 là **dự báo** (Bước 08) hoặc **số chiếu** của WorldPop: luôn gọi đúng tên, kèm khoảng 80%; backtest chỉ đo khả năng tái tạo chuỗi WorldPop; baseline thắng mô hình chuẩn thì nói thẳng và dùng baseline.
8. Không tạo dữ liệu giả lập trong thư mục dự án (file tạo dữ liệu giả `tests/make_test_fixture.py` đã xoá 26/11/2026).
9. Trả lời người dùng bằng tiếng Việt, tách rõ **dữ liệu** và **giả định**, chỉ ra rủi ro và câu hỏi còn mở.

## Lệnh
```bash
bash scripts/0_kiem_tra_may.sh           # kiểm tra máy: công cụ, .venv, Java 17, pyspark, Docker, file bắt buộc, cổng
bash scripts/1_thu_thap/1a_tai_du_lieu.sh           # tải dữ liệu (~1 GB, gồm lưới 1 km các năm chiếu 2025–2030 cho Bước 08)
bash scripts/chay_toan_bo.sh              # 1 thu thập (1b, 1c) → 2 Spark 5 bước → 3 báo cáo; khoảng 1 phút, không cần Docker
.venv/bin/python scripts/2_spark_5_buoc/chay_spark.py   # chỉ chạy Spark (local[*]); HDFS_URI=... để dùng HDFS, SPARK_MASTER=yarn để dùng YARN
bash scripts/cum_hdfs_yarn/hdfs_yarn_cli.sh truoc       # (cụm Docker) đưa data/lake/bronze lên HDFS; "sau" để liệt kê kết quả
bash scripts/cum_hdfs_yarn/yarn_python_env.sh           # (một lần) gói Python cho executor trên YARN
```
Chạy lẻ một bước: `.venv/bin/python scripts/3_bao_cao/bao_cao.py` hoặc mở file rồi F5 trong VS Code. Bảng kết quả mới phải được thêm vào `THU_MUC_BANG` trong `scripts/chung/common.py` (đúng thư mục nội dung) rồi ghi qua hàng chờ (`nc.xuat`) hoặc `save_csv(df, bang("ten.csv"), S)`. Chạy không Docker / có Docker và mọi hướng dẫn khác: `docs/HUONG_DAN_CHAY.md`.
Tài liệu: `docs/BAO_CAO_7_GIAI_DOAN.md` (báo cáo theo 7 giai đoạn vòng đời, pipeline 5 bước, số đo tối ưu), `docs/HUONG_DAN_CHAY.md` (cài và chạy: Phần A không Docker, Phần B có Docker, Phần C tra cứu), `docs/HUONG_DAN_DEMO.md` (kịch bản demo, giới thiệu sản phẩm trên VS Code), `docs/KIEN_TRUC.md` (kiến trúc, đối chiếu thiết kế, lý do) – sửa code thì sửa tài liệu tương ứng.

## Cấu trúc
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

## Giả định
| Mã | Nội dung | Ở đâu |
|---|---|---|
| A1 | Ngưỡng KPI chính 3 km đường chim bay là **giả định do nhóm tự nghiên cứu chốt** (người dùng xác nhận 17/11/2026), không dựa trên văn bản quy định nào → luôn kèm độ nhạy 2 km, 5 km, theo mạng đường và theo 15 phút xe máy | common.py, b4_tong_hop / b4_khuyen_nghi |
| A2 | Dân số = WorldPop R2025A 100 m constrained (ước tính mô hình); năm hiện trạng 2024 (năm cuối trước sắp xếp 01/01/2025); ranh giới xã: xem A12 | 1b, b2_kiem_tra_lam_sach, b3_bien_doi |
| A3 | Tỷ lệ 60+ của ô 100 m = tỷ lệ 60+ của ô 1 km chứa nó (ô 1 km = 0 → tỷ lệ trung bình xã) | b3_bien_doi (T1) |
| A4 | Chuỗi hạ cấp từ 1 km (giữ phân bố ô 100 m năm 2024) chỉ dùng làm chuỗi học của dự báo và được kiểm định (`6_du_bao/validation_downscale_A4.csv`); KPI dùng 100 m trực tiếp vì cả 10 năm đều có file 100 m | b4_du_bao |
| A5 | Trạm không có toạ độ nhập tay hợp lệ: vị trí ước lượng = tâm dân cư xã (trọng số dân 2024); KPI luôn kèm bản "chỉ trạm có toạ độ thực". Hiện 56/56 trạm có toạ độ thực | b3_bien_doi |
| A6 | Mạng đường OSM hiện tại, hai chiều, tốc độ xe máy theo loại đường, đoạn ra đường đi bộ 5 km/h. Mạng đường **gồm cả cao tốc** (142,6 km, `1_du_lieu_nguon/qa_network.csv`) dù xe máy không được đi – người dùng chốt giữ nguyên 17/11/2026; phải nêu là hạn chế trong báo cáo, mức ảnh hưởng chưa đo | 1c, b3_bien_doi |
| A7 | "Xã miền núi" theo địa hình: xã có độ dốc TB theo dân > 5° hoặc độ cao TB theo dân > 50 m (chưa đối chiếu danh sách chính thức) | b3_bien_doi (T4) |
| A8 | Chẩn đoán vùng trắng: địa hình (dốc > 8°, cao > 150 m, hệ số vòng > 1,5); phân tán (< 10 người/ô hoặc vùng < 300 người) | b4_tong_hop / b4_khuyen_nghi |
| A9 | Dự báo 2030: tổng dân từng xã = dự báo của mô hình thắng backtest trên chuỗi 2015–2024 (chuỗi học làm tròn tới 10⁻⁶ người trước khi khớp ETS vì ETS nhạy với sai số làm tròn 10⁻¹⁰ – sửa 26/11/2026); trong xã, dân 2030 phân bố theo nhịp tăng ô 1 km của số chiếu WorldPop 2030; khoảng 80% lấy từ sai số backtest ở tầm 5 năm trong khi dự báo xa 6 năm nên có thể hơi hẹp; vị trí 56 trạm giữ nguyên; chỉ theo 56 xã cũ | b4_du_bao |
| A10 | Ứng viên vị trí mới = ô 100 m đông dân nhất mỗi khối 300 × 300 m; bán kính phủ 3 km | b4_tong_hop / b4_khuyen_nghi |
| A11 | Lệch WorldPop so với số liệu NQ 1251 chấp nhận ±20% (mốc số liệu NQ chưa xác định năm); quy tắc ±3% của bản thiết kế chỉ là cảnh báo (người dùng chốt 26/11/2026) | b2_kiem_tra_lam_sach |
| A12 | Ranh giới 56 xã thực tế lấy từ GADM 4.1: OSM mốc 31/12/2024 đã vẽ theo bản đồ sau sáp nhập nên chỉ khớp 45/56 xã, `1c_tra_cuu_khong_gian.py` tự chọn nguồn khớp nhiều hơn (xem `1_du_lieu_nguon/qa_boundaries.csv`, cột `nguon_dung`) | 1c |
| A13 | Toạ độ trạm lấy từ file người dùng nhập tay `data/raw/location-tyt.xlsx`. Nguồn theo người dùng (17/11/2026): toạ độ được kiểm tra theo Cổng dữ liệu Đà Nẵng (congdulieu.vn), dữ liệu cập nhật 12/12/2024 – mốc này nằm trước ngày sắp xếp 01/01/2025 nên khớp phạm vi 56 trạm. Claude chưa tự đối chiếu với cổng dữ liệu; chưa kiểm chứng thực địa; giấy phép dùng lại dữ liệu của cổng chưa được xác nhận. Dòng sai mẫu / ngoài hộp bao / tên lạ vào `silver/quarantine_records` (xem `2_chat_luong/spark_quarantine_tong_hop.csv`, `2_chat_luong/qa_stations.csv`) và trạm đó quay về ước lượng A5. Mỗi lần file này đổi phải chạy lại `chay_toan_bo.sh` | b2_kiem_tra_lam_sach, b3_bien_doi |
| A14 | KPI theo năm 2015–2024 giữ nguyên vị trí 56 trạm như hiện nay (không có dữ liệu lịch sử vị trí trạm); thay đổi qua các năm chỉ phản ánh thay đổi phân bố dân cư | b3_bien_doi, b4_tong_hop |
| A15 | **Trùng lặp cấp chuyên môn (sửa 17/11/2026):** ứng viên **rà soát** gộp trạm = TYT có **≥ 6 CSKCB trong 2 km** đường chim bay, đếm từ `data/raw/danh-sach-cskcb.xlsx`. Ngưỡng 6/2 km do nhóm chọn **sau khi** đã biết NQ 1251 gộp xã nào, nên p-value trong `5_khuyen_nghi/doi_soat_nq1251_quy_tac.csv` là **lạc quan** (chọn hậu nghiệm trên 5 phương án, 56 quan sát): không được đọc như kiểm định mù. Bản A15 cũ (≥ 3 cơ sở/1 km + dân dưới trung vị + TYT khác ≤ 1,5 km) vẫn được tính trong cùng bảng để đối chiếu. Đầu ra là **danh sách rà soát**, không phải đề nghị đóng cửa (xem A16) | b4_tong_hop / b4_khuyen_nghi |
| A16 | **Khả năng thay thế TYT (nhóm diễn giải, 17/11/2026):** 15 nhóm gói dịch vụ trong `data/reference/tt30_2024_goi_dich_vu.csv` là **nội dung Điều 2 Thông tư 30/2024/TT-BYT** (ban hành 04/11/2024, hiệu lực 19/12/2024); nhưng hai cột `nhom_chuc_nang` và `benh_vien_tuyen_tren_thay_the` là **đánh giá của nhóm**, không có trong văn bản. Bản PDF trong `data/reference/30-byt.pdf` là bản thảo (trống số/ngày) nên báo cáo phải dẫn bản chính thức. Phân cấp ban đầu / cơ bản / chuyên sâu theo Luật KCB 15/2023/QH15 Điều 104; TYT thuộc cấp ban đầu – chưa đối chiếu quyết định xếp cấp cụ thể của Sở Y tế | b4_tong_hop / b4_khuyen_nghi |
| A17 | **Danh sách CSKCB (17/11/2026):** `data/raw/danh-sach-cskcb.xlsx` do người dùng cung cấp, 30 cơ sở (16 tuyến huyện, 13 tuyến tỉnh, 1 tuyến trung ương; 27 cấp cơ bản, 2 cấp chuyên sâu, 1 cấp ban đầu), toạ độ đọc được cả 30. Claude **chưa tự đối chiếu** với công bố gốc của Sở Y tế; danh sách không chứa TYT nào và không chứa phòng khám tư nhỏ, nên **số đếm là sàn dưới**. Nếu danh sách thiếu cơ sở ở Hoà Vang thì mức chênh trung tâm – miền núi bị phóng đại | b2_kiem_tra_lam_sach, b4_tong_hop / b4_khuyen_nghi |
| A18 | **Vùng lõm theo thời gian (26/11/2026, theo bản thiết kế):** ô cần **trên 20 phút** xe máy tới trạm gần nhất (theo mạng đường, A6) được tính là vùng lõm, song song với ngưỡng 15 phút và 5 km; ngưỡng 20 phút lấy từ bản thiết kế của người dùng, không dựa trên văn bản quy định | common.py, b4_tong_hop / b4_khuyen_nghi |
