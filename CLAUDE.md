# CLAUDE.md – MIS5007: Tiếp cận trạm y tế xã/phường Đà Nẵng cũ

File này Claude Code đọc tự động mỗi khi mở dự án. Giữ ngắn, sửa khi quy ước thay đổi.

## Bối cảnh
- Đồ án nhóm học phần MIS5007 – Dữ liệu lớn và Ứng dụng (thạc sĩ). Người đọc báo cáo: ban lãnh đạo Sở Y tế.
- Câu hỏi quản lý (bối cảnh sắp xếp đơn vị hành chính từ 01/01/2025): bao nhiêu % dân sống trong 3 km tới trạm y tế gần nhất; ai ở ngoài; vì sao; đặt thêm trạm ở đâu.
- **Phạm vi thời gian phân tích: 2015 → 31/12/2024** (10 mốc năm WorldPop 2015–2024, năm hiện trạng 2024; người dùng mở rộng từ 2019 lên 2015 ngày 16/11/2026). **Dự báo tới 2030** bằng `08_forecast.py` được bật lại theo yêu cầu người dùng ngày 17/11/2026 (đã tắt từ 16/11): chuỗi học 2015–2024, kết quả dự báo tách riêng, không trộn vào KPI các năm phân tích.
- Phạm vi: **56 trạm = 56 xã/phường Đà Nẵng cũ, bản đồ hành chính trước 01/01/2025** (45 phường, 11 xã) – danh sách gốc `data/reference/danh_sach_56_tram.csv`.
  Từ 01/01/2025 (NQ 1251/NQ-UBTVQH15) còn 47 đơn vị; từ 01/07/2025 TP mới có 94 đơn vị. Chỉ dùng hai mốc này khi được yêu cầu (kịch bản gộp 56→47 nằm trong `07_diagnose.py`).
- Nguồn: S1 WorldPop Global2 R2025A (lưới 100 m, 1 km, nhóm tuổi), S2 OpenStreetMap (ranh giới mốc 31/12/2024, mạng đường), toạ độ 56 trạm người dùng nhập tay (`data/raw/location-tyt.xlsx`), bổ trợ: AWS Terrain Tiles (độ cao), GADM 4.1 (đối chiếu ranh giới), số liệu dân số NQ 1251 (`data/reference/dan_so_chinh_thuc_nq1251.csv`).

## Quy tắc bắt buộc
1. **Không bịa số.** Mọi con số trong câu trả lời và báo cáo phải trích một file cụ thể: `outputs/tables/*.csv`, `logs/*.log`, `data/manifest_raw.csv`, `outputs/run_manifest.csv`. Chưa có file → nói "chưa có số", không ước đoán, không dùng dữ liệu giả lập.
2. Không sửa, không xoá `data/raw/` và `data/reference/`. Không tải dữ liệu ngoài `scripts/01_download.sh` khi chưa hỏi.
3. Mọi giả định mang mã A1–A14 (bảng dưới). Giả định mới → thêm A15, A16… vào bảng này và báo người dùng.
4. Chạy theo thứ tự 01 → `run_all.sh` (02–09, 13, 14, 10, 12, 11a) → 11 (Spark). Sau bước 11 đọc `doi_soat_spark.csv`. Sau mỗi bước: đọc log mới nhất trong `logs/` và các bảng `qa_*.csv`; báo mọi dòng `CẦN XEM`, `LỆCH`, `FAIL` trước khi chạy bước sau.
5. Khi sửa code: ghi file đầu ra qua `save_csv` / `register` trong `scripts/common.py` để `run_manifest.csv` đủ. Không đổi tên cột đầu ra khi chưa hỏi.
6. Giấy phép phải ghi trong báo cáo: WorldPop CC BY 4.0; OSM ODbL 1.0 ("© OpenStreetMap contributors", CSDL phái sinh chia sẻ lại cùng ODbL); GADM chỉ học thuật, không phân phối lại; AWS Terrain Tiles ghi nguồn.
7. #Truthmode: WorldPop là **ước tính mô hình**, không phải đếm đầu người. Dữ liệu cách ly (quarantine) phải là dòng lỗi thật của nguồn, không tạo lỗi giả để minh hoạ. Số sau 2024 là **dự báo** (bước 08) hoặc **số chiếu** của WorldPop: luôn gọi đúng tên, kèm khoảng 80%; backtest chỉ đo khả năng tái tạo chuỗi WorldPop; baseline thắng mô hình chuẩn thì nói thẳng và dùng baseline.
8. `tests/make_test_fixture.py` tạo dữ liệu GIẢ LẬP chỉ để thử code; không bao giờ chạy nó trong thư mục dự án thật.
9. Trả lời người dùng bằng tiếng Việt, tách rõ **dữ liệu** và **giả định**, chỉ ra rủi ro và câu hỏi còn mở.

## Lệnh
```bash
bash scripts/00_check.sh                  # kiểm tra máy: công cụ, Docker, file bắt buộc, cổng
source .venv/bin/activate                 # môi trường ảo (Python 3.11, uv; cài từ requirements.lock.txt)
bash scripts/01_download.sh               # tải dữ liệu (~1 GB, gồm lưới 1 km các năm chiếu 2025–2030 cho bước 08)
bash scripts/run_all.sh                   # 02–09, 13, 14, 10, 12, 11a (pandas + tạo CSV thô cho Spark)
bash scripts/run_spark.sh                 # bước 11: Spark trong container, các vùng ở data/spark/
bash scripts/11b_hdfs_yarn_cli.sh truoc   # (cụm Docker của dự án) đưa CSV thô lên HDFS; "sau" để liệt kê kết quả
```
Chạy lẻ một bước: `cd scripts && python 06_kpi.py`. Bước 11 trên HDFS và mọi hướng dẫn khác: `docs/HUONG_DAN_SU_DUNG.md`.
Tài liệu: `docs/HUONG_DAN_CAI_DAT.md` (máy mới), `docs/HUONG_DAN_SU_DUNG.md`, `docs/KIEN_TRUC.md` – sửa code thì sửa tài liệu tương ứng.

## Cấu trúc
```
scripts/   00_check.sh, 01_download.sh, 02–13 *.py, 11a_landing.py, 11b_hdfs_yarn_cli.sh, common.py, run_all.sh, run_spark.sh
docs/      hướng dẫn cài đặt, hướng dẫn sử dụng, tài liệu kiến trúc
data/raw/        dữ liệu tải về + location-tyt.xlsx     data/reference/  danh sách 56 trạm, số liệu NQ 1251
data/interim/    parquet/geojson trung gian (pandas)    data/landing/    CSV thô cho Spark (vùng thu nhận)
data/spark/      curated/ quarantine/ gold/ (Parquet của Spark khi không dùng HDFS)
outputs/tables/  mọi bảng kết quả (CSV)                 outputs/figures/  h1–h5, h7–h9 (PNG; h6 chỉ có khi có xã giảm tiếp cận; h8 bản đồ hành chính trước 2025, h9 trước/sau đề xuất)
outputs/run_manifest.csv  file đầu ra + số dòng + SHA-256 + script tạo ra     logs/  nhật ký từng lần chạy
docker/ notebooks/        cụm HDFS + YARN + JupyterLab của học phần và notebook bước 11
_luu_tru_pham_vi_2015_2030/  kết quả cũ (năm cơ sở 2025, có dự báo 2030) – không dùng cho báo cáo
```

## Giả định
| Mã | Nội dung | Ở đâu |
|---|---|---|
| A1 | Ngưỡng KPI chính 3 km đường chim bay là **giả định do nhóm tự nghiên cứu chốt** (người dùng xác nhận 17/11/2026), không dựa trên văn bản quy định nào → luôn kèm độ nhạy 2 km, 5 km, theo mạng đường và theo 15 phút xe máy | common.py, 06, 11 |
| A2 | Dân số = WorldPop R2025A 100 m constrained (ước tính mô hình); năm hiện trạng 2024 (năm cuối trước sắp xếp 01/01/2025); ranh giới xã: xem A12 | 02, 03 |
| A3 | Tỷ lệ 60+ của ô 100 m = tỷ lệ 60+ của ô 1 km chứa nó | 03 |
| A4 | Năm không có file 100 m: hạ cấp từ 1 km, giữ phân bố trong ô 1 km của năm cơ sở. Hiện cả 10 năm 2015–2024 đều có file 100 m nên A4 chỉ còn được kiểm định, không dùng cho KPI | 03 |
| A5 | Trạm không có trên OSM: vị trí ước lượng = tâm dân cư xã; KPI luôn kèm bản "chỉ trạm OSM" | 04, 06 |
| A6 | Mạng đường OSM hiện tại, hai chiều, tốc độ xe máy theo loại đường, đoạn ra đường đi bộ 5 km/h. Mạng đường **gồm cả cao tốc** (142,6 km, `qa_network.csv`) dù xe máy không được đi – người dùng chốt giữ nguyên 17/11/2026; phải nêu là hạn chế trong báo cáo, mức ảnh hưởng chưa đo | 05 |
| A7 | "Xã miền núi" theo địa hình: xã có độ dốc TB theo dân > 5° hoặc độ cao TB theo dân > 50 m (chưa đối chiếu danh sách chính thức) | 06 |
| A8 | Chẩn đoán vùng trắng: địa hình (dốc > 8°, cao > 150 m, hệ số vòng > 1,5); phân tán (< 10 người/ô hoặc vùng < 300 người) | 07 |
| A9 | Dự báo 2030 (bật lại 17/11/2026): tổng dân từng xã = dự báo của mô hình thắng backtest trên chuỗi 2015–2024; trong xã, dân 2030 phân bố theo nhịp tăng ô 1 km của số chiếu WorldPop 2030; khoảng 80% lấy từ sai số backtest ở tầm 5 năm trong khi dự báo xa 6 năm nên có thể hơi hẹp; vị trí 56 trạm giữ nguyên | 08 |
| A10 | Ứng viên vị trí mới = ô 100 m đông dân nhất mỗi khối 300 × 300 m; bán kính phủ 3 km | 09 |
| A11 | Lệch WorldPop so với số liệu NQ 1251 chấp nhận ±20% (mốc số liệu NQ chưa xác định năm) | 10 |
| A12 | Ranh giới 56 xã thực tế lấy từ GADM 4.1 (thay cho OSM trong A2): OSM mốc 31/12/2024 đã vẽ theo bản đồ sau sáp nhập nên chỉ khớp 45/56 xã, `02_boundaries.py` tự chọn nguồn khớp nhiều hơn (xem `qa_boundaries.csv`, cột `nguon_dung`) | 02 |
| A13 | Toạ độ trạm ưu tiên file người dùng nhập tay `data/raw/location-tyt.xlsx`. Nguồn theo người dùng (17/11/2026): toạ độ được kiểm tra theo Cổng dữ liệu Đà Nẵng (congdulieu.vn), dữ liệu cập nhật 12/12/2024 – mốc này nằm trước ngày sắp xếp 01/01/2025 nên khớp phạm vi 56 trạm. Claude chưa tự đối chiếu với cổng dữ liệu; chưa kiểm chứng thực địa; giấy phép dùng lại dữ liệu của cổng chưa được xác nhận. Toạ độ ngoài hộp bao bị loại và quay về OSM / ước lượng A5 (xem `qa_stations.csv`, `qa_stations_nhap_tay_bi_loai.csv`) Người dùng tiếp tục sửa file ngày 17/11/2026 (lần gần nhất 09:59: Hòa Bắc, Hòa Ninh, Hòa Sơn); bản đang dùng đặt trạm Hòa Ninh trở lại trong ranh giới xã. Mỗi lần file này đổi phải chạy lại từ bước 04 và chạy lại Spark | 04 |
| A14 | KPI theo năm 2015–2024 giữ nguyên vị trí 56 trạm như hiện nay (không có dữ liệu lịch sử vị trí trạm); thay đổi qua các năm chỉ phản ánh thay đổi phân bố dân cư | 11 |
