# Tiếp cận trạm y tế xã/phường – Đà Nẵng cũ

Dự án đo mức độ người dân tiếp cận được trạm y tế ở 56 xã/phường Đà Nẵng theo bản đồ hành chính **trước** đợt sắp xếp 01/01/2025,
trên 10 mốc năm 2015–2024 (năm hiện trạng 2024). Đồ án học phần MIS5007 – Dữ liệu lớn và Ứng dụng.

Dự án trả lời:

- Bao nhiêu phần trăm dân sống trong 3 km tới trạm gần nhất, theo đường chim bay và theo đường đi?
- Dân bình quân mỗi trạm, khoảng cách trung vị, số dân ở xa trên 5 km, người 60+ được tiếp cận, chênh lệch phường – xã miền núi.
- Dữ liệu đầu vào tin được tới đâu (thiếu, trùng, sai toạ độ, lệch tổng dân số).
- Nên đặt thêm điểm ở đâu.

Cách làm: pandas xử lý phần bản đồ và mạng đường trên máy; một pipeline Spark đọc dữ liệu thô với schema, tách dòng lỗi vào vùng quarantine,
tính lại toàn bộ KPI và đối soát với bản pandas. Spark chạy được trong một container đơn lẻ hoặc trên cụm HDFS + YARN + JupyterLab bằng Docker.

## Bắt đầu nhanh

Cần macOS, Docker Desktop đang chạy, `uv` và khoảng 20 GB ổ đĩa trống. Kho GitHub đã kèm ba file nhóm tự chuẩn bị;
cách lấy dự án khi không dùng GitHub: [hướng dẫn cài đặt](docs/HUONG_DAN_CAI_DAT.md#3-lấy-thư-mục-dự-án).

```bash
git clone https://github.com/tranhuy99dlk/tyt-danang.git mis5007_tram_y_te && cd mis5007_tram_y_te
bash scripts/00_check.sh                                              # máy đủ điều kiện chưa
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.lock.txt
bash scripts/01_download.sh                                           # tải khoảng 1 GB dữ liệu nguồn mở
source .venv/bin/activate && bash scripts/run_all.sh                  # phần pandas + CSV thô cho Spark
bash scripts/run_spark.sh                                             # pipeline Spark
```

Chạy đúng thì lệnh cuối in ra dòng `Doi soat: 25 phep, 0 LECH.` và kết quả nằm trong `outputs/tables/`.
Trên máy thử, hai lệnh cuối mất 2–3 phút và 4–9 phút (chưa tính thời gian tải dữ liệu và image Docker ở lần đầu).
Mở `outputs/tables/kpi_tong_hop_spark.csv` để xem KPI chính.

## Tài liệu

| Bạn là | Đọc |
|---|---|
| Người mới, cần cài và chạy trên một máy chưa có gì | [docs/HUONG_DAN_CAI_DAT.md](docs/HUONG_DAN_CAI_DAT.md) |
| Người dùng, cần chạy lại, đọc kết quả, sửa toạ độ trạm, đổi tham số, xử lý lỗi | [docs/HUONG_DAN_SU_DUNG.md](docs/HUONG_DAN_SU_DUNG.md) |
| Người cần hiểu thiết kế: thành phần, luồng dữ liệu, pipeline Spark, cụm Docker, các đánh đổi | [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md) |
| Người sửa code hoặc viết báo cáo: quy tắc làm việc và bảng giả định A1–A14 | [CLAUDE.md](CLAUDE.md) |

## Cấu trúc thư mục

```
scripts/      00_check.sh, 01_download.sh, 02–13 *.py, 11a_landing.py, 11b_hdfs_yarn_cli.sh,
              run_all.sh, run_spark.sh, common.py (mọi tham số)
docs/         hướng dẫn cài đặt, hướng dẫn sử dụng, tài liệu kiến trúc
docker/       docker-compose.yml – cụm HDFS + YARN + JupyterLab
notebooks/    buoc11_pyspark_hdfs.ipynb – PySpark nối HDFS trong JupyterLab
data/raw/         dữ liệu nguồn tải về + location-tyt.xlsx (toạ độ trạm nhập tay)
data/reference/   danh sách 56 trạm, số liệu dân số NQ 1251
data/interim/     file trung gian của pandas
data/landing/     CSV thô cho Spark
data/spark/       curated / quarantine / gold khi không dùng HDFS
outputs/tables/   mọi bảng kết quả (CSV)        outputs/figures/   hình h1–h5, h7–h9
outputs/run_manifest.csv   mỗi file kết quả: script tạo ra, số dòng, SHA-256
logs/             nhật ký từng lần chạy
```

`data/interim/`, `data/landing/`, `data/spark/`, `outputs/` và `logs/` đều tạo lại được bằng cách chạy lại pipeline.
`data/raw/location-tyt.xlsx` và `data/reference/` thì không; đừng xoá.

## Nguồn dữ liệu và giấy phép

| Nguồn | Dùng cho | Giấy phép |
|---|---|---|
| WorldPop Global2 R2025A | Dân số lưới 100 m, 1 km, nhóm tuổi | CC BY 4.0 – ghi nguồn WorldPop, University of Southampton |
| OpenStreetMap (Overpass API) | Mạng đường, ranh giới đối chiếu, cơ sở y tế | ODbL 1.0 – ghi "© OpenStreetMap contributors"; dữ liệu phái sinh chia sẻ lại phải cùng ODbL |
| GADM 4.1 | Ranh giới 56 xã | Chỉ dùng học thuật/phi thương mại, không phân phối lại |
| AWS Terrain Tiles (Mapzen) | Độ cao, độ dốc | Ghi nguồn |
| Nhóm thực hiện | Danh sách và toạ độ 56 trạm (nhập tay, kiểm tra theo Cổng dữ liệu Đà Nẵng (congdulieu.vn), dữ liệu cập nhật 12/12/2024) | Ghi nguồn cổng dữ liệu; điều kiện dùng lại chưa được xác nhận |
| Báo chí dẫn NQ 1251/NQ-UBTVQH15 | Số liệu dân số đối chiếu | Xem cột nguồn trong `data/reference/` |

## Giới hạn cần biết trước khi dùng số liệu

- Dân số là ước tính mô hình của WorldPop, không phải số đếm.
- Ngưỡng 3 km là giả định của nhóm; luôn đọc kèm kết quả 2 km, 5 km, theo đường đi và theo 15 phút.
- Toạ độ trạm do nhóm nhập tay, kiểm tra theo Cổng dữ liệu Đà Nẵng (congdulieu.vn), dữ liệu cập nhật 12/12/2024; chưa kiểm chứng thực địa.
- Số liệu sau năm 2024 (`forecast_*.csv`, `access_2030_*.csv`, hình h5) là **dự báo** từ chuỗi 2015–2024, không phải số đo; đọc kèm khoảng 80%.

Chi tiết: [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md#10-hạn-chế-đã-biết).
