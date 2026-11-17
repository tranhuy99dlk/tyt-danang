# Hướng dẫn sử dụng

Dành cho người đã cài xong dự án (xem [HUONG_DAN_CAI_DAT.md](HUONG_DAN_CAI_DAT.md)) và cần chạy lại, đọc kết quả hoặc thay đổi đầu vào.
Muốn hiểu vì sao hệ thống được thiết kế như vậy: [KIEN_TRUC.md](KIEN_TRUC.md).

Mọi lệnh chạy từ thư mục gốc của dự án (thư mục chứa `README.md`).

## 1. Chạy

| Muốn làm | Lệnh | Thời gian trên máy thử |
|---|---|---|
| Kiểm tra máy đủ điều kiện | `bash scripts/00_check.sh` | vài giây |
| Tải dữ liệu nguồn (chạy lại chỉ tải file còn thiếu) | `bash scripts/01_download.sh` | tuỳ mạng |
| Chạy toàn bộ phần pandas và tạo CSV thô cho Spark | `source .venv/bin/activate && bash scripts/run_all.sh` | khoảng 2 phút |
| Chạy một bước lẻ | `cd scripts && ../.venv/bin/python 06_kpi.py` | – |
| Chạy Spark, không cần cụm HDFS | `bash scripts/run_spark.sh` | 4–6 phút |
| Chạy Spark trên HDFS | xem mục 5 | 4–6 phút |

Thứ tự bắt buộc: `01_download.sh` → `run_all.sh` → Spark. `run_all.sh` dừng ngay ở bước đầu tiên bị lỗi.

Các bước phụ thuộc nhau theo chuỗi 02 → 03 → 04 → 05 → 06 → (07, 09 → 13) → 10 → 12 → 11a → 11. Bước 13 đọc `greedy_sites.csv`
của bước 09 nên phải chạy sau 09. Sửa đầu vào của một bước thì phải chạy lại
từ bước đó tới hết, rồi chạy lại Spark; nếu không, bảng đối soát sẽ báo `LỆCH`.

## 2. Câu hỏi nào, xem file nào

Tất cả nằm trong `outputs/tables/` trừ khi ghi khác.

| Câu hỏi | File | Cột/dòng cần đọc |
|---|---|---|
| Bao nhiêu % dân trong 3 km tới trạm? | `kpi_tong_hop_spark.csv` | dòng loại "KPI chính" |
| Theo đường đi thì sao, mất bao lâu? | `kpi_tong_hop_spark.csv` | "theo mạng đường", "15 phút xe máy", "Hệ số vòng" |
| Dân bình quân mỗi trạm, khoảng cách trung vị, dân ở xa trên 5 km, người 60+ | `kpi_tong_hop_spark.csv` | dòng loại "KPI phụ" |
| Phường và xã miền núi chênh nhau bao nhiêu? | `kpi_theo_nhom_spark.csv` | `ty_le_trong_3km_pct` theo `nhom` |
| Xã nào yếu nhất? | `kpi_theo_xa_spark.csv` | sắp theo `ty_le_trong_3km_mang_duong_pct` |
| Thay đổi qua các năm 2015–2024 | `kpi_theo_nam_spark.csv`, `kpi_theo_xa_nam_spark.csv` | mỗi dòng một năm |
| Mỗi trạm phục vụ bao nhiêu dân? | `tram_vung_phuc_vu_spark.csv` | `dan_phuc_vu` |
| Trạm nằm cạnh đường nào, đi lại có thuận tiện không? | `tram_giao_thong.csv` | `duong_gan_nhat`, `cap_duong_gan_nhat`, `he_so_vong_trung_vi` |
| Dân ở xa trạm tập trung ở đâu, vì sao? | `white_zones.csv`, hình `h1`, `h2` | `nguyen_nhan` |
| Sáp nhập 56 → 47 trạm ảnh hưởng thế nào? | `scenario_nq1251.csv` | 3 kịch bản |
| Nên đặt thêm điểm ở đâu? | `greedy_sites.csv`, `greedy_before_after.csv`, `diem_de_xuat_giao_thong.csv`, hình `h7` | `thu_tu`, `dan_moi_duoc_phu` |
| Lưới dân cư từng ô 100 m | `dan_cu_luoi_100m.csv`, `tiep_can_luoi_100m.csv` | giải thích cột ở `giai_thich_csv_luoi.csv` |
| Trường này nghĩa là gì, nguồn nào, nhạy cảm không? | `tu_dien_du_lieu.csv` | – |

Các bảng `kpi_overview.csv`, `kpi_by_*.csv`, `station_catchment.csv` là bản pandas của cùng những chỉ số trên; hai bản phải khớp nhau (mục 3).

## 3. Đọc kết quả kiểm tra

Sau mỗi lần chạy, đọc ba bảng này trước khi dùng bất kỳ con số nào.

| Bảng | Đọc cột | Bình thường | Phải xử lý khi |
|---|---|---|---|
| `qa_summary.csv` | `dat` | Phần lớn `ĐẠT`; một số `CẦN XEM` đã biết (xem dưới) | Xuất hiện `CẦN XEM` mới |
| `doi_soat_spark.csv` | `ket_qua` | Mọi dòng `ĐẠT` | Có dòng `LỆCH` |
| `spark_so_dong_theo_nguon.csv` | `so_dong_cach_ly` | Pixel dân số và lưới 1 km có tỷ lệ cách ly cao vì NoData và vùng ngoài 56 xã | `stations_raw` có ít hơn 56 dòng hợp lệ |

Các mục `CẦN XEM` đang tồn tại và đã được ghi nhận là hạn chế của dữ liệu:

- 8 trạm nằm ngoài ranh giới xã của chính nó từ 2 m đến 104 m (cột `kc_ngoai_ranh_gioi_m` trong `data/interim/stations_56.csv`; ranh giới GADM thô).
- Trạm cách tâm dân cư của xã trên 3 km (Hòa Bắc, Hòa Ninh).
- Phường có ranh giới OSM khác GADM (OSM đã vẽ theo bản đồ sau sáp nhập).
- WorldPop lệch khoảng 21% so với số liệu NQ 1251.

Muốn xem dòng nào bị cách ly: `spark_quarantine_tong_hop.csv` (đếm theo lý do), `spark_quarantine_mau.csv` (tối đa 10 dòng mẫu mỗi lý do).
Bản đầy đủ là Parquet ở `data/spark/quarantine/dong_loi` hoặc `/mis5007/quarantine/dong_loi` trên HDFS. Ý nghĩa mã lý do: [KIEN_TRUC.md](KIEN_TRUC.md#6-pipeline-spark-bước-11).

## 4. Việc thường làm

### Sửa hoặc bổ sung toạ độ trạm

1. Mở `data/raw/location-tyt.xlsx`, sửa ô toạ độ ở cột thứ ba theo dạng `vĩ độ, kinh độ` (ví dụ `16.0500, 108.1797`). Giữ nguyên tên xã ở cột thứ hai.
2. Chạy lại từ bước 04:
   ```bash
   source .venv/bin/activate && cd scripts
   for s in 04_stations 05_network 06_kpi 07_diagnose 09_greedy 13_giao_thong 10_quality 12_export_csv 11a_landing; do python $s.py || break; done
   cd .. && bash scripts/run_spark.sh
   ```
3. Đọc `outputs/tables/qa_stations.csv` và `qa_stations_nhap_tay_bi_loai.csv`. Toạ độ nằm ngoài Đà Nẵng cũ bị loại, trạm đó quay về vị trí ước lượng.

### Đổi ngưỡng khoảng cách, năm phân tích, tốc độ

Tất cả nằm trong `scripts/common.py`:

| Tham số | Hiện tại | Ý nghĩa |
|---|---|---|
| `MAIN_KM` | 3.0 | Ngưỡng KPI chính (giả định A1) |
| `THRESH_KM` | 2, 3, 5 | Các ngưỡng độ nhạy |
| `WHITE_KM` | 5.0 | Ngưỡng "vùng trắng" |
| `YEARS` | 2015–2024 | Các năm phân tích |
| `BASE_YEAR` | 2024 | Năm hiện trạng |
| `SPEED_KMH`, `WALK_KMH` | theo loại đường; 5 | Tốc độ xe máy và đi bộ (A6) |

Sau khi đổi: chạy lại `run_all.sh` rồi Spark. Đổi `YEARS` thì phải sửa dòng `YRS=` trong `scripts/01_download.sh` cho khớp và tải thêm file.
Đổi `BASE_YEAR` làm đổi tên cột (ví dụ `pop_2024`). Mọi thay đổi giả định phải được ghi vào bảng giả định trong [CLAUDE.md](../CLAUDE.md).

### Thêm một quy tắc kiểm tra trong Spark

Trong `scripts/11_spark_pipeline.py`, tìm lời gọi `split(...)` của nguồn cần thêm và chèn một cặp `("MA_LY_DO", điều_kiện)` vào danh sách.
Điều kiện phải trả về `true` cho dòng **vi phạm**. Chạy lại Spark; số dòng cách ly theo mã mới xuất hiện trong `spark_quarantine_tong_hop.csv`.

### Sửa code và giữ khả năng truy vết

Ghi file kết quả bằng `save_csv(...)` hoặc `register(...)` trong `common.py` để file được đưa vào `outputs/run_manifest.csv` kèm SHA-256.

### Tuỳ chỉnh bằng biến môi trường

Các tham số phân tích nằm trong `common.py` (bảng ở trên). Các biến dưới đây chỉ đổi cách chạy, không đổi kết quả.

| Biến | Script đọc | Mặc định | Tác dụng |
|---|---|---|---|
| `PAR` | `01_download.sh` | `4` | Số luồng tải song song |
| `FULL` | `01_download.sh` | `0` | `FULL=1` tải thêm 7 file nhóm tuổi 60+ độ phân giải 100 m năm 2024; hiện chưa có script nào đọc các file này |
| `PYTHON` | `run_all.sh` | `python3` | Trình thông dịch Python dùng để chạy các bước |
| `SPARK_IMAGE` | `run_spark.sh` | `jupyter/pyspark-notebook:latest` | Image Docker chạy bước 11 |
| `HDFS_DIR`, `NN`, `RM` | `11b_hdfs_yarn_cli.sh` | `/mis5007`, `mis5007-namenode`, `mis5007-resourcemanager` | Thư mục gốc trên HDFS, tên container NameNode và ResourceManager |
| `HDFS_URI` | `11_spark_pipeline.py` | không đặt | Đặt thì đọc và ghi các vùng trên HDFS; không đặt thì dùng `data/landing/` và `data/spark/` |
| `SPARK_DRIVER_MEMORY` | `11_spark_pipeline.py` | `3g` | Bộ nhớ cấp cho driver Spark |
| `SPARK_SHUFFLE_PARTITIONS` | `11_spark_pipeline.py` | `16` | Số phân vùng khi shuffle |
| `SPARK_MASTER` | `11_spark_pipeline.py` | `local[*]` | Địa chỉ master của Spark |
| `SPARK_UI` | `11_spark_pipeline.py` | `false` | `true` bật Spark UI trong lúc pipeline chạy |
| `HDFS_REPLICATION` | `11_spark_pipeline.py` | `1` | Hệ số nhân bản khi ghi lên HDFS |

Các biến của `11_spark_pipeline.py` được đọc **bên trong container**, nên đặt chúng trước `bash scripts/run_spark.sh` không có tác dụng:
script này không chuyển biến vào container. Cách đặt đúng:

- Chạy trên cụm: thêm `-e TÊN=giá_trị` vào lệnh `docker exec` ở mục 5, cạnh `-e HDFS_URI=...`.
- Chạy bằng `run_spark.sh`: thêm `-e TÊN=giá_trị` vào dòng `docker run` trong `scripts/run_spark.sh`.

Đổi `HDFS_DIR` thì phải đổi phần đường dẫn trong `HDFS_URI` cho khớp.

## 5. Chạy trên cụm HDFS + YARN + JupyterLab

```bash
docker compose -f docker/docker-compose.yml up -d        # dựng 5 container (lần đầu phải tải image)
bash scripts/11b_hdfs_yarn_cli.sh truoc                   # tạo các vùng trên HDFS, đưa 6 file CSV thô lên
docker exec -w /home/jovyan/work/mis5007_tram_y_te -e HDFS_URI=hdfs://namenode:9000/mis5007 \
  mis5007-spark-notebook python scripts/11_spark_pipeline.py
bash scripts/11b_hdfs_yarn_cli.sh sau                     # liệt kê kết quả trên HDFS, fsck, trạng thái YARN
```

Hoặc chạy bước 11 trong JupyterLab: mở http://localhost:8888, nhập mật khẩu ghi ở dòng `JUPYTER_TOKEN` trong `docker/docker-compose.yml`,
mở `work/mis5007_tram_y_te/notebooks/buoc11_pyspark_hdfs.ipynb` và chạy lần lượt các ô.

| Giao diện | Địa chỉ | Xem gì |
|---|---|---|
| HDFS NameNode | http://localhost:9871 | Utilities → Browse the file system → `/mis5007` |
| YARN | http://localhost:8088 | Nodes, Applications |
| JupyterLab | http://localhost:8888 | Notebook bước 11 |
| Spark UI | http://localhost:4041 | Chỉ mở được khi có job Spark đang chạy trong notebook |

Minh chứng cho báo cáo: ảnh chụp ba giao diện đầu, notebook đã chạy, và hai file `logs/11b_hdfs_yarn_cli_truoc_*.log`, `logs/11b_hdfs_yarn_cli_sau_*.log`.

Quản lý cụm:

| Lệnh | Tác dụng |
|---|---|
| `docker compose -f docker/docker-compose.yml ps` | Xem trạng thái 5 container |
| `docker compose -f docker/docker-compose.yml stop` | Dừng, giữ dữ liệu HDFS |
| `docker compose -f docker/docker-compose.yml start` | Chạy lại |
| `docker compose -f docker/docker-compose.yml down -v` | Gỡ hẳn và **xoá toàn bộ dữ liệu HDFS** của cụm |

Spark chạy `local[*]` trong container JupyterLab và đọc/ghi HDFS. YARN chỉ được dùng qua CLI; danh sách ứng dụng YARN luôn rỗng.

## 6. Xử lý sự cố

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `01_download.sh` báo `FAIL` | Mạng chập chờn hoặc máy chủ Overpass bận | Chạy lại lệnh; file đã tải được bỏ qua |
| Một bước Python báo `No such file` ở `data/interim/` | Chưa chạy bước trước nó | Chạy `run_all.sh` từ đầu |
| `AssertionError: Thieu file 100 m nam co so` | Chưa tải đủ raster | Chạy `bash scripts/01_download.sh` |
| `ModuleNotFoundError` khi chạy bước Python | Chưa kích hoạt môi trường ảo | `source .venv/bin/activate` |
| `doi_soat_spark.csv` có dòng `LỆCH` | Chạy Spark trên CSV thô cũ hơn kết quả pandas | Chạy lại `run_all.sh` rồi Spark; trên HDFS thêm `11b_hdfs_yarn_cli.sh truoc` |
| `docker: Cannot connect to the Docker daemon` | Docker Desktop chưa mở | Mở Docker Desktop, chờ biểu tượng báo đang chạy |
| `port is already allocated` khi `up -d` | Cổng bị chương trình khác dùng | Đổi số bên trái dấu `:` ở mục `ports` trong `docker/docker-compose.yml` |
| `container name ... is already in use` | Còn container cũ cùng tên | `docker compose -f docker/docker-compose.yml down` rồi `up -d` |
| Spark báo `Java heap space` | Docker được cấp ít RAM, hoặc driver Spark thiếu bộ nhớ | Tăng RAM trong Docker Desktop; hoặc tăng `SPARK_DRIVER_MEMORY` (ví dụ `4g`) theo cách ghi ở mục 4, phần "Tuỳ chỉnh bằng biến môi trường" |
| `hdfs dfsadmin -report` có "Under replicated blocks" | File ghi với hệ số nhân bản 3 trên cụm 1 DataNode | `bash scripts/11b_hdfs_yarn_cli.sh sau` (tự đưa về 1) |
| `Name node is in safe mode` | NameNode vừa khởi động | Chờ khoảng một phút; `11b_hdfs_yarn_cli.sh truoc` tự chờ |
| Bước 11 trên HDFS báo `Path does not exist: .../landing/...` | Chưa đưa CSV thô lên HDFS | `bash scripts/11b_hdfs_yarn_cli.sh truoc` |

Không tìm ra nguyên nhân: mở file log mới nhất của bước bị lỗi trong `logs/`.

## 7. Khi đưa số liệu vào báo cáo

- Mỗi con số phải dẫn về một file trong `outputs/tables/` hoặc `logs/`. Chưa có file thì ghi "chưa có số".
- WorldPop là ước tính mô hình; ghi rõ điều này cạnh mọi con số dân.
- Ngưỡng 3 km là giả định của nhóm (A1); luôn kèm kết quả 2 km, 5 km, theo đường đi và theo 15 phút.
- Ghi giấy phép: WorldPop CC BY 4.0; OpenStreetMap ODbL 1.0 ("© OpenStreetMap contributors"); GADM chỉ dùng học thuật, không phân phối lại; AWS Terrain Tiles ghi nguồn.
- Kết quả trong `_luu_tru_pham_vi_2015_2030/` thuộc phạm vi cũ (năm cơ sở 2025, có dự báo); không dùng.
