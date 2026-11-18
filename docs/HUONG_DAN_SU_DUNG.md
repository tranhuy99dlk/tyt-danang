# Hướng dẫn sử dụng

Dành cho người đã cài xong dự án (xem [HUONG_DAN_CAI_DAT.md](HUONG_DAN_CAI_DAT.md)) và cần chạy lại, đọc kết quả hoặc thay đổi đầu vào.
Muốn hiểu vì sao hệ thống được thiết kế như vậy: [KIEN_TRUC.md](KIEN_TRUC.md).

Mọi lệnh chạy từ thư mục gốc của dự án (thư mục chứa `README.md`).

## 1. Chạy

| Muốn làm | Lệnh | Thời gian trên máy thử |
|---|---|---|
| Kiểm tra máy đủ điều kiện | `bash scripts/00_check.sh` | vài giây |
| Tải dữ liệu nguồn (chạy lại chỉ tải file còn thiếu) | `bash scripts/01_download.sh` | tuỳ mạng |
| Chạy toàn bộ phần pandas và tạo CSV thô cho Spark | `source .venv/bin/activate && bash scripts/run_all.sh` | 2–3 phút |
| Chạy một bước lẻ | `cd scripts && ../.venv/bin/python 06_kpi.py` | – |
| Chạy Spark rồi bước 15, không cần cụm HDFS | `bash scripts/run_spark.sh` | 4–9 phút |
| Chạy Spark trên HDFS rồi bước 15 | xem mục 5 | 4–9 phút |
| Chạy riêng bước 15 (TYT và CSKCB) | `.venv/bin/python scripts/15_tuyen_tren.py` | vài giây |

Thứ tự bắt buộc: `01_download.sh` → `run_all.sh` → Spark → bước 15. `run_all.sh` dừng ngay ở bước đầu tiên bị lỗi.
`run_spark.sh` tự chạy bước 15 sau khi Spark xong; khi chạy Spark trên cụm HDFS thì phải tự chạy bước 15 (mục 5).

Các bước phụ thuộc nhau theo chuỗi 02 → 03 → 04 → 05 → 06 → (07, 08, 09 → 13, 14) → 10 → 12 → 11a → 11 → 15.
Bước 13 và 14 đọc `greedy_sites.csv` của bước 09 nên phải chạy sau 09. Bước 15 đọc hai bảng của Spark
(`tram_vung_phuc_vu_spark.csv`, `kpi_theo_xa_spark.csv`) nên phải chạy sau bước 11. Sửa đầu vào của một bước thì phải chạy lại
từ bước đó tới hết, rồi chạy lại Spark và bước 15; nếu không, bảng đối soát sẽ báo `LỆCH` hoặc bước 15 dùng số cũ.

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
| Đà Nẵng trước 01/01/2025 nằm trong khung toạ độ nào, gồm những xã/phường nào? | `pham_vi_da_nang_cu.csv`, `xa_phuong_56_toa_do.csv`, hình `h8` | khung kinh độ, vĩ độ; số trên bản đồ |
| Tiếp cận thay đổi thế nào trước và sau khi thêm điểm đề xuất? | `ban_do_truoc_sau_de_xuat.csv`, hình `h9` | `ty_le_truoc_pct`, `ty_le_sau_pct` |
| Sáp nhập 56 → 47 trạm ảnh hưởng thế nào? | `scenario_nq1251.csv` | 3 kịch bản |
| Đến 2030 dân và người 60+ từng xã dự báo là bao nhiêu? | `forecast_commune_2030.csv`, `forecast_total_2030.csv`, hình `h5` | `dan_2030`, `dan_2030_lo80`, `dan_2030_hi80` |
| Mô hình dự báo nào được chọn, sai số bao nhiêu? | `forecast_backtest.csv`, `forecast_model_choice.csv` | `MAPE_pct` |
| Đến 2030 xã nào giảm tiếp cận? | `access_2030_change.csv`, `access_2030_total.csv` | `thay_doi_pp`, `giam_tiep_can` |
| Nên đặt thêm điểm ở đâu? | `greedy_sites.csv`, `greedy_before_after.csv`, `diem_de_xuat_giao_thong.csv`, hình `h7` | `thu_tu`, `dan_moi_duoc_phu` |
| Nên tăng hay giảm số trạm? | `greedy_before_after.csv` (thêm điểm) và `ung_vien_gop_tram.csv` (rà soát gộp) | `ty_le_3km` theo `K`; `trang_thai_nq1251` |
| Trạm có đặt đúng chỗ không? | `qa_summary.csv` nhóm "Sai toạ độ"; `data/interim/stations_56.csv` | `kc_tram_tam_dan_cu_m`, `kc_ngoai_ranh_gioi_m` |
| Trạm nào gánh quá nhiều hay quá ít dân? | `tram_vung_phuc_vu_spark.csv` | `dan_phuc_vu`, `dan60_phuc_vu` |
| Quanh mỗi TYT có bao nhiêu CSKCB, cấp nào, gần nhất bao xa? | `tyt_cskcb.csv`, `tyt_cskcb_theo_nhom.csv` | `kc_cskcb_gan_nhat_m`, `cskcb_2km`, `cap_*_2km` |
| Danh sách 30 CSKCB đã làm sạch | `cskcb_danang.csv` | `tuyen`, `hang`, `cap` |
| Quy tắc gộp trạm của nhóm khớp NQ 1251 tới đâu? | `doi_soat_nq1251_quy_tac.csv`, `doi_soat_nq1251_theo_xa.csv` | `dung_TP`, `bao_dong_gia_FP`, `bo_sot_FN`; `phan_loai_nq1251` |
| TYT phải làm những gói dịch vụ nào, cơ sở khác thay được không? | `data/reference/tt30_2024_goi_dich_vu.csv` | `benh_vien_tuyen_tren_thay_the` (đánh giá của nhóm) |
| 8 đơn vị gộp theo NQ 1251 | `data/reference/nq1251_sap_xep.csv` | `cac_xa_nhap_vao`, `so_xa_nhap` |
| Lưới dân cư từng ô 100 m | `dan_cu_luoi_100m.csv`, `tiep_can_luoi_100m.csv` | giải thích cột ở `giai_thich_csv_luoi.csv` |
| Trường này nghĩa là gì, nguồn nào, nhạy cảm không? | `tu_dien_du_lieu.csv` | – |

Các bảng `kpi_overview.csv`, `kpi_by_*.csv`, `station_catchment.csv` là bản pandas của cùng những chỉ số trên; hai bản phải khớp nhau (mục 3).

## 3. Đọc kết quả kiểm tra

Sau mỗi lần chạy, đọc ba bảng này trước khi dùng bất kỳ con số nào.

| Bảng | Đọc cột | Bình thường | Phải xử lý khi |
|---|---|---|---|
| `qa_summary.csv` | `dat` | Phần lớn `ĐẠT`; một số `CẦN XEM` đã biết (xem dưới) | Xuất hiện `CẦN XEM` mới |
| `doi_soat_spark.csv` | `ket_qua` | Mọi dòng `ĐẠT` | Có dòng `LỆCH` |
| `spark_so_dong_theo_nguon.csv` | `so_dong_loi_du_lieu`, `so_dong_ngoai_pham_vi` | Lỗi dữ liệu chỉ có 9 dòng ở `stations_raw` (8 dòng tên quận, 1 dòng tiêu đề của file Excel); ngoài phạm vi rất nhiều ở pixel dân số, lưới 1 km, ô lưới (NoData của WorldPop, ngoài 56 xã) – bình thường | Lỗi dữ liệu xuất hiện ở nguồn khác, hoặc `stations_raw` có ít hơn 56 dòng hợp lệ |
| log mới nhất `logs/15_tuyen_tren_*.log` | dòng `Doi chieu NQ 1251` | `... code va file tham chieu KHOP`; `CSKCB doc duoc: 30 co so` | Có chữ `LECH` (danh sách xã gộp trong code khác `nq1251_sap_xep.csv`) hoặc `CAN XEM` (CSKCB không đọc được toạ độ) |

Các mục `CẦN XEM` đang tồn tại và đã được ghi nhận là hạn chế của dữ liệu:

- 8 trạm nằm ngoài ranh giới xã của chính nó, xa nhất 104 m (cột `kc_ngoai_ranh_gioi_m` trong `data/interim/stations_56.csv`). Mức lệch này nhiều khả năng do ranh giới GADM thô; không dùng làm bằng chứng trạm đặt sai chỗ.
- Trạm cách tâm dân cư của xã trên 3 km: chỉ còn Trạm Y tế xã Hòa Ninh (3.150 m). Đây là bằng chứng chính cho nhận định "đặt sai chỗ".
- Phường có ranh giới OSM khác GADM (OSM đã vẽ theo bản đồ sau sáp nhập).
- WorldPop lệch khoảng 21% so với số liệu NQ 1251.

Dòng không vào bảng sạch chia hai loại (cột `loai`):

| Loại | Vùng lưu trữ | Ý nghĩa | Lần chạy 18/11/2026 |
|---|---|---|---|
| `LOI_DU_LIEU` | `quarantine/dong_loi` | Lỗi thật: sai kiểu, thiếu hoặc trùng khoá, toạ độ sai… | 9 dòng, đều là dòng tiêu đề/tên quận trong `location-tyt.xlsx` |
| `NGOAI_PHAM_VI` | `excluded/ngoai_pham_vi` | Không phải lỗi: mã NoData −99999 WorldPop dùng cho ô không có dân, pixel ngoài 56 xã, năm ngoài phạm vi | 2.936.766 dòng |

Xem số dòng theo loại và lý do ở `spark_quarantine_tong_hop.csv`, dòng mẫu ở `spark_quarantine_mau.csv` (tối đa 10 dòng mỗi lý do).
Bản đầy đủ là Parquet ở `data/spark/quarantine/`, `data/spark/excluded/` hoặc `/mis5007/quarantine`, `/mis5007/excluded` trên HDFS.
Ý nghĩa và loại của từng mã lý do: [KIEN_TRUC.md](KIEN_TRUC.md#6-pipeline-spark-bước-11).

## 4. Việc thường làm

### Sửa hoặc bổ sung toạ độ trạm

1. Mở `data/raw/location-tyt.xlsx`, sửa ô toạ độ ở cột thứ ba theo dạng `vĩ độ, kinh độ` (ví dụ `16.0500, 108.1797`). Giữ nguyên tên xã ở cột thứ hai.
2. Chạy lại từ bước 04:
   ```bash
   source .venv/bin/activate && cd scripts
   for s in 04_stations 05_network 06_kpi 07_diagnose 08_forecast 09_greedy 13_giao_thong 14_ban_do 10_quality 12_export_csv 11a_landing; do python $s.py || break; done
   cd .. && bash scripts/run_spark.sh
   ```
3. Đọc `outputs/tables/qa_stations.csv` và `qa_stations_nhap_tay_bi_loai.csv`. Toạ độ nằm ngoài Đà Nẵng cũ bị loại, trạm đó quay về vị trí ước lượng.

### Cập nhật danh sách cơ sở khám chữa bệnh

1. Sửa `data/raw/danh-sach-cskcb.xlsx` (sheet đầu tiên). Bước 15 đọc các cột theo tên: `Tên`, `Mã`, `Tuyến CMKT`, `Hạng bệnh viện`,
   `Cấp CMKT`, `Điểm`, `Địa chỉ`, `tọa độ`. Cột `tọa độ` ghi dạng `vĩ độ, kinh độ`; dòng không đọc được toạ độ bị loại và log ghi `CAN XEM`.
   Cột `Cấp CMKT` dùng đúng ba giá trị `Cấp ban đầu`, `Cấp cơ bản`, `Cấp chuyên sâu`.
2. Chạy lại riêng bước 15: `.venv/bin/python scripts/15_tuyen_tren.py` (không cần chạy lại Spark).
3. Ghi lại thay đổi vào giả định A17 trong [CLAUDE.md](../CLAUDE.md) (số cơ sở, nguồn, ngày cập nhật).

### Đổi ngưỡng rà soát gộp trạm (A15)

Hai hằng số ở đầu `scripts/15_tuyen_tren.py`: `A15_BK` (bán kính, mặc định 2000 m) và `A15_NGUONG` (số CSKCB tối thiểu, mặc định 6).
Bảng `doi_soat_nq1251_quy_tac.csv` luôn tính kèm 4 quy tắc khác để so. Đổi ngưỡng để "khớp NQ 1251 hơn" làm p-value càng lạc quan;
mọi thay đổi phải ghi vào A15.

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
Điều kiện phải trả về `true` cho dòng **vi phạm**. Mã mới được tính là lỗi dữ liệu (`quarantine/`); nếu nó chỉ đánh dấu dữ liệu ngoài phạm vi chứ không phải lỗi, thêm mã vào bộ `MA_NGOAI_PHAM_VI` ở đầu file để dòng vào `excluded/`. Chạy lại Spark; số dòng theo mã mới xuất hiện trong `spark_quarantine_tong_hop.csv`.

### Xem Spark thực thi truy vấn thế nào

Mỗi lần chạy, bước 11 ghi kế hoạch thực thi (`explain("formatted")`) của truy vấn KPI chính toàn 56 xã vào `logs/plan_kpi_2024.txt`: các phép nối chéo broadcast, tổng hợp, quét Parquet đã lưu. Thư mục `logs/` không được đưa lên GitHub; muốn nộp làm minh chứng thì chép file ra ngoài.

### Sửa code và giữ khả năng truy vết

Ghi file kết quả bằng `save_csv(...)` hoặc `register(...)` trong `common.py` để file được đưa vào `outputs/run_manifest.csv` kèm SHA-256.

### Tuỳ chỉnh bằng biến môi trường

Các tham số phân tích nằm trong `common.py` (bảng ở trên). Các biến dưới đây chỉ đổi cách chạy, không đổi kết quả.

| Biến | Script đọc | Mặc định | Tác dụng |
|---|---|---|---|
| `PAR` | `01_download.sh` | `4` | Số luồng tải song song |
| `FULL` | `01_download.sh` | `0` | `FULL=1` tải thêm 7 file nhóm tuổi 60+ độ phân giải 100 m năm 2024; hiện chưa có script nào đọc các file này |
| `PYTHON` | `run_all.sh`, `run_spark.sh` | `python3` (run_all), `.venv/bin/python` (run_spark) | Trình thông dịch Python dùng để chạy các bước pandas và bước 15 |
| `SPARK_IMAGE` | `run_spark.sh` | `jupyter/pyspark-notebook:latest` | Image Docker chạy bước 11 |
| `HDFS_DIR`, `NN`, `RM` | `11b_hdfs_yarn_cli.sh` | `/mis5007`, `mis5007-namenode`, `mis5007-resourcemanager` | Thư mục gốc trên HDFS, tên container NameNode và ResourceManager |
| `HDFS_URI` | `11_spark_pipeline.py` | không đặt | Đặt thì đọc và ghi các vùng trên HDFS; không đặt thì dùng `data/landing/` và `data/spark/` |
| `SPARK_DRIVER_MEMORY` | `11_spark_pipeline.py` | `3g` | Bộ nhớ cấp cho driver Spark |
| `SPARK_SHUFFLE_PARTITIONS` | `11_spark_pipeline.py` | `16` | Số phân vùng khi shuffle |
| `SPARK_MASTER` | `11_spark_pipeline.py` | `local[*]` | Địa chỉ master của Spark |
| `SPARK_UI` | `11_spark_pipeline.py` | `false` | `true` bật Spark UI trong lúc pipeline chạy |
| `SPARK_PARTITION_OVERWRITE` | `11_spark_pipeline.py` | `static` | `dynamic` chỉ ghi đè các phân vùng năm có trong dữ liệu mới. Chỉ dùng khi cập nhật vài năm; nếu thu hẹp `YEARS` mà vẫn dùng `dynamic`, năm cũ còn lại và đối soát báo `LỆCH` |
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
.venv/bin/python scripts/15_tuyen_tren.py                 # bước 15, chạy trên máy sau khi Spark xong
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
| Bước 11 dừng với `RuntimeError: Thieu .../_SUCCESS` | Một lần ghi Parquet trước đó bị ngắt giữa chừng (Parquet không có giao dịch), bảng còn rỗng hoặc dở dang | Chạy lại bước 11 từ đầu; không đọc bảng đó bằng công cụ khác cho tới khi chạy xong |
| `ModuleNotFoundError` khi chạy bước Python | Chưa kích hoạt môi trường ảo | `source .venv/bin/activate` |
| `doi_soat_spark.csv` có dòng `LỆCH` | Chạy Spark trên CSV thô cũ hơn kết quả pandas | Chạy lại `run_all.sh` rồi Spark; trên HDFS thêm `11b_hdfs_yarn_cli.sh truoc` |
| `docker: Cannot connect to the Docker daemon` | Docker Desktop chưa mở | Mở Docker Desktop, chờ biểu tượng báo đang chạy |
| `port is already allocated` khi `up -d` | Cổng bị chương trình khác dùng | Đổi số bên trái dấu `:` ở mục `ports` trong `docker/docker-compose.yml` |
| `container name ... is already in use` | Còn container cũ cùng tên | `docker compose -f docker/docker-compose.yml down` rồi `up -d` |
| Spark báo `Java heap space` | Docker được cấp ít RAM, hoặc driver Spark thiếu bộ nhớ | Tăng RAM trong Docker Desktop; hoặc tăng `SPARK_DRIVER_MEMORY` (ví dụ `4g`) theo cách ghi ở mục 4, phần "Tuỳ chỉnh bằng biến môi trường" |
| `hdfs dfsadmin -report` có "Under replicated blocks" | File ghi với hệ số nhân bản 3 trên cụm 1 DataNode | `bash scripts/11b_hdfs_yarn_cli.sh sau` (tự đưa về 1) |
| `Name node is in safe mode` | NameNode vừa khởi động | Chờ khoảng một phút; `11b_hdfs_yarn_cli.sh truoc` tự chờ |
| Bước 11 trên HDFS báo `Path does not exist: .../landing/...` | Chưa đưa CSV thô lên HDFS | `bash scripts/11b_hdfs_yarn_cli.sh truoc` |
| Bước 15 báo `No such file ... tram_vung_phuc_vu_spark.csv` | Chưa chạy Spark | Chạy `bash scripts/run_spark.sh` (tự chạy bước 15) |
| Log bước 15 có `LECH: danh sach xa gop trong code vs file tham chieu` | `data/reference/nq1251_sap_xep.csv` và danh sách `NQ1251_GROUPS` trong `15_tuyen_tren.py` khác nhau | Sửa cho khớp; danh sách trong code cũng phải khớp `07_diagnose.py` |

Không tìm ra nguyên nhân: mở file log mới nhất của bước bị lỗi trong `logs/`.

## 7. Khi đưa số liệu vào báo cáo

- Không viết "cách ly 2.936.775 dòng lỗi". Viết đúng: lỗi dữ liệu là 9 dòng (dòng tiêu đề và tên quận trong file toạ độ nhập tay); 2.936.766 dòng còn lại là ngoài phạm vi – mã NoData WorldPop dùng cho ô không có dân và pixel ngoài 56 xã – không phải dữ liệu hỏng (`spark_quarantine_tong_hop.csv`, cột `loai`).

- Mỗi con số phải dẫn về một file trong `outputs/tables/` hoặc `logs/`. Chưa có file thì ghi "chưa có số".
- WorldPop là ước tính mô hình; ghi rõ điều này cạnh mọi con số dân.
- Mọi con số sau năm 2024 là dự báo (bước 08) hoặc số chiếu của WorldPop; ghi rõ là dự báo và kèm khoảng 80% (A9).
- Ngưỡng 3 km là giả định của nhóm (A1); luôn kèm kết quả 2 km, 5 km, theo đường đi và theo 15 phút.
- `ung_vien_gop_tram.csv` là danh sách rà soát, không phải đề nghị đóng trạm; khi dẫn mức khớp với NQ 1251 phải nói rõ ngưỡng được chọn sau khi biết kết quả (A15).
- Phân loại "cơ sở khác thay thế được" của 15 gói dịch vụ là đánh giá của nhóm, không có trong Thông tư 30/2024/TT-BYT; dẫn bản chính thức của thông tư, không dẫn bản thảo PDF trong `data/reference/` (A16).
- Số đếm CSKCB là sàn dưới vì danh sách không có phòng khám tư nhỏ (A17).
- Ghi giấy phép: WorldPop CC BY 4.0; OpenStreetMap ODbL 1.0 ("© OpenStreetMap contributors"); GADM chỉ dùng học thuật, không phân phối lại; AWS Terrain Tiles ghi nguồn.
- Kết quả trong `_luu_tru_pham_vi_2015_2030/` thuộc phạm vi cũ (năm cơ sở 2025, có dự báo); không dùng.
