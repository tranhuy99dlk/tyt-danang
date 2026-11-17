# Tài liệu kiến trúc

Dành cho người cần hiểu dự án được ghép từ những phần nào, dữ liệu đi qua đâu và vì sao thiết kế như vậy.
Cách cài đặt: [HUONG_DAN_CAI_DAT.md](HUONG_DAN_CAI_DAT.md). Cách dùng hằng ngày: [HUONG_DAN_SU_DUNG.md](HUONG_DAN_SU_DUNG.md).

## 1. Bối cảnh và mục tiêu

Đồ án học phần MIS5007 – Dữ liệu lớn và Ứng dụng. Người đọc kết quả là ban lãnh đạo Sở Y tế.

Bối cảnh: từ 01/01/2025 Đà Nẵng sắp xếp đơn vị hành chính cấp xã (NQ 1251/NQ-UBTVQH15). Dự án đo mức tiếp cận
trạm y tế của người dân theo bản đồ **trước** sắp xếp: 56 xã/phường, mỗi xã một trạm.

Hệ thống phải trả lời được:

1. Dữ liệu có đủ tin cậy không (thiếu, trùng, sai toạ độ, lệch tổng dân số)?
2. Bao nhiêu phần trăm dân sống trong 3 km tới trạm gần nhất, tính theo đường chim bay và theo đường đi?
3. Các chỉ số phụ: dân bình quân mỗi trạm, khoảng cách trung vị, dân ở xa trên 5 km, tỷ lệ người 60+ được tiếp cận, chênh lệch phường – xã miền núi.
4. Nên đặt thêm điểm ở đâu.

Phạm vi phân tích: 10 mốc năm 2015–2024, năm hiện trạng 2024. Bước 08 dự báo thêm tới 2030 từ chuỗi này; kết quả dự báo nằm trong các file riêng.

Yêu cầu kỹ thuật của học phần: Docker Desktop, PySpark, HDFS NameNode, HDFS/YARN CLI, PySpark kết nối HDFS trong JupyterLab;
pipeline Spark phải đọc với schema, kiểm tra, tách lỗi, làm sạch, biến đổi/join, tổng hợp, ghi kết quả và có đối soát.

## 2. Sơ đồ tổng thể

```
 NGUỒN                THU NHẬN               LƯU TRỮ                 XỬ LÝ                    KIỂM TRA              ĐẦU RA
┌──────────────────┐ ┌────────────────────┐ ┌─────────────────────┐ ┌──────────────────────┐ ┌───────────────────┐ ┌──────────────────┐
│ WorldPop R2025A  │ │ 01_download.sh     │ │ data/raw/           │ │ pandas (trên máy)    │ │ 10_quality.py     │ │ outputs/tables/  │
│  .tif 100 m,1 km │►│  tải + SHA-256     │►│  + manifest_raw.csv │►│  02 ranh giới        │►│  qa_summary.csv   │►│  *.csv (KPI, QA) │
│ OpenStreetMap    │ │                    │ │ data/reference/     │ │  03 lưới dân số      │ │  tu_dien_du_lieu  │ │ outputs/figures/ │
│  (Overpass API)  │ │ 11a_landing.py     │ │ data/interim/       │ │  04 trạm             │ │                   │ │  h1–h5, h7–h9    │
│ GADM 4.1         │ │  .tif/.xlsx → CSV  │ ├─────────────────────┤ │  05 mạng đường       │ │ 11 (bước 7)       │ │ run_manifest.csv │
│ AWS Terrain Tiles│ │  thô, không sửa    │ │ landing    CSV thô  │ │  06 KPI, 07, 09, 13  │ │  doi_soat_spark   │ │  SHA-256 mỗi file│
│ location-tyt.xlsx│ │                    │ │ curated    Parquet  │ ├──────────────────────┤ │  bảo toàn số dòng,│ │ notebook đã chạy │
│  (toạ độ nhập tay)│ │ 11b …_cli.sh      │ │ quarantine Parquet  │ │ Spark (trong Docker) │ │  tổng dân;        │ │ logs/*.log       │
│ NQ 1251 (dân số) │ │  hdfs dfs -put     │ │ gold       Parquet  │ │  11_spark_pipeline   │ │  Spark ↔ pandas   │ │                  │
└──────────────────┘ └────────────────────┘ │ (data/spark/ │ HDFS) │ └──────────────────────┘ └───────────────────┘ └──────────────────┘
                                            └─────────────────────┘
```

Có hai động cơ xử lý chạy nối tiếp nhau:

- **pandas trên máy** làm phần hình học và đồ thị mà Spark không có sẵn: cắt raster, xét điểm nằm trong đa giác xã, Dijkstra trên mạng đường.
- **Spark trong Docker** nhận dữ liệu thô dạng CSV, tự kiểm tra, tách lỗi, tính lại toàn bộ KPI và đối soát với kết quả pandas.

## 3. Thành phần

| Thành phần | Công nghệ | Vai trò | Nằm ở |
|---|---|---|---|
| Tải dữ liệu | bash, curl, xargs, shasum | Tải file nguồn mở, ghi URL + SHA-256 + giấy phép | `scripts/01_download.sh` |
| Xử lý không gian | Python 3.11, pandas, rasterio, shapely, pyproj, scipy | Ranh giới, lưới dân số, trạm, mạng đường, KPI, chẩn đoán, đề xuất | `scripts/02`–`07`, `09`, `13` |
| Kiểm tra chất lượng | pandas | 4 nhóm kiểm tra + từ điển dữ liệu | `scripts/10_quality.py` |
| Thu nhận cho Spark | rasterio, pandas, openpyxl | Đổ raster và Excel ra CSV thô | `scripts/11a_landing.py` |
| Pipeline Spark | PySpark 3.5 | Schema, kiểm tra, quarantine, biến đổi, tổng hợp, đối soát | `scripts/11_spark_pipeline.py` |
| Lưu trữ phân tán | HDFS 3.2.1 (1 NameNode, 1 DataNode) | Các vùng landing / curated / quarantine / gold | cụm Docker |
| Quản lý tài nguyên | YARN (ResourceManager, NodeManager) | Xem trạng thái cụm qua CLI | cụm Docker |
| Giao diện làm việc | JupyterLab + PySpark | Chạy notebook bước 11 nối HDFS | `notebooks/buoc11_pyspark_hdfs.ipynb` |
| Tham số dùng chung | Python | Năm, ngưỡng km, hộp bao, tốc độ, đường dẫn | `scripts/common.py` |

## 4. Luồng dữ liệu theo từng bước

| Bước | Script | Đọc | Ghi | Việc chính |
|---|---|---|---|---|
| 01 | `01_download.sh` | Internet | `data/raw/`, `data/manifest_raw.csv` | Tải WorldPop (100 m, 1 km, nhóm tuổi), OSM, GADM, độ cao |
| 02 | `02_boundaries.py` | OSM ranh giới, GADM, danh sách 56 trạm | `communes_56.geojson`, `qa_boundaries.csv` | Ghép 56 xã với ranh giới; chọn nguồn khớp nhiều hơn |
| 03 | `03_grid.py` | raster 100 m, 1 km, nhóm tuổi, độ cao | `grid_100m.parquet`, `grid_panel_2015_2024.parquet`, `commune_series.csv` | Mỗi ô 100 m có dân: xã, dân từng năm, dân 60+, độ cao, độ dốc |
| 04 | `04_stations.py` | `location-tyt.xlsx`, OSM cơ sở y tế | `stations_56.csv`, `qa_stations.csv` | Toạ độ 56 trạm: nhập tay → OSM → ước lượng |
| 05 | `05_network.py` | OSM đường | `road_graph.npz`, `grid_network.parquet` | Đồ thị đường; Dijkstra đa nguồn từ 56 trạm |
| 06 | `06_kpi.py` | lưới, trạm, mạng đường | `kpi_overview.csv`, `kpi_by_*.csv`, `grid_access.parquet` | KPI chính và phụ (bản pandas) |
| 07 | `07_diagnose.py` | `grid_access.parquet` | `white_zones.csv`, `scenario_nq1251.csv`, h1–h4 | Vùng trắng; kịch bản gộp 56 → 47 trạm |
| 08 | `08_forecast.py` | `commune_series.csv`, `grid_access.parquet`, lưới 1 km 2025–2030 | `forecast_*.csv`, `access_2030_*.csv`, `grid_2030.parquet`, h5 | So 4 mô hình bằng backtest, dự báo dân và dân 60+ từng xã tới 2030, tiếp cận 3 km năm 2030 |
| 09 | `09_greedy.py` | `grid_access.parquet` | `greedy_*.csv`, h7 | Xếp hạng 10 điểm đề xuất |
| 13 | `13_giao_thong.py` | OSM đường, đồ thị, vùng phục vụ, `greedy_sites.csv` của bước 09 | `tram_giao_thong.csv`, `diem_de_xuat_giao_thong.csv` | Đường cạnh từng trạm, từng điểm đề xuất |
| 14 | `14_ban_do.py` | ranh giới, trạm, `grid_access.parquet`, `greedy_sites.csv` | `pham_vi_da_nang_cu.csv`, `xa_phuong_56_toa_do.csv`, `ban_do_truoc_sau_de_xuat.csv`, h8, h9 | Bản đồ hành chính trước 01/01/2025 và bản đồ so sánh trước/sau đề xuất |
| 10 | `10_quality.py` | mọi bảng trên | `qa_summary.csv`, `qa_official_population.csv`, `tu_dien_du_lieu.csv` | Thiếu, trùng, sai toạ độ, lệch tổng; từ điển dữ liệu |
| 12 | `12_export_csv.py` | `grid_access.parquet` | `dan_cu_luoi_100m.csv`, `tiep_can_luoi_100m.csv` | Xuất lưới ra CSV kèm giải thích cột |
| 11a | `11a_landing.py` | raster, Excel, parquet | `data/landing/*.csv` | CSV thô cho Spark, giữ nguyên cả giá trị lỗi |
| 11b | `11b_hdfs_yarn_cli.sh` | `data/landing/` | HDFS `/mis5007/landing`, log CLI | `hdfs dfs -put`, `fsck`, `yarn node -list` … |
| 11 | `11_spark_pipeline.py` | landing | curated, quarantine, gold, `*_spark.csv`, `doi_soat_spark.csv` | Xem mục 6 |

Lưới 1 km các năm 2025–2030 của WorldPop là số chiếu; chỉ bước 08 đọc chúng, để đối chiếu và để phân bố dân 2030 trong từng xã (giả định A9). Spark (bước 11) không tính dự báo.

## 5. Các vùng lưu trữ

| Vùng | Trên máy | Trên HDFS | Nội dung | Ai ghi |
|---|---|---|---|---|
| raw | `data/raw/` | – | File nguồn nguyên bản | 01, người dùng (Excel) |
| reference | `data/reference/` | – | Danh sách 56 trạm, số liệu NQ 1251 | người dùng |
| interim | `data/interim/` | – | Parquet/GeoJSON trung gian của pandas | 02–06 |
| landing | `data/landing/` | `/mis5007/landing` | 6 file CSV thô | 11a (và 11b đưa lên HDFS) |
| curated | `data/spark/curated/` | `/mis5007/curated` | Bảng đã sạch: sự kiện ô × năm, danh mục ô, trạm, xã | 11 |
| quarantine | `data/spark/quarantine/` | `/mis5007/quarantine` | Dòng lỗi kèm lý do, phân vùng theo nguồn | 11 |
| gold | `data/spark/gold/` | `/mis5007/gold` | 6 bảng KPI | 11 |
| outputs | `outputs/tables/`, `outputs/figures/` | – | CSV và hình cho báo cáo | mọi bước |

Bước 11 chỉ dùng một trong hai cột: không đặt `HDFS_URI` thì ghi vào `data/spark/`, đặt `HDFS_URI` thì đọc và ghi trên HDFS.

## 6. Pipeline Spark (bước 11)

**Đầu vào** – 6 file CSV, mỗi file có schema khai báo bằng `StructType`. Đọc ở chế độ `PERMISSIVE`: dòng sai kiểu hoặc sai số cột
không làm hỏng lần đọc mà rơi vào cột `_corrupt_record`.

| File | Một dòng là | Khoá |
|---|---|---|
| `pop_100m_raw.csv` | một pixel 100 m trong một năm, giá trị thô | year, row, col |
| `cells_dim.csv` | một pixel: toạ độ, xã, ô 1 km, độ cao | row, col |
| `age_1km_raw.csv` | một ô 1 km × năm × biến (tổng dân hoặc một nhóm tuổi) | year, k1_row, k1_col, bien |
| `stations_raw.csv` | một dòng của file Excel nhập tay | dong_excel |
| `net_dist.csv` | một ô: đường đi ngắn nhất tới trạm | cell_id |
| `dm_56_tram.csv` | một trạm/xã trong danh mục | ma_tram |

**Kiểm tra và tách lỗi** – hàm `split()` áp danh sách quy tắc cho từng nguồn. Dòng vi phạm ít nhất một quy tắc được ghi vào vùng
quarantine với `nguon`, `ly_do` (nhiều mã nối bằng `;`), `khoa`, `ban_ghi` (nguyên văn dạng JSON).

| Mã lý do | Ý nghĩa | Áp cho |
|---|---|---|
| `SAI_KIEU_HOAC_SO_COT` | Không đọc được theo schema | mọi nguồn |
| `THIEU_KHOA`, `TRUNG_KHOA` | Thiếu hoặc trùng khoá | mọi nguồn |
| `THIEU_GIA_TRI`, `GIA_TRI_AM_NODATA` | Rỗng, hoặc âm (mã NoData −99999 của WorldPop) | dân số 100 m, 1 km |
| `NAM_NGOAI_PHAM_VI` | Năm ngoài 2015–2024 | dân số 100 m, 1 km |
| `NGOAI_56_XA`, `MA_XA_KHONG_CO_TRONG_DANH_MUC` | Pixel không thuộc xã nào trong phạm vi | ô lưới, dân số |
| `SAI_TOA_DO_NGOAI_HOP_BAO`, `NGOAI_HOP_BAO` | Toạ độ ngoài Đà Nẵng cũ | ô lưới, trạm |
| `KHONG_PHAI_BAN_GHI_TRAM`, `SAI_DINH_DANG_TOA_DO`, `TEN_KHONG_KHOP_DANH_MUC`, `TRUNG_TRAM` | Dòng tiêu đề/tên quận, toạ độ sai dạng, tên lạ, trùng trạm | trạm |
| `KHONG_TOI_DUOC_HOAC_AM`, `KHONG_THUOC_O_CO_DAN` | Không có đường tới, hoặc ô không thuộc lưới | mạng đường |
| `THIEU_TOA_DO` | Ô lưới thiếu kinh độ, vĩ độ hoặc toạ độ mét | ô lưới |
| `KHONG_CO_O_LUOI`, `O_LUOI_BI_CACH_LY` | Pixel dân số không có trong danh mục ô lưới, hoặc thuộc một ô đã bị cách ly | dân số 100 m |
| `BIEN_KHONG_HOP_LE` | Tên biến không phải `pop` hay một trong 7 nhóm tuổi `t_60` … `t_90` | lưới 1 km |

Lần chạy 17/11/2026 chỉ phát sinh bốn mã: `GIA_TRI_AM_NODATA`, `NGOAI_56_XA`, `KHONG_PHAI_BAN_GHI_TRAM`, `SAI_DINH_DANG_TOA_DO`
(`outputs/tables/spark_quarantine_tong_hop.csv`). Các mã còn lại là quy tắc đang bật nhưng chưa có dòng nào vi phạm.

**Làm sạch** – bỏ trùng; tách chuỗi "vĩ độ, kinh độ" thành hai số; chuẩn hoá tên xã để ghép với danh mục; chiếu toạ độ trạm sang UTM 49N;
bỏ ô có dân bằng 0 ở mọi năm.

**Biến đổi và join**

| Mã | Phép biến đổi | Ý nghĩa nghiệp vụ |
|---|---|---|
| T1 | Gộp 7 nhóm tuổi 60–90+ của lưới 1 km, nối vào ô 100 m | Số người cao tuổi từng ô (giả định A3) |
| T2 | Nối chéo ô có dân × 56 trạm (broadcast), lấy trạm gần nhất | Khoảng cách đường chim bay tới trạm |
| T3 | Nối khoảng cách mạng đường, chia cho đường chim bay | Hệ số vòng: đường đi có thuận tiện không (A6) |
| T4 | Trung bình độ dốc, độ cao theo dân của từng xã | Phân loại phường / xã đồng bằng / xã miền núi (A7) |

**Tổng hợp** – KPI theo năm, theo xã × năm, theo xã, theo nhóm, theo trạm và toàn 56 xã. Trung vị có trọng số dân được tính bằng
cửa sổ cộng dồn, cùng định nghĩa với bản pandas.

**Ghi** – mỗi bảng curated được ghi ra vùng lưu trữ rồi đọc lại trước khi dùng tiếp. Cách này cắt chuỗi phụ thuộc của Spark
và bảo đảm tầng gold được tính từ dữ liệu đã lưu.

**Đối soát** – 25 phép, ghi vào `doi_soat_spark.csv`:

- Bảo toàn: số dòng thô = hợp lệ + cách ly; tổng dân thô = hợp lệ + cách ly; số dòng đọc lại = số dòng đã ghi.
- Spark so với pandas: tổng dân theo xã × năm, khoảng cách tới trạm gần nhất, toạ độ trạm sau chiếu, KPI theo xã, theo nhóm, toàn 56 xã.

## 7. Hạ tầng Docker

File cấu hình: `docker/docker-compose.yml`, tên dự án compose là `mis5007`.

| Container | Image | Cổng trên máy | Dùng để |
|---|---|---|---|
| `mis5007-namenode` | bde2020/hadoop-namenode 3.2.1 | 9871 | HDFS NameNode và giao diện duyệt file |
| `mis5007-datanode` | bde2020/hadoop-datanode 3.2.1 | 9864 | Lưu block dữ liệu |
| `mis5007-resourcemanager` | bde2020/hadoop-resourcemanager 3.2.1 | 8088 | YARN |
| `mis5007-nodemanager` | bde2020/hadoop-nodemanager 3.2.1 | 8042 | YARN |
| `mis5007-spark-notebook` | jupyter/pyspark-notebook | 8888, 4041 | JupyterLab, PySpark, Spark UI |

- Các container nói chuyện với nhau trong mạng `hadoop_spark_net` bằng tên dịch vụ (`namenode`, `datanode`, `resourcemanager`), nên địa chỉ HDFS luôn là `hdfs://namenode:9000`.
- Thư mục dự án được gắn thẳng vào `/home/jovyan/work/mis5007_tram_y_te` của container JupyterLab: Spark đọc code và ghi CSV kết quả ngay vào thư mục dự án.
- Dữ liệu HDFS nằm trong hai volume `mis5007_hadoop_namenode`, `mis5007_hadoop_datanode`; `docker compose stop` không xoá chúng.

## 8. Quyết định thiết kế và đánh đổi

| Quyết định | Lý do | Cái giá phải trả |
|---|---|---|
| Hình học và Dijkstra làm bằng pandas/scipy, không làm trong Spark | Spark không có sẵn phép điểm-trong-đa-giác và đường đi ngắn nhất; dữ liệu đủ nhỏ để chạy trên một máy | Hai động cơ, phải đối soát với nhau |
| Spark tính lại KPI từ CSV thô thay vì chỉ đọc kết quả pandas | Đáp ứng yêu cầu pipeline Spark đầy đủ; có hai cách tính độc lập để kiểm tra chéo | Logic bị lặp ở hai nơi; sửa công thức phải sửa cả hai |
| CSV thô giữ nguyên mã NoData và pixel ngoài phạm vi | Dữ liệu quarantine là lỗi thật của nguồn, không phải lỗi tạo ra để minh hoạ | File landing lớn hơn (3 triệu dòng pixel) |
| Không dùng API chỉ đường; tự dựng đồ thị từ OSM và chạy Dijkstra | Miễn phí, chạy lại được, không vướng hạn mức và giấy phép | Không có ùn tắc, đường một chiều; tốc độ là giả định (A6) |
| Ranh giới xã lấy từ GADM 4.1 | OSM mốc 31/12/2024 đã vẽ theo bản đồ sau sáp nhập, chỉ khớp 45/56 xã (A12) | GADM thô hơn và không được phân phối lại |
| Toạ độ trạm do nhóm nhập tay, kiểm tra theo Cổng dữ liệu Đà Nẵng (congdulieu.vn), dữ liệu cập nhật 12/12/2024 | OSM chỉ có 4/56 trạm | Nhập thủ công nên có thể gõ nhầm; chưa kiểm chứng thực địa (A13) |
| Spark chạy `local[*]`, không chạy trên YARN | Image NodeManager của học phần không cài Python nên không chạy được executor PySpark | YARN chỉ được dùng qua CLI để xem trạng thái cụm |
| Tên container có tiền tố `mis5007-`, cổng 9871 và 4041 | Chạy song song được với cụm khác dùng tên `namenode` và cổng 9870, 4040 | Khác tên/cổng trong tài liệu chung của lớp |
| Hệ số nhân bản HDFS = 1 | Cụm chỉ có một DataNode | Không có bản sao dự phòng |
| Mọi tham số nằm trong `common.py` | Đổi năm hay ngưỡng ở một chỗ, tên file và nhãn tự đổi theo | Đổi năm cơ sở làm đổi tên cột (`pop_2024`) |

## 9. Truy vết và chất lượng

| Cần biết | Xem ở |
|---|---|
| File nguồn tải từ đâu, lúc nào, có bị đổi không | `data/manifest_raw.csv` (URL, SHA-256, giờ tải, giấy phép) |
| File kết quả do script nào tạo, bao nhiêu dòng | `outputs/run_manifest.csv` |
| Một lần chạy đã in ra gì | `logs/<tên bước>_<giờ UTC>.log` |
| Dữ liệu có vấn đề gì | `outputs/tables/qa_summary.csv` và các bảng `qa_*.csv` |
| Spark và pandas có khớp không | `outputs/tables/doi_soat_spark.csv` |
| Dòng nào bị loại và vì sao | `spark_quarantine_tong_hop.csv`, `spark_quarantine_mau.csv`, vùng quarantine |
| Ý nghĩa từng trường | `outputs/tables/tu_dien_du_lieu.csv`, `giai_thich_csv_luoi.csv` |

## 10. Hạn chế đã biết

- WorldPop là ước tính mô hình, không phải số đếm; lệch khoảng 21% so với số liệu NQ 1251 ở các phường trung tâm (`qa_official_population.csv`).
- Mạng đường gồm cả cao tốc dù xe máy không được đi; mức ảnh hưởng chưa đo (A6).
- KPI theo năm giữ nguyên vị trí 56 trạm hiện nay vì không có dữ liệu lịch sử vị trí trạm (A14).
- "Xã miền núi" phân loại theo địa hình, chưa đối chiếu danh sách chính thức (A7).
- Mạng đường và danh sách cơ sở y tế OSM được tải ở thời điểm chạy `01_download.sh`; tải lại vào ngày khác có thể cho số liệu đường đi hơi khác.
- Dự án mới chạy thử trên macOS (máy Intel); Linux và Windows chưa được kiểm tra.

Bảng giả định đầy đủ A1–A14 nằm trong [CLAUDE.md](../CLAUDE.md).
