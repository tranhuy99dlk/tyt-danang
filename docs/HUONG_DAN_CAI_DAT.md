# Hướng dẫn cài đặt cho người mới

Mục tiêu: từ một máy tính chưa có gì, chạy được toàn bộ dự án và ra đủ bảng kết quả.
Làm lần lượt từ mục 1 đến mục 9. Mỗi mục có phần "Thấy gì là đúng" để tự kiểm tra trước khi đi tiếp.

Tóm tắt cho người đã quen:

```bash
bash scripts/00_check.sh                                              # máy đủ điều kiện chưa
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.lock.txt
bash scripts/01_download.sh                                           # tải dữ liệu nguồn
source .venv/bin/activate && bash scripts/run_all.sh                  # phần pandas + CSV thô cho Spark
bash scripts/run_spark.sh                                             # Spark (11, 16), đối chiếu CSKCB (15), kiểm tra/chốt số (17)
```

## 1. Máy cần gì

| Hạng mục | Yêu cầu |
|---|---|
| Hệ điều hành | macOS. Dự án mới được chạy thử trên macOS 15, máy Intel. Linux và Windows chưa được kiểm tra (xem mục 10) |
| Ổ đĩa trống | Từ 20 GB: dữ liệu nguồn khoảng 1 GB, môi trường Python 0,5 GB, image Docker khoảng 9 GB, kết quả dưới 0,5 GB |
| RAM | Máy 16 GB; cấp cho Docker ít nhất 6 GB |
| Mạng | Cần Internet để tải dữ liệu (khoảng 1 GB) và image Docker (lần đầu) |
| Quyền | Cài được phần mềm; không cần quyền quản trị để chạy dự án |

## 2. Cài công cụ

Cần ba thứ: Docker Desktop, `uv` (quản lý Python), và các lệnh có sẵn trên macOS (`bash`, `curl`, `shasum`, `xargs`).

**Docker Desktop**

1. Tải tại https://www.docker.com/products/docker-desktop/ và cài như ứng dụng thường.
2. Mở Docker Desktop, chờ biểu tượng cá voi báo đang chạy.
3. Vào Settings → Resources, đặt Memory từ 6 GB trở lên, bấm Apply & Restart.

**uv** – công cụ tạo môi trường Python và tự tải đúng bản Python 3.11:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Đóng rồi mở lại Terminal để nhận lệnh `uv`. Không muốn dùng `uv`: cài Python 3.11 từ https://www.python.org/downloads/ và dùng lệnh thay thế ở mục 5.

Thấy gì là đúng:

```bash
docker --version && docker compose version && uv --version
```

In ra ba dòng phiên bản, không có dòng `command not found`. Bản đã thử: Docker 29.9, Docker Compose 5.6, uv 0.12.

## 3. Lấy thư mục dự án

Mã nguồn nằm ở https://github.com/tranhuy99dlk/tyt-danang. Có hai cách lấy thư mục dự án.

**Cách 1 – clone từ GitHub**

```bash
git clone https://github.com/tranhuy99dlk/tyt-danang.git mis5007_tram_y_te
cd mis5007_tram_y_te
```

Kho có sẵn code, tài liệu, ba file nhóm tự chuẩn bị, dữ liệu OpenStreetMap nhóm đã tải và kết quả mẫu.
Kho không chứa WorldPop, độ cao và GADM; mục 6 sẽ tải về (khoảng 1 GB). Vì dữ liệu OpenStreetMap đi kèm,
các chỉ số theo đường đi ra đúng số của nhóm, giống gói A bên dưới.

**Cách 2 – nhận file nén từ người trong nhóm**

Dùng khi không vào được GitHub. Có hai kiểu gói:

| Gói | Chứa | Dung lượng | Khi nào dùng |
|---|---|---|---|
| A – đầy đủ | Code, tài liệu, dữ liệu nguồn đã tải, kết quả mẫu | khoảng 1,1 GB | Muốn ra **đúng** các con số nhóm đã có |
| B – nhẹ | Code, tài liệu, ba file nhóm tự chuẩn bị | dưới 1 MB | Mạng tốt, chấp nhận số liệu đường đi lệch nhẹ |

Vì sao có khác biệt: mạng đường OpenStreetMap được tải ở thời điểm chạy. Tải lại vào ngày khác thì bản đồ đường có thể đã được cập nhật,
nên các chỉ số theo đường đi có thể khác đôi chút. Chỉ số theo đường chim bay không bị ảnh hưởng.

Người gửi tạo gói bằng lệnh sau, chạy ở thư mục **chứa** `mis5007_tram_y_te`:

```bash
# Gói A
zip -r mis5007_goi_A.zip mis5007_tram_y_te -x "*/.venv/*" "*/data/interim/*" "*/data/landing/*" "*/data/spark/*" "*/data/raw/gadm/*" "*/_luu_tru_*/*" "*/__pycache__/*" "*.DS_Store" "*/logs/*"
# Gói B
zip -r mis5007_goi_B.zip mis5007_tram_y_te -x "*/.venv/*" "*/data/interim/*" "*/data/landing/*" "*/data/spark/*" "*/data/raw/worldpop/*" "*/data/raw/terrain/*" "*/data/raw/gadm/*" "*/data/raw/osm/*" "*/data/manifest_raw.csv" "*/outputs/*" "*/logs/*" "*/_luu_tru_*/*" "*/__pycache__/*" "*.DS_Store"
```

Cả hai gói đều không kèm dữ liệu GADM vì giấy phép GADM không cho phân phối lại; script tải sẽ tự lấy về.

Giải nén, rồi mở Terminal tại thư mục dự án:

```bash
cd đường/dẫn/tới/mis5007_tram_y_te
```

Ba file sau **không tải được bằng script**; chúng có sẵn trong kho GitHub và bắt buộc phải có trong gói nén:

| File | Nội dung |
|---|---|
| `data/raw/location-tyt.xlsx` | Toạ độ 56 trạm do nhóm nhập tay, kiểm tra theo Cổng dữ liệu Đà Nẵng (congdulieu.vn), dữ liệu cập nhật 12/12/2024 |
| `data/reference/danh_sach_56_tram.csv` | Danh sách 56 trạm/xã |
| `data/reference/dan_so_chinh_thuc_nq1251.csv` | Số liệu dân số NQ 1251 để đối chiếu |

## 4. Kiểm tra máy

```bash
bash scripts/00_check.sh
```

Thấy gì là đúng: dòng cuối là `KET QUA: du dieu kien.` Dòng `[LUU Y] chua co .venv` ở lần đầu là bình thường; mục 5 sẽ tạo.
Có dòng `[THIEU]` thì xử lý theo gợi ý ngay trên dòng đó rồi chạy lại.

## 5. Tạo môi trường Python

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements.lock.txt
```

`requirements.lock.txt` ghi đúng phiên bản thư viện nhóm đã chạy. Không dùng `uv`:

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.lock.txt
```

Thấy gì là đúng:

```bash
.venv/bin/python -c "import pandas, rasterio, shapely, pyproj, scipy, pyarrow, openpyxl; print('OK', pandas.__version__)"
```

In ra `OK 3.1.1`.

## 6. Tải dữ liệu nguồn

```bash
bash scripts/01_download.sh
```

Script tải 153 file (WorldPop, độ cao, GADM) qua 4 luồng song song, rồi 4 truy vấn OpenStreetMap, tổng khoảng 1 GB.
Với gói A, gần như mọi file đã có nên script chỉ tải GADM.
Với bản clone từ GitHub, script tải đủ 153 file và bỏ qua 4 truy vấn OpenStreetMap (in `SKIP`) vì dữ liệu này đã có trong kho.

Thấy gì là đúng: gần cuối có dòng `OK=<số file vừa tải>  FAIL=0`. Có `FAIL` thì chạy lại đúng lệnh đó; file đã tải được bỏ qua.
Mỗi file tải về được ghi URL, SHA-256 và giấy phép vào `data/manifest_raw.csv`.

## 7. Chạy pipeline

```bash
source .venv/bin/activate
bash scripts/run_all.sh
```

Chạy 13 bước nối tiếp, mất khoảng 2–3 phút trên máy thử. Màn hình in `=== 02_boundaries ===`, `=== 03_grid ===`, … cho tới `=== 11a_landing ===`.

Thấy gì là đúng:

- Dòng cuối: `Xong. Ket qua: ../outputs/tables, …`
- Trong phần `10_quality` có dòng `QA: 32 kiem tra, 7 CAN XEM`. Bảy mục này là hạn chế đã biết của dữ liệu, giải thích ở [HUONG_DAN_SU_DUNG.md](HUONG_DAN_SU_DUNG.md#3-đọc-kết-quả-kiểm-tra).
- `outputs/tables/` có các file `kpi_overview.csv`, `qa_summary.csv`, `tu_dien_du_lieu.csv`; `data/landing/` có 6 file CSV.

## 8. Chạy pipeline Spark

```bash
bash scripts/run_spark.sh
```

Lệnh này chạy các bước 11 và 16 trong container Spark, rồi chạy bước 15 (TYT và cơ sở khám chữa bệnh) và 17 (kiểm tra/chốt số liệu) bằng Python trong `.venv`. Lần đầu Docker tải image `jupyter/pyspark-notebook` (khoảng 7 GB sau khi giải nén) nên sẽ lâu;
các lần sau mất 4–9 phút trên máy thử.

Màn hình in dần các dòng `INFO` theo từng khâu (đọc, kiểm tra, biến đổi, ghi, đối soát).

Thấy gì là đúng: có dòng `Doi soat: 28 phep, 0 LECH.` và mọi dòng có `7.` đều kết thúc bằng `ĐẠT`; sau đó có dòng `Doi chieu NQ 1251: ... KHOP`, bước 17 báo các file trong manifest đều `ĐẠT`, và dòng cuối chỉ tới `so_lieu_chot.csv`.
Cảnh báo của Spark và Java không hiện trên màn hình mà được ghi vào `logs/11_spark_stderr_*.log`.

Tới đây dự án đã chạy xong và có đủ bảng kết quả. Mục 9 chỉ cần khi phải trình diễn HDFS, YARN và JupyterLab.

## 9. Dựng cụm HDFS + YARN + JupyterLab (tuỳ chọn)

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps          # chờ tới khi cả 5 dòng ghi (healthy)
bash scripts/11b_hdfs_yarn_cli.sh truoc
docker exec -w /home/jovyan/work/mis5007_tram_y_te -e HDFS_URI=hdfs://namenode:9000/mis5007 \
  mis5007-spark-notebook python scripts/11_spark_pipeline.py
bash scripts/11b_hdfs_yarn_cli.sh sau
.venv/bin/python scripts/15_tuyen_tren.py
```

Lần đầu Docker tải thêm bốn image Hadoop. Bước Spark trên HDFS mất 4–9 phút trên máy thử.

Thấy gì là đúng:

- `11b … truoc` in `Found 6 items` dưới `/mis5007/landing`.
- Bước Spark in dòng đầu có `landing=hdfs://namenode:9000/mis5007/landing` và dòng cuối `Doi soat: 28 phep, 0 LECH.`
- `11b … sau` in `Status: HEALTHY`, `Under-replicated blocks: 0` và `Total Nodes:1`.
- Mở được http://localhost:9871 (HDFS), http://localhost:8088 (YARN), http://localhost:8888 (JupyterLab; mật khẩu ở dòng `JUPYTER_TOKEN` trong `docker/docker-compose.yml`).

Tuỳ chọn – chạy executor trên YARN thay vì `local[*]` (cần thêm khoảng 100 MB tải gói Python ở lần đầu):

```bash
bash scripts/11c_yarn_python_env.sh
docker exec -w /home/jovyan/work/mis5007_tram_y_te -e HDFS_URI=hdfs://namenode:9000/mis5007 -e SPARK_MASTER=yarn \
  mis5007-spark-notebook python scripts/11_spark_pipeline.py
```

Thấy gì là đúng: dòng thứ hai in `YARN: ung dung application_..., 2 executor x 2 nhan x 1536m`, dòng cuối `Doi soat: ... 0 LECH.`,
và trang http://localhost:8088 có ứng dụng `mis5007_tram_y_te` trạng thái `FINISHED`, `SUCCEEDED`.

Dùng xong: `docker compose -f docker/docker-compose.yml stop`.

## Kiểm tra cuối: so với kết quả tham chiếu

Mở `outputs/tables/kpi_tong_hop_spark.csv`. Với gói A hoặc bản clone từ GitHub, các giá trị phải trùng bảng dưới (lần chạy ngày 17/11/2026, năm hiện trạng 2024).
Với gói B, ba dòng đầu phải trùng; các dòng theo đường đi có thể lệch nhẹ.

| Chỉ số | Giá trị tham chiếu |
|---|---|
| Tổng dân 56 xã/phường năm 2024 (WorldPop, ước tính) | 1.177.774 người |
| Tỷ lệ dân trong 3 km đường chim bay | 98,69% |
| Dân bình quân mỗi trạm | 21.032 người/trạm |
| Tỷ lệ dân trong 3 km theo mạng đường | 96,10% |
| Tỷ lệ dân tới trạm trong 15 phút xe máy | 99,71% |

Số dòng cũng phải khớp (`outputs/tables/spark_so_dong_theo_nguon.csv`): `pop_100m_raw` có 3.006.720 dòng thô, `stations_raw` có 56 dòng hợp lệ.

## 10. Hệ điều hành khác

Phần này **chưa được chạy thử**; ghi lại để người làm biết chỗ cần chú ý.

- **Linux:** các lệnh giống macOS. Container JupyterLab chạy bằng người dùng có mã 1000; nếu tài khoản trên máy có mã khác, container có thể không ghi được vào thư mục dự án.
- **Windows:** các script dùng `bash`, `curl`, `xargs`, `shasum` nên cần chạy trong WSL2 (Ubuntu), bật Docker Desktop → Settings → Resources → WSL integration, và đặt thư mục dự án bên trong WSL.

## 11. Lỗi hay gặp khi cài

| Hiện tượng | Cách xử lý |
|---|---|
| `uv: command not found` sau khi cài | Đóng và mở lại Terminal |
| `00_check.sh` báo Docker chưa chạy | Mở ứng dụng Docker Desktop, chờ khởi động xong |
| `01_download.sh` có dòng `FAIL` | Chạy lại lệnh; thường do mạng hoặc máy chủ OpenStreetMap bận |
| `run_all.sh` báo `ModuleNotFoundError` | Quên `source .venv/bin/activate` |
| `00_check.sh` báo `[THIEU] data/raw/location-tyt.xlsx` hoặc file trong `data/reference/` | Gói nhận được thiếu file ở mục 3; xin lại từ người gửi, hoặc clone lại từ GitHub |
| `run_spark.sh` in một dòng rồi im lặng lâu ở lần đầu | Docker đang tải image; mở Terminal khác chạy `docker pull jupyter/pyspark-notebook:latest` để xem tiến độ |
| `port is already allocated` ở mục 9 | Máy đang dùng cổng đó; đổi số bên trái dấu `:` trong `docker/docker-compose.yml` |

Các lỗi khi vận hành: [HUONG_DAN_SU_DUNG.md](HUONG_DAN_SU_DUNG.md#6-xử-lý-sự-cố).

## 12. Hỏi ai

Nhóm điền bảng này trước khi chuyển giao; hiện chưa có thông tin.

| Việc | Người phụ trách |
|---|---|
| Toạ độ trạm, danh sách 56 trạm | (chưa điền) |
| Code pipeline, Spark, Docker | (chưa điền) |
| Nội dung báo cáo, giả định | (chưa điền) |

## Đọc tiếp

- [HUONG_DAN_SU_DUNG.md](HUONG_DAN_SU_DUNG.md) – câu hỏi nào xem file nào, sửa toạ độ trạm, đổi tham số.
- [KIEN_TRUC.md](KIEN_TRUC.md) – dữ liệu đi qua đâu, vì sao thiết kế như vậy.
- [CLAUDE.md](../CLAUDE.md) – quy tắc làm việc và bảng giả định A1–A17.
