# Hướng dẫn chạy dự án: không cần Docker và có Docker

Dành cho người mới học code, tự chạy dự án trên MacBook bằng VS Code. Có hai cách chạy:

| | **Phần A – Không Docker (mặc định)** | **Phần B – Có Docker (tuỳ chọn)** |
|---|---|---|
| Dùng khi | Làm báo cáo, demo, sửa đầu vào, đọc code | Trình diễn HDFS, YARN, JupyterLab của học phần |
| Spark chạy ở đâu | `local[*]` – mọi nhân CPU của chính máy | Trong container; executor trên NodeManager YARN |
| Dữ liệu nằm ở | `data/lake/bronze`, `silver`, `gold` trên máy | `hdfs://namenode:9000/mis5007/bronze`, `silver`, `gold` |
| Cần cài | VS Code, uv, Java 17 | Thêm Docker Desktop (RAM cho Docker nên 10–12 GB) |
| Thời gian một lần chạy | khoảng 1 phút | HDFS khoảng 1,5 phút; YARN khoảng 3–5 phút |
| Kết quả có khác không | Bộ số chuẩn để làm báo cáo | Cùng code, cùng kết quả; dự báo 2030 lệch vài người do thư viện trong container khác phiên bản |

Làm Phần A trước. Phần B chỉ làm khi cần trình diễn cụm và **luôn kết thúc bằng bước B6** để có lại bộ số chuẩn trên máy.

> **Quy ước**
> - `Cmd` là phím ⌘ trên Mac, `Ctrl` là phím Control.
> - "Gõ lệnh" nghĩa là gõ hoặc dán dòng lệnh vào **Terminal** rồi bấm Enter. Từ bước A3 trở đi, dùng Terminal ngay trong VS Code (`Ctrl` + `` ` ``).
> - "Chạy task X" nghĩa là: Cmd + Shift + P → gõ **Tasks: Run Task** → chọn X. Mọi lệnh trong tài liệu đều đã có task tương ứng.
> - Chữ sau dấu `#` trong khối lệnh là chú thích, gõ hay không đều được.

---

## Phần A – Chạy trên máy bằng VS Code (không cần Docker)

Lần đầu mất khoảng 30–40 phút, phần lớn là chờ cài phần mềm và tải 1 GB dữ liệu. Từ lần thứ hai chỉ cần bấm một phím và chờ khoảng 1 phút.
Mỗi bước có mục **"Thấy gì là đúng"**: chưa thấy đúng thì chưa sang bước sau, xem Phần C.4 (xử lý sự cố).

### A1. Cài phần mềm nền (một lần)

1. **Homebrew** (công cụ cài phần mềm cho macOS): mở ứng dụng Terminal (Cmd + Space, gõ "Terminal"), gõ `brew --version`.
   Nếu báo `command not found` thì cài theo trang https://brew.sh, xong đóng và mở lại Terminal.
2. **VS Code**: tải tại https://code.visualstudio.com, kéo vào thư mục Applications, mở lên một lần.
3. **uv** (quản lý Python) và **Java 17** (Spark cần Java):

```bash
brew install uv openjdk@17
```

Không cần chạy lệnh `sudo ln …` mà Homebrew gợi ý sau khi cài Java; dự án tự tìm Java ở `/opt/homebrew/opt/openjdk@17`.

**Thấy gì là đúng:** `uv --version` in ra `uv 0.x…`; `/opt/homebrew/opt/openjdk@17/bin/java -version` in ra `openjdk version "17…"`.

### A2. Lấy thư mục dự án

```bash
cd ~/Desktop
git clone https://github.com/tranhuy99dlk/tyt-danang.git mis5007_tram_y_te
```

Không dùng được GitHub thì xin file nén từ nhóm và giải nén ra Desktop. Thư mục phải có sẵn `data/raw/location-tyt.xlsx`,
`data/raw/danh-sach-cskcb.xlsx` và thư mục `data/reference/`. Những file này do nhóm chuẩn bị, script không tải lại được.

### A3. Mở dự án trong VS Code và cài extension

1. VS Code → **File → Open Folder…** → chọn thư mục dự án → **Open**. Nếu được hỏi "Do you trust the authors…" → **Yes, I trust the authors**.
2. Góc dưới phải hiện "This workspace has extension recommendations" → **Install All**. Không thấy thì bấm Cmd + Shift + X, gõ `@recommended`
   rồi cài cả 5 extension: Python, Pylance, Python Debugger, Jupyter, Rainbow CSV.
3. Mở Terminal trong VS Code: menu **Terminal → New Terminal**.

**Thấy gì là đúng:** cột Explorer bên trái (Cmd + Shift + E) hiện các thư mục `scripts`, `docs`, `data`, `outputs`.

### A4. Tạo môi trường Python cho dự án

```bash
uv venv --python 3.11 .venv                                         # thư mục .venv chứa Python 3.11 riêng cho dự án
uv pip install --python .venv/bin/python -r requirements.lock.txt   # cài đúng phiên bản thư viện nhóm đã chạy (có pyspark)
```

Sau đó Cmd + Shift + P → **Python: Select Interpreter** → chọn dòng có `.venv`.

**Thấy gì là đúng:** lệnh dưới in ra `OK 3.1.1 3.5.10`; góc dưới phải VS Code ghi `3.11.x ('.venv')`.

```bash
.venv/bin/python -c "import pandas, rasterio, shapely, scipy, statsmodels, pyspark; print('OK', pandas.__version__, pyspark.__version__)"
```

### A5. Kiểm tra máy

Chạy task **0 – Kiểm tra máy** (hoặc gõ `bash scripts/0_kiem_tra_may.sh`).

**Thấy gì là đúng:** dòng cuối `KET QUA: du dieu kien.` Gặp dòng `[THIEU]` thì làm theo gợi ý ngay trên dòng đó. Dòng `[LUU Y]` về Docker là bình thường
ở Phần A.

### A6. Tải dữ liệu nguồn (khoảng 1 GB, một lần)

Chạy task **1a – Tải dữ liệu nguồn (~1 GB, một lần)**. Lần thử ngày 26/11/2026 mất khoảng 5 phút.

**Thấy gì là đúng:** gần cuối có `OK=153  FAIL=0`. Nếu dự án lấy từ GitHub, 2 truy vấn OpenStreetMap in `SKIP` vì dữ liệu đã có sẵn.
Có dòng `FAIL` thì chạy lại task: file đã tải xong sẽ được bỏ qua.

### A7. Chạy toàn bộ

Bấm **Cmd + Shift + B** (task mặc định **Chạy toàn bộ (1 thu thập → 2 Spark 5 bước → 3 báo cáo)**). Lần chạy ngày 27/11/2026 mất 53 giây.

Terminal lần lượt in:

| Dòng | Phần | Việc |
|---|---|---|
| `=== 1_thu_thap/1b_chuyen_doi_bronze ===` | 1 · Thu thập | Chuyển đổi raster WorldPop, Excel, bảng tham chiếu sang CSV thô → tầng Bronze |
| `=== 1_thu_thap/1c_tra_cuu_khong_gian ===` | 1 · Thu thập | Ranh giới 56 xã, ô nào thuộc xã nào, độ cao, đồ thị đường → Bronze |
| `=== 2_spark_5_buoc/chay_spark ===` | 2 · Spark | Các dòng `===== BUOC 1 - Doc` … `===== BUOC 5 - Ghi`, cuối cùng là bảng thời gian và số job của từng bước |
| `=== 3_bao_cao/bao_cao ===` | 3 · Báo cáo | Vẽ hình, từ điển dữ liệu, kiểm tra sổ kết quả, chốt số liệu |

**Thấy gì là đúng:**

- `4. Doi chieu NQ 1251: 17 xa gop thanh 8 don vi - code va file tham chieu KHOP`
- `QA: 28 kiem tra, 7 CAN XEM` (7 mục là hạn chế đã biết của dữ liệu, xem C.2)
- `Doi soat: 48 phep, 0 LECH.`
- `Kiem tra run_manifest: 57 file, 57 DAT, 0 CAN XEM`
- Dòng cuối `Xong. Ket qua: outputs/tables (CSV), outputs/figures (PNG), data/lake (bronze/silver/gold)…`

### A8. Xem kết quả trong VS Code

Bảng kết quả nằm trong `outputs/tables/`, chia theo nhóm nội dung của báo cáo:

| Thư mục | Nội dung | Giai đoạn vòng đời |
|---|---|---|
| `1_du_lieu_nguon/` | Danh mục Bronze, bảng Spark đã đọc, QA ranh giới, QA mạng đường, toạ độ 56 xã | 2 · Thu thập |
| `2_chat_luong/` | Số dòng sạch / lỗi / ngoài phạm vi, mẫu dòng bị loại, lệch tổng dân ±3%, kiểm tra trạm và lưới, danh sách trạm và CSKCB đã làm sạch | 3 · Làm sạch |
| `3_mo_ta_kpi/` | Mọi bảng KPI (năm, xã, nhóm, quận, vùng phục vụ trạm) | 5 · Mô tả |
| `4_chan_doan/` | Vùng lõm và nguyên nhân | 5 · Chẩn đoán |
| `5_khuyen_nghi/` | Điểm đặt mới, kịch bản NQ 1251, TYT – CSKCB, ứng viên rà soát gộp, bảng bản đồ trước/sau | 5 · Khuyến nghị |
| `6_du_bao/` | Chuỗi theo xã, backtest, dự báo 2030, tiếp cận 2030 | 5 · Dự báo |
| `7_kiem_chung/` | Đối soát, QA tổng hợp, đo Parquet/CSV, thời gian 5 bước, phạm vi, từ điển dữ liệu, kiểm tra sổ, số liệu chốt | 4 · Quản trị, 7 · Giám sát |
| `luoi_100m/` | Lưới 100 m dạng CSV (dân cư; khoảng cách tới trạm và CSKCB) cho Excel / GIS | 6 · Phục vụ |

| Muốn xem | Mở | Cách xem |
|---|---|---|
| KPI chính | `outputs/tables/3_mo_ta_kpi/kpi_tong_hop_spark.csv` | Bấm vào file: Rainbow CSV tô màu từng cột |
| Bản đồ, biểu đồ | `outputs/figures/h1…h9_*.png` | Bấm vào file, VS Code hiện ảnh |
| Đối soát (mọi dòng phải ĐẠT) | `outputs/tables/7_kiem_chung/doi_soat_spark.csv` | Cột `ket_qua` |
| Thời gian từng bước Spark | `outputs/tables/7_kiem_chung/thoi_gian_5_buoc.csv` | Cột `thoi_gian_giay`, `so_job_spark` |
| Con số dùng trong báo cáo lấy từ file nào | `outputs/tables/7_kiem_chung/so_lieu_chot.csv` | Cột `file`, `cach_lay` |
| Spark thực thi truy vấn thế nào | `logs/plan_khoang_cach_2024.txt`, `logs/plan_pruning_2024.txt` | Cmd + F tìm `Broadcast`, `PartitionFilters` |
| Nhật ký lần chạy | `logs/spark_5_buoc_<giờ>.log` | Mở file mới nhất |
| Hồ dữ liệu | `data/lake/` | Thư mục con chia theo `source_system=…`, `year=…`, `ma_quan=…` |

Bảng "câu hỏi nào xem file nào" đầy đủ ở C.1. Báo cáo theo 7 giai đoạn: [BAO_CAO_7_GIAI_DOAN.md](BAO_CAO_7_GIAI_DOAN.md).

### A9. Chạy từng phần, debug, đọc code

**Chạy riêng từng phần.** Dùng các task **1b**, **1c**, **2 – Pipeline Spark 5 bước**, **3 – Báo cáo**, hoặc mở file rồi bấm F5 và chọn
**Chạy / debug script đang mở**. Thứ tự bắt buộc là `1a` → `1b` → `1c` → `2` → `3`, vì phần sau đọc kết quả của phần trước.
Các file `b1_…` – `b5_…` không chạy riêng được: `2_spark_5_buoc/chay_spark.py` gọi lần lượt từng file.

**Đặt điểm dừng (breakpoint):**
1. Mở `scripts/2_spark_5_buoc/b4_tong_hop.py`, tìm dòng `kq = pd.DataFrame(kq)  # bảng KPI tổng hợp` (Cmd + F).
2. Bấm vào khoảng trống bên trái số dòng: hiện chấm đỏ.
3. Cmd + Shift + D → chọn **Debug pipeline Spark 5 bước** → F5.
4. Khi dừng: khung **Variables** hiện biến `kq`, `g1`, `g3`…; trong **Debug Console** gõ `kq.head(10)`.
5. F10 chạy từng dòng, F5 chạy tiếp tới hết, Shift + F5 dừng hẳn.

Điểm dừng đặt bên trong hàm chạy trên executor (UDF `norm_name`, `utm49n_cap`; `backtest_mot_xa`, `du_bao_mot_xa` của `applyInPandas`)
sẽ không dừng, vì Spark chạy các hàm đó ở tiến trình con.

**Thứ tự đọc code** (mỗi dòng đều có chú thích tiếng Việt ở cuối dòng):

| Thứ tự | File | Đọc để hiểu |
|---|---|---|
| 1 | `scripts/chung/common.py` | Mọi tham số (3 km, năm 2015–2024, hộp bao…), bảng `THU_MUC_BANG`, hàm dùng chung |
| 2 | `scripts/1_thu_thap/1b_chuyen_doi_bronze.py` | Chuyển đổi đa nguồn vào Bronze, vì sao giữ nguyên giá trị |
| 3 | `scripts/1_thu_thap/1c_tra_cuu_khong_gian.py` | Điểm trong đa giác, độ cao, đồ thị đường |
| 4 | `scripts/2_spark_5_buoc/chay_spark.py` | 5 bước nối tiếp, mỗi bước là một khối `with nc.buoc(...)` |
| 5 | `scripts/2_spark_5_buoc/spark_chung.py` | SparkSession, ghi/đọc Parquet an toàn, hàng chờ kết quả, đo thời gian |
| 6 | `b1_doc.py` → `b2_kiem_tra_lam_sach.py` → `b3_bien_doi.py` → `b4_tong_hop.py` (+ `b4_khuyen_nghi.py`, `b4_du_bao.py`) → `b5_ghi.py` | 5 bước Đọc → Kiểm tra & làm sạch → Biến đổi → Tổng hợp → Ghi |
| 7 | `scripts/3_bao_cao/bao_cao.py` | Vẽ hình, từ điển dữ liệu, chốt số |

### A10. Sửa đầu vào, đổi tham số rồi chạy lại

Sau mọi thay đổi: bấm Cmd + Shift + B (chạy lại toàn bộ khoảng 1 phút, chắc chắn hơn chạy lẻ). VS Code mở `data/raw/` và `data/reference/`
ở chế độ chỉ đọc để tránh sửa nhầm; sửa file Excel bằng Excel hoặc Numbers.

| Việc | Làm thế nào | Đọc gì sau khi chạy |
|---|---|---|
| Sửa toạ độ một trạm | `data/raw/location-tyt.xlsx`, cột thứ ba dạng `vĩ độ, kinh độ` (ví dụ `16.0500, 108.1797`); giữ tên xã ở cột thứ hai | `2_chat_luong/spark_quarantine_tong_hop.csv` (nguồn `tyt_raw`), `2_chat_luong/qa_stations.csv`. Toạ độ sai mẫu / ngoài hộp bao vào quarantine; trạm đó dùng tâm dân cư xã (A5) |
| Cập nhật danh sách CSKCB | `data/raw/danh-sach-cskcb.xlsx`, sheet đầu; cột `STT`, `Mã`, `Tên`, `Tuyến CMKT`, `Hạng bệnh viện`, `Cấp CMKT` (`Cấp ban đầu` / `Cấp cơ bản` / `Cấp chuyên sâu`), `Điểm`, `Địa chỉ`, `tọa độ` | `2_chat_luong/cskcb_danang.csv`; ghi thay đổi vào A17 trong `CLAUDE.md` |
| Đổi ngưỡng rà soát gộp trạm (A15) | Hằng số `A15_NGUONG`, `MIN_KCB_1KM`, `MAX_TYT_GAN_M` ở đầu `scripts/2_spark_5_buoc/b4_khuyen_nghi.py` | `5_khuyen_nghi/ung_vien_gop_tram.csv`; đổi ngưỡng để "khớp NQ 1251 hơn" làm p-value càng lạc quan, phải ghi vào A15 |
| Đổi ngưỡng khoảng cách, năm, tốc độ | `scripts/chung/common.py`: `MAIN_KM` (3), `THRESH_KM` (2, 3, 5), `WHITE_KM` (5), `TIME_MIN` / `TIME_LOM_MIN` (15 / 20), `YEARS` / `BASE_YEAR` (2015–2024 / 2024), `PROJ_YEARS` / `FORECAST_YEAR`, `POP_TOL_CANH_BAO_PCT` / `POP_TOL_A11_PCT` (3 / 20), `SPEED_KMH`, `WALK_KMH`, `BBOX`, `CRS_M` | Đổi `YEARS` phải sửa dòng `YRS=` trong `1a_tai_du_lieu.sh` và tải thêm; đổi `BASE_YEAR` làm đổi tên cột (`pop_2024`); ghi giả định vào `CLAUDE.md` |
| Thêm một quy tắc kiểm tra | Trong `b2_kiem_tra_lam_sach.py`, chèn cặp `("MA_LY_DO", điều_kiện_vi_phạm)` vào lời gọi `tach(...)` của nguồn cần thêm. Mã chỉ đánh dấu "ngoài phạm vi" (không phải lỗi) thì thêm vào `MA_NGOAI_PHAM_VI` | `2_chat_luong/spark_quarantine_tong_hop.csv` |
| Thêm một bảng kết quả mới | Thêm tên file vào `THU_MUC_BANG` (`common.py`); trong Spark Bước 1–4 gửi vào hàng chờ bằng `nc.xuat("ten.csv", df, S, note=…)`, Bước 5 tự ghi | `outputs/run_manifest.csv` có dòng mới |

---

## Phần B – Chạy có Docker (HDFS + YARN + JupyterLab, tuỳ chọn)

Cụm gồm 5 container: NameNode, DataNode, ResourceManager, NodeManager và JupyterLab có PySpark. Cùng một code Spark 5 bước như Phần A;
chỉ khác hai biến môi trường: `HDFS_URI` (đọc/ghi HDFS) và `SPARK_MASTER=yarn` (executor chạy trên YARN).

### B1. Chuẩn bị Docker Desktop (một lần)

1. Tải Docker Desktop tại https://www.docker.com/products/docker-desktop/, mở lên và chờ biểu tượng cá voi báo "running".
2. Docker Desktop → **Settings → Resources → Memory**: đặt 10–12 GB nếu máy có đủ RAM. Lý do: với máy ảo 7,7 GB, cả cụm đã dùng tới 7.387 MiB
   khi chạy YARN (đo 26/11/2026), quá sát giới hạn.
3. Trên MacBook chip Apple (M1–M5): 4 image Hadoop chỉ có bản amd64 nên Docker chạy qua giả lập, chậm hơn nhưng vẫn chạy được.
   Image JupyterLab có bản arm64.

### B2. Bật cụm

Chạy task **Docker 1 – Bật cụm HDFS + YARN + JupyterLab**. Lần đầu phải tải 5 image (khoảng 9 GB).

**Thấy gì là đúng:** sau khoảng 30–40 giây, chạy lại `docker compose -f docker/docker-compose.yml ps`: cả 5 dòng có `(healthy)`.

### B3. Đưa Bronze lên HDFS

Phải chạy Phần A ít nhất một lần trước bước này, để đã có `data/lake/bronze`. Sau đó chạy task **Docker 2 – Đưa Bronze lên HDFS**.

**Thấy gì là đúng:** dòng cuối `Da ghi logs/hdfs_yarn_cli_truoc_<giờ>.log`; trên http://localhost:9871 → Utilities → Browse the file system có `/mis5007/bronze`.

### B4. Chạy pipeline Spark 5 bước trên cụm

| Cách | Task | Thời gian | Ghi chú |
|---|---|---|---|
| **B4a – trên HDFS** (Spark `local[*]` trong container) | **Docker 3a – Chạy Spark 5 bước trên HDFS** | khoảng 1,5 phút | Đọc Bronze từ HDFS, ghi Silver / Gold lên HDFS |
| **B4b – trên YARN** (executor trên NodeManager) | Lần đầu: **Docker 3b – Đóng gói Python cho YARN** (khoảng 1 phút, gói 247 MB). Sau đó: **Docker 3c – Chạy Spark 5 bước trên YARN** | khoảng 3–5 phút | NodeManager không có Python nên phải gửi kèm gói Python amd64 |

**Thấy gì là đúng:**
- Log in đủ 5 khối `===== BUOC 1` … `BUOC 5` và dòng `Doi soat: 47 phep, 0 LECH.` Có 47 thay vì 48 phép vì phép so với pyproj tự bỏ: container không có pyproj.
- Riêng YARN: dòng đầu log có `master=yarn`. Lần chạy lại ngày 27/11/2026 sau khi đổi tên: HDFS 77 giây, YARN 171 giây, cả hai 47/47 ĐẠT, ứng dụng
  `FINISHED` / `SUCCEEDED` (`logs/spark_5_buoc_20261127T005538Z.log`, `logs/spark_5_buoc_20261127T005707Z.log`).

### B5. Xem kết quả trên cụm

Chạy task **Docker 4 – Liệt kê HDFS, fsck, ứng dụng YARN**. Kết quả ghi vào `logs/hdfs_yarn_cli_sau_<giờ>.log`; phải có `Status: HEALTHY`.

| Giao diện | Địa chỉ | Xem gì |
|---|---|---|
| HDFS NameNode | http://localhost:9871 | Utilities → Browse the file system → `/mis5007/bronze`, `silver`, `gold` |
| YARN ResourceManager | http://localhost:8088 | Applications: `mis5007_tram_y_te` `FINISHED` / `SUCCEEDED`; Nodes |
| JupyterLab | http://localhost:8888 | Mật khẩu ở dòng `JUPYTER_TOKEN` trong `docker/docker-compose.yml`; mở `work/mis5007_tram_y_te/notebooks/pyspark_hdfs_5_buoc.ipynb` |
| Spark UI | http://localhost:4041 | Chỉ có khi đang có job Spark chạy trong container |

Mở notebook ngay trong VS Code: mở `notebooks/pyspark_hdfs_5_buoc.ipynb` → **Select Kernel** → **Existing Jupyter Server** →
nhập `http://localhost:8888/?token=` kèm giá trị `JUPYTER_TOKEN`.

Danh sách ứng dụng trên YARN chỉ còn khi cụm đang bật. Dừng cụm là mất danh sách này, nên chụp màn hình nếu cần làm minh chứng.

### B6. Dừng cụm và lấy lại bộ số chuẩn trên máy (bắt buộc)

1. Chạy task **Docker 5 – Dừng cụm (giữ dữ liệu HDFS)**.
2. Bấm **Cmd + Shift + B** để chạy lại Phần A.

Lý do: khi chạy trên cụm, các bảng trong `outputs/tables/` là kết quả từ container, nơi thư viện khác phiên bản (dự báo 2030 lệch khoảng 7 người).
Báo cáo phải dùng bộ số chạy trên máy.

Quản lý cụm: `docker compose -f docker/docker-compose.yml ps | stop | start`. Lệnh `down -v` gỡ hẳn cụm và **xoá toàn bộ dữ liệu HDFS**.

---

## Phần C – Tra cứu

### C.1. Câu hỏi nào, xem file nào

Đường dẫn tính từ `outputs/tables/`.

| Câu hỏi | File | Cột / dòng cần đọc |
|---|---|---|
| Bao nhiêu % dân trong 3 km tới trạm? Theo đường đi, 15 phút? | `3_mo_ta_kpi/kpi_tong_hop_spark.csv` | dòng loại "KPI chính" |
| Dân bình quân mỗi trạm, p50, p95, dân > 5 km, người 60+, > 20 phút, CSKCB | `3_mo_ta_kpi/kpi_tong_hop_spark.csv` | dòng loại "KPI phụ", "KPI phụ (mới)" |
| Phường và xã miền núi chênh bao nhiêu? | `3_mo_ta_kpi/kpi_theo_nhom_spark.csv` | `ty_le_trong_3km_pct` theo `nhom` |
| So sánh 7 quận/huyện; xã nào yếu nhất? | `3_mo_ta_kpi/kpi_theo_quan_spark.csv`, `3_mo_ta_kpi/kpi_theo_xa_spark.csv` | sắp theo `ty_le_trong_3km_mang_duong_pct` |
| Thay đổi qua các năm 2015–2024 | `3_mo_ta_kpi/kpi_theo_nam_spark.csv`, `3_mo_ta_kpi/kpi_theo_xa_nam_spark.csv` | mỗi dòng một năm |
| Mỗi trạm phục vụ bao nhiêu dân? | `3_mo_ta_kpi/tram_vung_phuc_vu_spark.csv` | `dan_phuc_vu` |
| Dân ở xa tập trung ở đâu, vì sao? | `4_chan_doan/white_zones.csv`, hình h1, h2 | `loai_vung`, `nguyen_nhan` |
| Nên đặt thêm điểm ở đâu, được bao nhiêu? | `5_khuyen_nghi/greedy_sites.csv`, `5_khuyen_nghi/greedy_before_after.csv`, `5_khuyen_nghi/ban_do_truoc_sau_de_xuat.csv`, hình h7, h9 | `thu_tu`, `dan_moi_duoc_phu`, `ty_le_3km` theo `K` |
| Sáp nhập 56 → 47 trạm ảnh hưởng thế nào? | `5_khuyen_nghi/scenario_nq1251.csv` | 3 kịch bản |
| Trạm nào cần rà soát gộp? Quy tắc khớp NQ 1251 tới đâu? | `5_khuyen_nghi/ung_vien_gop_tram.csv`, `5_khuyen_nghi/doi_soat_nq1251_quy_tac.csv`, `5_khuyen_nghi/doi_soat_nq1251_theo_xa.csv` | `trang_thai_nq1251`; `dung_TP`, `bao_dong_gia_FP`, `bo_sot_FN` |
| Quanh mỗi TYT có bao nhiêu CSKCB? | `5_khuyen_nghi/tyt_cskcb.csv`, `5_khuyen_nghi/tyt_cskcb_theo_nhom.csv` | `cskcb_2km`, `kc_cskcb_gan_nhat_m` |
| Dự báo 2030; mô hình nào, sai số bao nhiêu; xã nào giảm tiếp cận? | `6_du_bao/forecast_total_2030.csv`, `6_du_bao/forecast_commune_2030.csv`, `6_du_bao/forecast_model_choice.csv`, `6_du_bao/access_2030_change.csv`, hình h5 | `dan_2030`, `dan_2030_lo80`, `dan_2030_hi80`; `MAPE_pct`; `giam_tiep_can` |
| Trạm có đặt đúng chỗ không? | `2_chat_luong/tram_y_te_56.csv`, `2_chat_luong/qa_stations.csv` | `kc_tram_tam_dan_cu_m`, `trong_ranh_gioi_xa` |
| Dữ liệu nào đã đọc, bao nhiêu dòng? | `1_du_lieu_nguon/bronze_danh_muc.csv`, `1_du_lieu_nguon/spark_bang_da_doc.csv` | `so_dong_doc` |
| Phạm vi Đà Nẵng cũ, 56 xã/phường | `7_kiem_chung/pham_vi_da_nang_cu.csv`, `1_du_lieu_nguon/xa_phuong_56_toa_do.csv`, hình h8 | khung toạ độ, số trên bản đồ |
| Lưới từng ô 100 m | `luoi_100m/dan_cu_luoi_100m.csv`, `luoi_100m/tiep_can_luoi_100m.csv` | ý nghĩa cột ở `7_kiem_chung/tu_dien_du_lieu.csv` |
| Parquet khác CSV thế nào khi quét? | `7_kiem_chung/so_sanh_parquet_csv.csv`, `logs/plan_pruning_2024.txt` | `dung_luong_file_quet_byte`, `partition_filters` |
| Bước Spark nào tốn thời gian nhất? | `7_kiem_chung/thoi_gian_5_buoc.csv` | `thoi_gian_giay`, `so_job_spark` |
| Con số nào dùng trong báo cáo, lấy từ đâu? | `7_kiem_chung/so_lieu_chot.csv` | `file`, `cach_lay`, `sha256_file` |

Bảng Parquet tương ứng nằm trong `data/lake/gold/`.

### C.2. Đọc kết quả kiểm tra trước khi dùng số

| Bảng | Đọc cột | Bình thường | Phải xử lý khi |
|---|---|---|---|
| `7_kiem_chung/doi_soat_spark.csv` | `ket_qua` | Mọi dòng `ĐẠT` (48 phép) | Có dòng `LỆCH` |
| `7_kiem_chung/qa_summary.csv` | `dat` | 7 dòng `CẦN XEM` đã biết (dưới đây) | Xuất hiện `CẦN XEM` mới |
| `2_chat_luong/spark_so_dong_theo_nguon.csv` | `so_dong_loi_du_lieu`, `so_dong_ngoai_pham_vi` | Lỗi thật chỉ 9 dòng ở `tyt_raw` (dòng tiêu đề, tên quận trong Excel); ngoài phạm vi rất nhiều ở pixel dân số (NoData, ngoài 56 xã) | Lỗi xuất hiện ở nguồn khác, hoặc `tyt_raw` còn dưới 56 dòng sạch |

Bảy mục `CẦN XEM` đã biết, đều là hạn chế của dữ liệu:
- 8 trạm nằm ngoài ranh giới xã của chính nó, xa nhất 104 m, nhiều khả năng do ranh giới GADM thô.
- 1 trạm cách tâm dân cư xã trên 3 km: Trạm Y tế xã Hòa Ninh (3.150 m).
- 8 phường có ranh giới OSM khác GADM (IoU < 0,7).
- WorldPop lệch số liệu NQ 1251 khoảng 21% ở cả năm 2023 và 2024. Mục này chiếm 2 dòng `CẦN XEM`.
- Quy tắc ±3% chỉ 2/17 bản ghi đạt ở cả năm 2023 và 2024. Mục này cũng chiếm 2 dòng `CẦN XEM`, và chỉ là **cảnh báo**, không phải lỗi.

### C.3. Biến môi trường

| Biến | Script đọc | Mặc định | Tác dụng |
|---|---|---|---|
| `PAR` | `1a_tai_du_lieu.sh` | `4` | Số luồng tải song song |
| `PYTHON` | `chay_toan_bo.sh` | `.venv/bin/python` | Trình thông dịch Python |
| `JAVA_HOME` | `spark_chung.py` | tự tìm `/opt/homebrew/opt/openjdk@17` | Java cho Spark |
| `HDFS_URI` | `spark_chung.py` | không đặt | Đặt thì Bronze / Silver / Gold nằm trên HDFS (Phần B) |
| `SPARK_MASTER` | `spark_chung.py` | `local[*]` | `yarn` để chạy executor trên YARN (cần `HDFS_URI`) |
| `SPARK_DRIVER_MEMORY` | `spark_chung.py` | `4g` (`2g` khi YARN) | Bộ nhớ driver |
| `SPARK_SHUFFLE_PARTITIONS` | `spark_chung.py` | `16` | Số phân vùng khi shuffle |
| `SPARK_UI` | `spark_chung.py` | `false` | `true` bật Spark UI (http://localhost:4040 trên máy) |
| `SPARK_PARTITION_OVERWRITE` | `spark_chung.py` | `static` | `dynamic` chỉ ghi đè phân vùng có trong dữ liệu mới |
| `HDFS_REPLICATION` | `spark_chung.py` | `1` | Hệ số nhân bản khi ghi HDFS |
| `SPARK_EXECUTOR_INSTANCES`, `SPARK_EXECUTOR_CORES`, `SPARK_EXECUTOR_MEMORY`, `YARN_PY_ARCHIVE` | `spark_chung.py` | `2`, `2`, `1g`, gói trên HDFS | Tài nguyên executor khi chạy YARN |
| `K_MAP` | `bao_cao.py` | `5` | Số điểm đề xuất vẽ trên hình h9 |
| `HDFS_DIR`, `NN`, `RM`, `NB`, `REBUILD` | `hdfs_yarn_cli.sh`, `yarn_python_env.sh` | `/mis5007`, tên container, `0` | Thư mục gốc trên HDFS, tên container, `REBUILD=1` để đóng gói lại Python |

### C.4. Xử lý sự cố

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `uv: command not found` sau khi cài | Terminal cũ chưa nhận đường dẫn | Đóng và mở lại Terminal / VS Code |
| Góc dưới phải không ghi `.venv`; `ModuleNotFoundError` | Chưa chọn trình thông dịch hoặc chưa cài thư viện | Cmd + Shift + P → **Python: Select Interpreter** → `.venv`; chạy lại lệnh cài ở A4 |
| `Unable to locate a Java Runtime`, `JAVA_GATEWAY_EXITED` | Chưa cài Java 17 | `brew install openjdk@17` |
| Máy Mac chip Intel báo không thấy Java | Java nằm ở chỗ khác | Sửa `JAVA_HOME` trong `.vscode/settings.json`, `tasks.json`, `launch.json` thành `/usr/local/opt/openjdk@17` |
| `1a_tai_du_lieu.sh` báo `FAIL` | Mạng chập chờn, máy chủ bận | Chạy lại task; file đã tải được bỏ qua |
| `FileNotFoundError: Chua co Bronze …` | Chạy Spark khi chưa chạy `1b`, `1c` | Cmd + Shift + B |
| `Python in worker has different version` | Spark gọi nhầm Python hệ thống | Chạy bằng task hoặc F5 (đã dùng `.venv`) |
| `RuntimeError: Thieu …/_SUCCESS` | Lần ghi Parquet trước bị ngắt giữa chừng | Chạy lại task **2 – Pipeline Spark 5 bước** |
| Spark báo `Java heap space` | Driver thiếu bộ nhớ | Đặt `SPARK_DRIVER_MEMORY=6g` trước lệnh chạy |
| `doi_soat_spark.csv` có `LỆCH` ở phần "So voi 19/11/2026" | Đầu vào đã đổi so với lần chốt | Bình thường nếu chủ động sửa toạ độ, CSKCB, ngưỡng: ghi lý do vào báo cáo; không sửa gì thì đọc log |
| `doi_soat_spark.csv` có `LỆCH` ở phần bảo toàn | Lỗi trong pipeline | Đọc log mới nhất `logs/spark_5_buoc_*.log` |
| F5 không chạy | Thiếu extension | Cài **Python Debugger** |
| `docker: Cannot connect to the Docker daemon` | Docker Desktop chưa mở | Mở Docker Desktop, chờ "running" |
| `port is already allocated` | Cổng bị chương trình khác dùng | Đổi số bên trái dấu `:` trong `docker/docker-compose.yml` |
| Trên HDFS báo `Path does not exist: …/bronze/…` | Chưa đưa Bronze lên HDFS | Task **Docker 2** |
| `SPARK_MASTER=yarn` báo không thấy `pyspark_env.tar.gz` | Chưa đóng gói Python | Task **Docker 3b** |
| `SystemExit: SPARK_MASTER=yarn can HDFS_URI` | YARN cần dữ liệu trên HDFS | Dùng task **Docker 3c** (đã có `HDFS_URI`) |
| `Lost executor … Exit status: 137` | Máy ảo Docker hết RAM, kernel giết executor (Spark tự chạy lại) | Giữ `SPARK_EXECUTOR_MEMORY=1g`; tăng Memory cho Docker Desktop (B1) |
| Executor YARN lỗi `./pyenv/bin/python` hoặc import numpy | Gói Python cũ | `REBUILD=1 bash scripts/cum_hdfs_yarn/yarn_python_env.sh` |
| Ổ đĩa đầy dần | Mỗi ngày chạy `1b` lại thêm một lần nạp Bronze (khoảng 160 MB, thư mục `ingestion_date=<ngày>`) | Spark chỉ đọc lần nạp mới nhất; có thể xoá các thư mục `ingestion_date` cũ trong `data/lake/bronze/source_system=*/` |

Không tìm ra nguyên nhân: mở log mới nhất của phần bị lỗi trong `logs/`.

### C.5. Khi đưa số liệu vào báo cáo

- Mỗi con số phải dẫn về một file trong `outputs/tables/` hoặc `logs/`; `7_kiem_chung/so_lieu_chot.csv` đã ghi sẵn file và cách lấy. Chưa có file thì ghi "chưa có số".
- Lỗi dữ liệu là 9 dòng; 3.030.871 dòng còn lại là **ngoài phạm vi** (NoData, ngoài 56 xã), không phải lỗi (`2_chat_luong/spark_quarantine_tong_hop.csv`).
- WorldPop là ước tính mô hình. Quy tắc ±3% chỉ là cảnh báo. Số sau 2024 là **dự báo**, luôn kèm khoảng 80% (A9).
- Ngưỡng 3 km là giả định của nhóm (A1), luôn kèm 2 km, 5 km, đường đi, 15 phút; 20 phút là A18.
- `ung_vien_gop_tram.csv` là danh sách **rà soát**, không phải đề nghị đóng trạm (A15, A16). Số đếm CSKCB là sàn dưới (A17).
- Không dẫn "tăng tốc 10–15 lần", "tiết kiệm 70% I/O" (chưa từng đo); dẫn `7_kiem_chung/so_sanh_parquet_csv.csv`.
- Ghi giấy phép: WorldPop CC BY 4.0; "© OpenStreetMap contributors" (ODbL 1.0); GADM chỉ dùng học thuật, không phân phối lại; AWS Terrain Tiles ghi nguồn.

## Danh sách tự kiểm tra

- [ ] Góc dưới phải VS Code ghi `3.11.x ('.venv')`.
- [ ] Task **0 – Kiểm tra máy** báo `KET QUA: du dieu kien.`
- [ ] Cmd + Shift + B chạy xong, có `Doi soat: 48 phep, 0 LECH.` và `Kiem tra run_manifest: 57 file, 57 DAT`.
- [ ] `3_mo_ta_kpi/kpi_tong_hop_spark.csv`: tổng dân 2024 = 1.177.774; trong 3 km = 98,69%; theo mạng đường = 96,10%; trong 15 phút = 99,71%.
- [ ] `outputs/figures/` có h1–h5, h7–h9.
- [ ] (Nếu làm Phần B) đã dừng cụm và chạy lại Cmd + Shift + B.
