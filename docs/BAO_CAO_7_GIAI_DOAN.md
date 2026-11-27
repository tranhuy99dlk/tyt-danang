# Báo cáo phân tích theo 7 giai đoạn vòng đời phân tích dữ liệu lớn

**Đề tài:** Tiếp cận trạm y tế xã/phường Đà Nẵng cũ (56 xã/phường, bản đồ hành chính trước 01/01/2025)
**Học phần:** MIS5007 – Dữ liệu lớn và Ứng dụng · **Người đọc chính:** Ban lãnh đạo Sở Y tế

Quy ước trong báo cáo:
- **Dữ liệu**: con số có trích file nguồn trong ngoặc, ví dụ (`3_mo_ta_kpi/kpi_tong_hop_spark.csv`). Bảng nằm ở `outputs/tables/<thư mục>/`.
- **Giả định**: mang mã A1–A18 (bảng đầy đủ ở [CLAUDE.md](../CLAUDE.md)).
- **Đánh giá của nhóm**: nhận định không đo được bằng dữ liệu, ghi rõ là đánh giá.
- WorldPop là **ước tính mô hình**, không phải số đếm đầu người. Số sau 2024 là **dự báo**, luôn kèm khoảng 80%.

Phần kỹ thuật chi tiết (kiến trúc, lý do từng lựa chọn): [KIEN_TRUC.md](KIEN_TRUC.md). Cách chạy: [HUONG_DAN_CHAY.md](HUONG_DAN_CHAY.md). Demo: [HUONG_DAN_DEMO.md](HUONG_DAN_DEMO.md).

---

## Giai đoạn 1 · Xác định vấn đề và mục tiêu

### 1.1 Bài toán nghiệp vụ

Từ 01/01/2025, Nghị quyết 1251/NQ-UBTVQH15 sắp xếp 56 xã/phường của Đà Nẵng cũ thành 47 đơn vị. 17 xã được gộp thành 8 đơn vị mới
(`data/reference/nq1251_sap_xep.csv`). Mỗi xã/phường có một trạm y tế (TYT), nên câu hỏi đặt ra là: mạng lưới 56 trạm đang phục vụ
dân đến đâu, và nên điều chỉnh thế nào. Câu hỏi quản lý được tách thành bốn câu:

| Câu hỏi | Loại phân tích | Trả lời ở |
|---|---|---|
| Bao nhiêu % dân sống trong 3 km tới trạm gần nhất? | Mô tả | Giai đoạn 5, KPI |
| Ai ở ngoài, ở đâu? | Mô tả + chẩn đoán | vùng lõm, KPI theo xã |
| Vì sao họ ở xa (địa hình hay dân cư phân tán)? | Chẩn đoán | `4_chan_doan/white_zones.csv` |
| Đặt thêm trạm ở đâu, trạm nào cần rà soát gộp, đến 2030 thay đổi gì? | Khuyến nghị + dự báo | `5_khuyen_nghi/`, `6_du_bao/` |

### 1.2 Bên liên quan

| Bên liên quan | Vai trò với kết quả | Nguồn xác định |
|---|---|---|
| Ban lãnh đạo Sở Y tế | Người ra quyết định: đặt thêm trạm, rà soát gộp trạm, ưu tiên đầu tư | đề bài học phần |
| Trạm y tế và UBND xã/phường | Bị tác động trực tiếp khi gộp hoặc đặt thêm trạm | đánh giá của nhóm |
| Người dân, đặc biệt người 60+ | Đối tượng hưởng lợi; nhóm 60+ được tính riêng (A3) | đánh giá của nhóm |
| Nhóm phân tích | Vận hành pipeline, cập nhật số hằng năm | đề bài học phần |

### 1.3 Quy trình hiện tại, điểm nghẽn và cơ hội

- **Quy trình hiện tại của Sở Y tế:** dự án **chưa có tài liệu** mô tả Sở đang lập kế hoạch mạng lưới trạm thế nào. Đây là câu hỏi mở cần hỏi Sở,
  nhóm không tự giả định.
- **Điểm nghẽn thấy được từ dữ liệu:**
  - Không có lưới dân số chính thức theo ô. Số liệu dân số chính thức chỉ có theo đơn vị hành chính (`data/reference/dan_so_chinh_thuc_nq1251.csv`),
    nên không đo được khoảng cách từ nhà dân tới trạm.
  - Ranh giới hành chính đang đổi: ranh giới OSM mốc 31/12/2024 đã vẽ theo bản đồ sau sáp nhập, chỉ khớp 45/56 xã. Nhóm phải dùng GADM 4.1 (A12, `1_du_lieu_nguon/qa_boundaries.csv`).
  - Toạ độ trạm là file nhập tay: 65 dòng, trong đó 9 dòng là tiêu đề / tên quận (`2_chat_luong/spark_so_dong_theo_nguon.csv`).
- **Cơ hội:** dữ liệu mở đủ chi tiết đã có sẵn: WorldPop lưới 100 m cho 10 năm, mạng đường OSM, độ cao. Thời điểm sắp xếp hành chính là lúc cần
  đánh giá lại mạng lưới.

### 1.4 Quyết định hành động và KPI

| Quyết định cụ thể | KPI dùng để quyết định | Baseline năm 2024 (dữ liệu) |
|---|---|---|
| Có cần đặt thêm trạm không, đặt ở đâu | % dân trong 3 km đường chim bay (KPI chính, A1) | **98,69%** |
| | % dân trong 3 km theo mạng đường | 96,10% |
| | % dân tới trạm trong 15 phút xe máy | 99,71% |
| | Dân cách trạm > 5 km (vùng trắng) | 2.624 người |
| | Dân cần > 20 phút xe máy (vùng lõm, A18) | 1.790 người |
| Ưu tiên địa bàn nào | Chênh lệch phường – xã miền núi (3 km, chim bay) | 51,84 điểm % |
| Rà soát gộp trạm | Số TYT có ≥ 6 CSKCB trong 2 km (A15) | 17/56 trạm |
| Chuẩn bị cho 2030 | % dân trong 3 km năm 2030 nếu giữ 56 trạm (**dự báo**) | 98,69% |

Nguồn: `3_mo_ta_kpi/kpi_tong_hop_spark.csv`, `5_khuyen_nghi/ung_vien_gop_tram.csv`, `6_du_bao/access_2030_total.csv`.

**Mức mục tiêu:** không có văn bản quy định "x% dân trong 3 km", và ngưỡng 3 km là giả định nhóm chốt (A1). Vì vậy báo cáo không đặt mục tiêu
cứng. Báo cáo đưa ra **các mức tham khảo đã tính được**: thêm 5 trạm thì 99,53%, thêm 10 trạm thì 99,82% (`5_khuyen_nghi/greedy_before_after.csv`).

---

## Giai đoạn 2 · Xác định và thu thập dữ liệu

### 2.1 Danh mục dữ liệu (data inventory)

| Nguồn | Loại theo khung | Định dạng gốc | Khối lượng (dữ liệu) | Tần suất cập nhật | Giấy phép / quyền truy cập |
|---|---|---|---|---|---|
| WorldPop R2025A 100 m (S1) | Bên ngoài, có cấu trúc (raster) | GeoTIFF | 11 file; 3.006.720 pixel × năm | Hằng năm | CC BY 4.0, tải tự do |
| WorldPop R2025A 1 km: tổng dân + nhóm tuổi | Bên ngoài, raster | GeoTIFF | 16 + 112 file; 454.656 dòng | Hằng năm, có số chiếu 2025–2030 | CC BY 4.0 |
| OpenStreetMap qua Overpass (S2): ranh giới, mạng đường | Bên ngoài, bán cấu trúc | JSON | 236.981 nút, 253.466 cạnh đồ thị | Liên tục (cộng đồng sửa) | ODbL 1.0 |
| GADM 4.1 cấp 3 | Bên ngoài, vector | GeoJSON | 56 đa giác dùng | Theo phiên bản | Chỉ học thuật, không phân phối lại |
| AWS Terrain Tiles z11 | Bên ngoài, raster | PNG mã hoá độ cao | 12 ô | Gần như cố định | Ghi nguồn |
| Toạ độ 56 trạm (`location-tyt.xlsx`) | "Nội bộ" – nhóm nhập tay theo Cổng dữ liệu Đà Nẵng (A13) | Excel | 65 dòng | Khi trạm đổi vị trí | Giấy phép cổng dữ liệu chưa xác nhận |
| Danh sách 30 CSKCB (`danh-sach-cskcb.xlsx`) | Do người dùng cung cấp, nguồn Sở Y tế (A17) | Excel | 30 dòng | Không rõ | Chưa đối chiếu công bố gốc |
| Danh mục 56 trạm, dân số NQ 1251, 8 đơn vị gộp, 15 gói dịch vụ TT 30/2024 | Tham chiếu | CSV, PDF | 56 / 17 / 11 / 15 dòng | Theo văn bản | Văn bản nhà nước |
| Nguồn nội bộ dạng giao dịch, CRM, log (ví dụ lượt khám tại trạm) | Nội bộ | – | **không có** | – | Chưa được cung cấp |
| Dữ liệu dòng (sự kiện, cảm biến) | Dòng | – | **không có** | – | Bài toán không có nguồn này |

Nguồn số liệu: `data/manifest_raw.csv` (158 file tải về, 1.066 MB, có URL, SHA-256, giờ tải, giấy phép) và
`1_du_lieu_nguon/spark_bang_da_doc.csv` (số dòng Spark đọc từ mỗi bảng Bronze).

### 2.2 Đánh giá nguồn dữ liệu

Các ô dưới đây là **đánh giá của nhóm**, trừ chỗ có trích file.

| Nguồn | Liên quan | Truy cập | Chất lượng | Nhạy cảm | Chi phí thu nhận |
|---|---|---|---|---|---|
| WorldPop 100 m | Cao: là mẫu số (dân) của mọi KPI | Mở | Ước tính mô hình, lệch số liệu NQ 1251 khoảng 21% (MAPE, `2_chat_luong/qa_official_population.csv`) | Thấp: ô 100 m, không có cá nhân | Thấp: tải tự động |
| OSM mạng đường | Cao: khoảng cách theo đường | Mở | Có cả cao tốc dù xe máy không được đi (A6); 97,54% nút thuộc thành phần liên thông lớn nhất (`1_du_lieu_nguon/qa_network.csv`) | Thấp | Thấp |
| GADM | Trung bình: chỉ để gán ô vào xã | Mở, học thuật | Ranh giới thô: 8 trạm nằm ngoài ranh giới xã của chính nó (`7_kiem_chung/qa_summary.csv`) | Thấp | Thấp |
| Toạ độ trạm nhập tay | Cao: vị trí cung cấp dịch vụ | Nhóm tự nhập | 56/56 trạm có toạ độ hợp lệ; chưa kiểm chứng thực địa (A13) | Thấp | Trung bình: công nhập tay |
| CSKCB | Trung bình: chỉ dùng cho A15 | Người dùng cung cấp | Thiếu phòng khám tư nhỏ, nên số đếm là sàn dưới (A17) | Thấp | Thấp |
| Lượt khám tại trạm (không có) | Rất cao: đo nhu cầu thật | Phải xin Sở | – | Cao: dữ liệu sức khoẻ cá nhân | Cao: thủ tục, ẩn danh hoá |

### 2.3 Phương pháp chuyển đổi dữ liệu nhiều nguồn: đã làm gì, có thông tin gì, vì sao

**Vấn đề:** dữ liệu đến từ 5 loại định dạng khác nhau (GeoTIFF, JSON Overpass, GeoJSON, PNG độ cao, Excel), nhưng Spark chỉ đọc tốt CSV, JSON và Parquet.

**Cách làm: mỗi loại nguồn có một bộ chuyển đổi riêng, tất cả đổ về cùng một "hợp đồng Bronze" chung.**

| Bộ chuyển đổi (`scripts/1_thu_thap/`, Python) | Đầu vào | Đầu ra Bronze | Giữ nguyên gì |
|---|---|---|---|
| Raster → bảng pixel (`1b_chuyen_doi_bronze.py`) | GeoTIFF WorldPop 100 m và 1 km | `worldpop/pop_100m_raw.csv`: year, row, col, pop_raw | Mọi giá trị, **kể cả mã NoData −99999** |
| Excel → CSV nguyên văn (`1b`) | `location-tyt.xlsx`, `danh-sach-cskcb.xlsx` | `co_so_y_te/tyt_raw.csv`: dong_excel, cot1, cot2, cot3 | Số dòng Excel và chuỗi gốc, **kể cả dòng tiêu đề** |
| Overpass JSON → đồ thị + ranh giới (`1c_tra_cuu_khong_gian.py`) | JSON đường, ranh giới | `duong_nut.csv`, `duong_canh.csv`, `ranh_gioi_56_xa.geojson` | Hình học gốc; tốc độ theo loại đường (A6) |
| Độ cao + đa giác → bảng tra cứu pixel (`1c`) | Terrain Tiles, GADM | `o_luoi_100m.csv`: ô → xã, độ cao, độ dốc, nút đường gần nhất | Toạ độ gốc; xã rỗng nếu pixel ngoài 56 xã |

**Hợp đồng Bronze chung** cho mọi nguồn:
1. Đường dẫn `bronze/source_system=<nguồn>/ingestion_date=<ngày nạp>/`. Bronze chỉ thêm, không sửa, nên mỗi lần nạp lại là một thư mục mới.
2. File gốc có SHA-256 trong `data/manifest_raw.csv`. Bảng Bronze nào sinh từ file gốc nào ghi ở `1_du_lieu_nguon/bronze_danh_muc.csv`.
3. **Không sửa giá trị nào** ở Bronze. Việc quyết định dòng nào sạch, dòng nào lỗi là của Spark Bước 2, có mã lý do.
4. Spark Bước 1 đọc mọi bảng bằng **schema khai báo** (`b1_doc.py`), không để Spark tự đoán kiểu.

**Có thông tin gì sau khi chuyển đổi** (`1_du_lieu_nguon/spark_bang_da_doc.csv`): 11 bảng CSV. tổng cộng 4.252.689 dòng, cùng 1 file GeoJSON có 56 đa giác.
Bảng lớn nhất là `pop_100m_raw` với 3.006.720 dòng (10 năm × 300.672 pixel của hộp bao).

**Vì sao làm như vậy:**
- Spark không đọc trực tiếp GeoTIFF hay Excel. Đổi sang CSV **giữ nguyên giá trị** thì kiểm tra chất lượng vẫn nằm trong Spark và có mã lý do,
  thay vì bị lọc ngầm trong bước chuyển đổi.
- Giữ NoData và dòng tiêu đề Excel ở Bronze thì đếm được chính xác bao nhiêu dòng bị loại và vì sao (Giai đoạn 3), đồng thời đối soát bảo toàn được:
  dòng thô = dòng sạch + dòng bị loại.
- Hợp đồng chung giúp thêm nguồn mới mà không phải sửa Bước 2–5.

**Gắn thêm nguồn Database hoặc Event** (đề xuất, **chưa làm** vì bài toán không có nguồn này):
- *Database* (ví dụ hệ thống quản lý khám chữa bệnh của Sở): đọc bằng `spark.read.format("jdbc")` với câu truy vấn và schema khai báo, chia song song
  theo `partitionColumn`. **Chụp lại một bản vào Bronze** (`source_system=his/ingestion_date=…`) trước khi kiểm tra, để chạy lại cho cùng kết quả
  dù cơ sở dữ liệu nguồn đã đổi.
- *Event* (ví dụ lượt khám phát sinh theo thời gian thực): đọc bằng Spark Structured Streaming (`readStream` với schema khai báo, watermark theo thời
  điểm sự kiện), ghi nối tiếp vào Bronze. Bước 2–5 có thể chạy theo lô trên Bronze như hiện nay, hoặc trong `foreachBatch`.
- Lý do chưa làm: không có nguồn thật, và quy tắc dự án là không tạo dữ liệu giả. Nhịp ra quyết định của bài toán là theo năm (Giai đoạn 5.2),
  nên xử lý theo lô là đủ.

---

## Giai đoạn 3 · Tích hợp, làm sạch và chuẩn bị dữ liệu

### 3.1 Khám phá phân bố

| Quan sát | Số liệu | Ý nghĩa cho xử lý |
|---|---|---|
| Phần lớn pixel trong hộp bao không thuộc phạm vi | 2.598.456 / 3.006.720 dòng dân số 100 m (86,4%) là NoData hoặc ngoài 56 xã (`2_chat_luong/spark_so_dong_theo_nguon.csv`) | Phải tách "ngoài phạm vi" khỏi "lỗi", nếu không sẽ thành "3 triệu dòng lỗi" |
| Ô có dân thực sự ít | 41.221 ô 100 m có dân trong 56 xã | Bảng phân tích chỉ khoảng 41 nghìn dòng mỗi năm, nhỏ hơn nhiều so với Bronze |
| Dân rất gần trạm, nhưng có đuôi dài | Trung vị 611 m, p95 1.984 m (`3_mo_ta_kpi/kpi_tong_hop_spark.csv`) | Phải báo cáo cả p95, không chỉ trung bình |
| Lệch so với số liệu chính thức | MAPE 21,0% năm 2024; chỉ 2/17 bản ghi trong ±3% (`2_chat_luong/qa_official_population.csv`) | Quy tắc ±3% chỉ cảnh báo, không loại (A11) |
| Dân 60+ | 134.755 / 1.177.774 người (11,4%) năm 2024 | KPI riêng cho 60+ (A3) |

### 3.2 Làm sạch

Spark Bước 2 (`b2_kiem_tra_lam_sach.py`) kiểm tra từng dòng theo mã lý do:

| Nhóm kiểm tra theo khung | Mã lý do | Kết quả lần chạy (`2_chat_luong/spark_quarantine_tong_hop.csv`) |
|---|---|---|
| Dữ liệu thiếu (Null) | `THIEU_KHOA`, `THIEU_TOA_DO`, `THIEU_GIA_TRI`, `THIEU_NUT_DUONG` | 0 dòng |
| Bản sao trùng lặp | `TRUNG_KHOA`, `TRUNG_TRAM` | 0 dòng |
| Sai định dạng / kiểu | `SAI_KIEU_HOAC_SO_COT` (cột `_corrupt_record`), `SAI_DINH_DANG_TOA_DO` | 1 dòng: ô toạ độ "Column1…" không đúng mẫu |
| Sai miền giá trị | `NGOAI_HOP_BAO`, `QUAN_KHONG_HOP_LE`, `NAM_NGOAI_PHAM_VI`, `GIA_TRI_KHONG_DUONG` | 0 dòng |
| Không phải bản ghi | `KHONG_PHAI_BAN_GHI_TRAM` | 8 dòng tiêu đề / tên quận trong Excel |
| Ngoài phạm vi (**không phải lỗi**) | `GIA_TRI_AM_NODATA`, `NGOAI_56_XA` | 3.030.871 dòng, đưa vào `silver/excluded_records` |

**Kết quả:** chỉ có 9 dòng lỗi thật, tất cả nằm trong file nhập tay. 56/56 trạm sạch.

### 3.3 Tích hợp và tạo đặc trưng

Spark Bước 3 (`b3_bien_doi.py`) nối 6 nguồn bằng các khoá sau:

| Phép nối | Khoá | Kỹ thuật Spark |
|---|---|---|
| Dân số 100 m ↔ ô / xã | (row, col) | join, ô 1 km qua (k1_row, k1_col) |
| Ô ↔ 56 trạm, 30 CSKCB | không có khoá bằng nhau: tìm điểm gần nhất | `F.broadcast` + nối chéo (`BroadcastNestedLoopJoin`) |
| Ô ↔ mạng đường | nút đường gần nhất `nut_duong` | Dijkstra trên driver, rồi join lại vào Spark |
| Excel trạm ↔ danh mục 56 xã | tên xã chuẩn hoá (`norm_name`) | UDF + broadcast join |

Các đặc trưng nghiệp vụ được tạo:
- `dist_m`: khoảng cách chim bay, mét, toạ độ UTM 49N.
- `net_dist_m`, `net_time_min`: khoảng cách và thời gian theo đường.
- `detour`: hệ số vòng.
- `pop60`: dân 60+ (A3).
- `nhom`: phường / xã đồng bằng / xã miền núi (A7).
- `kc_cskcb_m`: khoảng cách tới CSKCB.

### 3.4 Nguyên tắc: không xoá điểm bất thường theo cảm tính

Pipeline chỉ loại dòng vi phạm một quy tắc đã định nghĩa. Các điểm bất thường **được giữ lại và gắn cờ**:
- 8 trạm nằm ngoài ranh giới xã của chính nó: giữ lại, vì có thể do ranh giới GADM thô.
- 1 trạm cách tâm dân cư xã hơn 3 km: giữ lại, ghi vào `7_kiem_chung/qa_summary.csv` với nhãn `CẦN XEM`.
- Lệch 21% giữa WorldPop và số liệu NQ 1251 là **cảnh báo**, không xoá.
- Ô rất đông dân và các cụm xa trạm chính là tín hiệu nghiệp vụ (vùng lõm, Giai đoạn 5), không phải nhiễu.
- Dòng NoData không bị xoá: chúng vào `excluded_records` kèm khoá tham chiếu, tra lại được bản gốc ở Bronze.

---

## Giai đoạn 4 · Lưu trữ và quản trị dữ liệu

### 4.1 Kiến trúc phân tầng

| Tầng theo khung | Tầng trong dự án | Định dạng | Dung lượng | Nội dung |
|---|---|---|---|---|
| Dữ liệu gốc | `data/raw/`, `data/reference/` | TIFF, JSON, XLSX, CSV | 1.066 MB tải về (`data/manifest_raw.csv`) | File gốc, không sửa, không xoá |
| Lake | `data/lake/bronze/` (hoặc HDFS `/mis5007/bronze`) | CSV, GeoJSON | 159 MB | Bản ghi thô, giữ nguyên giá trị |
| Đã chuẩn hoá (Parquet) | `data/lake/silver/` | Parquet Snappy, chia thư mục `year` / `ma_quan` | 42,0 MB trên HDFS (trước tối ưu 79,9 MB) | Dữ liệu sạch, quarantine, excluded |
| Phục vụ | `data/lake/gold/` + `outputs/tables/` | Parquet + CSV | Gold 3,2 MB | KPI, ma trận khoảng cách, đề xuất, dự báo |

**Vì sao tầng phục vụ là Gold Parquet + CSV, không phải Warehouse hay NoSQL:** Gold chỉ khoảng 3 MB, người dùng ít và đọc theo năm. Người dùng đã chốt
không dùng PostgreSQL/PostGIS (26/11/2026). Parquet và CSV đọc được bằng Excel và mọi công cụ BI, nên không cần thêm một hệ quản trị cơ sở dữ liệu.

### 4.2 Quản trị

| Yêu cầu | Đã làm | Chưa làm / đề xuất |
|---|---|---|
| Phân quyền | `data/raw`, `data/reference` khoá chỉ đọc trong VS Code; HDFS bật kiểm tra quyền | Cụm demo chạy bằng user `root`; triển khai thật cần tài khoản riêng và Kerberos / ACL |
| Chủ sở hữu | Mỗi nguồn ghi chủ sở hữu và giấy phép trong `data/manifest_raw.csv`: WorldPop (Univ. Southampton), OSM contributors, GADM, Sở Y tế (CSKCB), nhóm (toạ độ nhập tay) | Cần người chịu trách nhiệm cập nhật toạ độ trạm phía Sở |
| Thời gian lưu giữ | Bronze giữ mọi lần nạp (`ingestion_date`); quarantine giữ đến khi sửa ở nguồn | Chưa có chính sách xoá tự động, ví dụ giữ 3 lần nạp gần nhất |
| Giấy phép | GADM không phân phối lại, nên `data/lake/` không đưa lên GitHub (`.gitignore`) | Báo cáo phải ghi "© OpenStreetMap contributors", CC BY 4.0 của WorldPop |
| Metadata | Schema khai báo trong `b1_doc.py`; `7_kiem_chung/tu_dien_du_lieu.csv` (ý nghĩa từng trường); `1_du_lieu_nguon/spark_bang_da_doc.csv` | – |
| Truy vết nguồn gốc (lineage) | Chuỗi đầy đủ, xem dưới bảng | – |

Chuỗi truy vết nguồn gốc gồm 5 mắt xích:
1. `data/manifest_raw.csv`: URL, SHA-256 và giờ tải của từng file gốc.
2. `1_du_lieu_nguon/bronze_danh_muc.csv`: file gốc nào sinh ra bảng Bronze nào.
3. Khoá trong `quarantine` / `excluded`: trỏ ngược lại dòng Bronze.
4. `outputs/run_manifest.csv`: script nào tạo file kết quả nào, số dòng, SHA-256.
5. `logs/`: mọi thứ in ra trong từng lần chạy.

---

## Giai đoạn 5 · Xử lý và phân tích

### 5.1 Chọn phương pháp theo câu hỏi nghiệp vụ

| Loại phân tích | Câu hỏi | Phương pháp | Kết quả chính (dữ liệu) |
|---|---|---|---|
| **Mô tả** | Bao nhiêu % dân trong 3 km? | KPI có trọng số dân: groupBy, phân vị có trọng số bằng hàm cửa sổ | 98,69% chim bay; 96,10% đường đi; 99,71% trong 15 phút; tỷ lệ gần như không đổi 2015–2024 (98,688% → 98,686%, `3_mo_ta_kpi/kpi_theo_nam_spark.csv`) |
| **Chẩn đoán** | Ai ở ngoài, vì sao? | Gom cụm ô xa trạm (`scipy.ndimage`), quy tắc địa hình / phân tán (A8) | 98 cụm cách trạm > 5 km với 2.623 người; 118 cụm cần > 20 phút với 1.788 người (`4_chan_doan/white_zones.csv`). Xã thấp nhất: Hòa Ninh 42,19% (đường đi 34,90%), Hòa Bắc 56,52% (`3_mo_ta_kpi/kpi_theo_xa_spark.csv`) |
| **Dự báo** | Đến 2030 thế nào? | So 4 mô hình bằng backtest; ETS tắt dần thắng (MAPE 0,234% dân số, 1,52% dân 60+, `6_du_bao/forecast_model_choice.csv`) | **Dự báo** dân 2030: 1.214.516 (80%: 1.205.428–1.223.605); dân 60+: 170.427 (80%: 163.210–177.645); tỷ lệ trong 3 km vẫn 98,69% nếu giữ 56 trạm |
| **Khuyến nghị** | Đặt thêm ở đâu? Rà soát trạm nào? | Tham lam bao phủ tối đa (A10); xét đủ 384 tổ hợp NQ 1251; quy tắc A15 | +5 trạm: 98,69% → 99,53%; gộp 56 → 47 không làm giảm tỷ lệ 3 km (98,69%) nhưng tăng trung vị từ 611 m lên 647–699 m (`5_khuyen_nghi/scenario_nq1251.csv`); 17 trạm rà soát, 14 đã gộp theo NQ 1251, 3 chưa gộp |

### 5.2 Nhịp xử lý: theo lô (batch)

| Tiêu chí | Bài toán này | Kết luận |
|---|---|---|
| Dữ liệu đổi bao lâu một lần | WorldPop hằng năm; OSM khi tải lại; toạ độ trạm khi trạm đổi | Không có dữ liệu thời gian thực |
| Quyết định ra bao lâu một lần | Kế hoạch mạng lưới theo năm, theo đợt sắp xếp hành chính | Độ trễ chấp nhận được tính bằng tuần |
| Chi phí | Một lần chạy khoảng 1 phút trên máy cá nhân (`7_kiem_chung/thoi_gian_5_buoc.csv`) | Không cần hạ tầng chạy liên tục |

→ Chọn **batch**. Near-real-time hay streaming chỉ đáng làm khi có dữ liệu sự kiện, ví dụ lượt khám từng ngày, và có quyết định cần phản ứng nhanh.
Bài toán này hiện không có cả hai.

### 5.3 Phương án xử lý: Spark pipeline 5 bước

```
[1. ĐỌC] ➔ [2. KIỂM TRA & LÀM SẠCH] ➔ [3. BIẾN ĐỔI] ➔ [4. TỔNG HỢP] ➔ [5. GHI]
```

| Bước | File | Việc chính | Kỹ thuật Spark |
|---|---|---|---|
| 1 · Đọc | `b1_doc.py` | Đọc 11 bảng CSV và 1 GeoJSON của Bronze | `StructType` khai báo, `PERMISSIVE` + `_corrupt_record`, `cache()` + `count()` |
| 2 · Kiểm tra & làm sạch | `b2_kiem_tra_lam_sach.py` | Kiểm Null / kiểu / miền giá trị / trùng; tách quarantine và excluded | `when` / `concat_ws` gắn mã lý do, `row_number` tìm bản trùng, broadcast join kiểm toàn vẹn tham chiếu |
| 3 · Biến đổi | `b3_bien_doi.py` | Chuẩn hoá UTM 49N, dân 60+; nối đa nguồn; cột nghiệp vụ; ghi Silver | `F.broadcast`, join, UDF; ghi rồi đọc lại Parquet chia thư mục `year` / `ma_quan` |
| 4 · Tổng hợp | `b4_tong_hop.py`, `b4_khuyen_nghi.py`, `b4_du_bao.py` | KPI, vùng lõm, khuyến nghị, dự báo, đối soát | `groupBy`, hàm cửa sổ (phân vị có trọng số), Spark SQL đối chiếu DataFrame API, `applyInPandas` |
| 5 · Ghi | `b5_ghi.py` | Ghi Gold, bảng CSV phục vụ, đo Parquet/CSV, kiểm tra ghi | Parquet Snappy `partitionBy`, cờ `_SUCCESS` |

**Điều chỉnh có lý do:** tầng Silver được ghi ở Bước 2 (bảng cách ly, đúng như khung quy định) và cuối Bước 3 (dữ liệu sạch), không dồn hết về Bước 5.
Lý do:
- Bước 4 đọc lại từ Parquet đã nén thay vì tính lại cả chuỗi từ 3 triệu dòng CSV.
- Nếu chạy hỏng giữa chừng, dữ liệu sạch vẫn còn.
- Bước 1–4 không ghi CSV hay Gold: mọi kết quả vào hàng chờ, Bước 5 ghi một lần. Nhờ vậy đối soát với bộ số đã chốt chạy được **trước** khi ghi.

**Tối ưu đã đo (27/11/2026):**

| Tối ưu | Đo được (trước → sau) | Kết quả đầu ra |
|---|---|---|
| `excluded_records` chỉ lưu khoá tham chiếu; bản gốc tra lại ở Bronze | Dung lượng file dữ liệu 57,7 MB → 16,3 MB (−72%). Bản cũ dao động 56–79 MB giữa các lần chạy vì thứ tự dòng thay đổi | Không đổi: quarantine vẫn giữ nguyên văn; bảng mẫu giống từng byte |
| Mỗi nguồn đếm một lượt theo (loại, lý do); bảng tổng hợp lý do lấy từ lượt đếm này | Bỏ 1 lượt quét / nguồn và 1 lượt quét toàn bộ 3 triệu dòng bị loại | Giống từng byte |
| Dòng mẫu lấy bằng `orderBy().limit(10)` mỗi nhóm | Bỏ hàm cửa sổ trên 3 triệu dòng dồn vào vài nhóm (lệch dữ liệu) | Giống từng byte |
| p50 và p95 của cùng một biến tính trong một lần quét cửa sổ | Số lần quét cửa sổ: 25 → 15 | Giống từng byte |
| Giải phóng bảng thô khỏi bộ nhớ đệm cuối Bước 2 | Bớt RAM cho Bước 3–5 (có ích khi executor YARN chỉ có 1 GB) | Không đổi |

**Thời gian chạy toàn pipeline Spark** trên MacBook Air M5, 5 cặp chạy xen kẽ ngày 27/11/2026. Log của code cũ lưu ngoài dự án; log của code mới ở `logs/spark_5_buoc_*.log`.

| Cặp | Thứ tự | Code cũ (4 giai đoạn) | Code mới (5 bước) | Tải máy (load average) |
|---|---|---|---|---|
| 1 | cũ trước | 232 giây | 138 giây | 15–21 |
| 2 | cũ trước | 154 giây | 84 giây | 13–21 |
| 3 | cũ trước | 73 giây | 68 giây | khoảng 13 |
| 4 | mới trước | 60 giây | 67 giây | 12 → 9 |
| 5 | mới trước | 51 giây | 47 giây | khoảng 8–9 |

Đọc bảng này như sau:
- Trong lúc đo, macOS chạy nền một tiến trình phân tích ảnh (`mediaanalysisd`) chiếm nhiều CPU, và tải máy giảm dần.
- Ba cặp đầu bản mới luôn chạy sau, tức lúc máy rảnh hơn, nên được lợi. Khi đảo thứ tự (cặp 4–5), chênh lệch đổi chiều.
- Kết luận trung thực: **tổng thời gian không đổi đáng kể**, chênh lệch nằm trong mức nhiễu của máy.
- Số job Spark gần như bằng nhau: 448 (cũ) và 439 (mới). Bước 1 mới đếm dòng ngay khi đọc nên thêm vài job, bù cho các lượt quét đã bỏ.
- Lợi ích chắc chắn của đợt tối ưu là **dung lượng lưu trữ** và **cấu trúc rõ theo 5 bước**. Mỗi bước có thời gian và số job riêng
  (`7_kiem_chung/thoi_gian_5_buoc.csv`), nên nếu cần tối ưu tiếp thì biết bắt đầu từ đâu: Bước 4 (KPI, khuyến nghị, dự báo) và Bước 2 (kiểm tra 3 triệu dòng)
  chiếm nhiều thời gian nhất.

**Đối soát:** 48/48 phép `ĐẠT` trên máy (`7_kiem_chung/doi_soat_spark.csv`). Chạy với executor trên YARN của cụm Docker: 47/47 `ĐẠT` (phép so với pyproj tự bỏ
vì container không có pyproj), 184 giây, ứng dụng `FINISHED` / `SUCCEEDED` (`logs/02_08_spark_20261126T165529Z.log`, `logs/hdfs_yarn_cli_sau_20261126T165855Z.log`).
Chạy lại sau khi đổi tên thư mục (27/11/2026): YARN 171 giây, 47/47 `ĐẠT` (`logs/spark_5_buoc_20261127T005707Z.log`).
Tầng Silver trên HDFS giảm từ 79,9 MB (`logs/hdfs_yarn_cli_sau_20261126T160110Z.log`) xuống 42,0 MB.
 Ngoài hai bảng mới và hai bảng chỉ đổi thứ tự dòng hoặc thời gian đo, mọi bảng CSV kết quả
**giống từng byte** với trước khi tái cấu trúc (so bằng SHA-256).

---

## Giai đoạn 6 · Trực quan hoá và diễn giải kết quả

### 6.1 Sản phẩm trực quan

| Hình (`outputs/figures/`) | Thông điệp cho người quyết định |
|---|---|
| `h1_ban_do_khoang_cach.png` | Khoảng cách từ từng ô 100 m tới trạm gần nhất; 98,69% dân trong 3 km, phần còn lại ở các xã Hòa Vang có tỷ lệ thấp (Hòa Ninh, Hòa Bắc, `3_mo_ta_kpi/kpi_theo_xa_spark.csv`) |
| `h2_vung_trang_dia_hinh.png` | Vùng cách trạm > 5 km kèm nguyên nhân: 1.864 / 2.623 người ở cụm mang cả địa hình và dân cư phân tán, 759 người ở cụm chỉ do phân tán (`4_chan_doan/white_zones.csv`) |
| `h3_ty_le_3km_theo_xa.png` | Hòa Ninh (42,19%) và Hòa Bắc (56,52%) thấp hơn hẳn các xã còn lại |
| `h4_mat_do_vs_khoang_cach.png` | Quan hệ giữa mật độ dân của ô và khoảng cách tới trạm |
| `h5_du_bao_dan_so.png` | **Dự báo** 2030 kèm khoảng 80%, đối chiếu số chiếu WorldPop |
| `h7_de_xuat_vi_tri.png`, `h9_truoc_sau_de_xuat.png` | Vị trí đề xuất và độ phủ trước / sau khi thêm trạm |
| `h8_ban_do_hanh_chinh_truoc_2025.png` | Phạm vi 56 xã/phường trước sắp xếp |

`h6` chỉ được vẽ khi có xã giảm tiếp cận đến 2030. Lần chạy này có 0 xã giảm (`6_du_bao/access_2030_total.csv`) nên không có h6.

### 6.2 Ba thông điệp chính

1. **Mạng lưới 56 trạm đã phủ gần hết dân:** 98,69% dân trong 3 km, 99,71% tới trạm trong 15 phút xe máy. Vấn đề còn lại nằm ở **2 xã miền núi**,
   nơi tỷ lệ chỉ 47,87%, chênh 51,84 điểm % so với phường.
2. **Gộp trạm theo NQ 1251 không làm mất độ phủ 3 km** (vẫn 98,69% ở cả kịch bản tốt nhất và kém nhất), nhưng dân phải đi xa hơn: trung vị tăng từ 611 m
   lên 647–699 m tuỳ trạm được giữ. Vì vậy **chọn giữ trạm nào quan trọng hơn gộp hay không**.
3. **Thêm 5 trạm đúng chỗ** đưa tỷ lệ lên 99,53% và giảm số dân cách trạm > 5 km từ 2.624 xuống 1.693 người. Đây là kết quả của thuật toán trên dữ liệu
   ước tính, cần khảo sát thực địa trước khi quyết định.

### 6.3 Có cần dashboard không: hiện chưa cần

| Tiêu chí | Bài toán này | Hướng |
|---|---|---|
| Ai dùng | Ban lãnh đạo Sở, số người ít | Báo cáo + bản đồ là đủ |
| Bao lâu xem một lần | Theo năm, theo đợt sắp xếp | Không cần màn hình theo dõi liên tục |
| Dữ liệu đổi bao lâu | Hằng năm (Giai đoạn 5.2) | Dashboard sẽ đứng yên cả năm |
| Nhu cầu xem chi tiết từng xã | Có, nhưng 56 xã | Bảng `3_mo_ta_kpi/kpi_theo_xa_spark.csv` lọc bằng Excel, cùng hình h3 |

→ **Không làm dashboard ở giai đoạn này.** Nên làm dashboard khi có một trong các điều kiện sau:
- Sở cung cấp dữ liệu phát sinh hằng ngày hoặc hằng tháng, ví dụ lượt khám.
- Nhiều phòng ban cùng theo dõi KPI theo xã.
- Có mục tiêu KPI cần giám sát định kỳ.

Khi đó, các bảng Gold (`data/lake/gold/`) đã sẵn sàng làm nguồn cho công cụ BI mà không phải sửa pipeline.

---

## Giai đoạn 7 · Triển khai, giám sát và đánh giá giá trị

Phần này là **đề xuất**: kết quả chưa được tích hợp vào quy trình của Sở Y tế.

### 7.1 Tích hợp vào quy trình ra quyết định

| Kết quả | Dùng cho quyết định | Hành động đề xuất |
|---|---|---|
| `5_khuyen_nghi/ung_vien_gop_tram.csv`: 3 trạm chưa gộp (Khuê Trung, Thạch Thang, Hòa Thuận Tây) | Rà soát gộp trạm | Sở rà soát chức năng dự phòng theo địa bàn; đây **không phải đề nghị đóng trạm** (A16) |
| `5_khuyen_nghi/greedy_sites.csv`: điểm đặt mới | Đặt thêm trạm / điểm trạm | Khảo sát thực địa 5 điểm đầu tiên |
| `3_mo_ta_kpi/kpi_theo_xa_spark.csv` | Ưu tiên đầu tư | Ưu tiên Hòa Ninh, Hòa Bắc |
| `5_khuyen_nghi/scenario_nq1251.csv` | Chọn trạm giữ lại khi gộp | Dùng kịch bản "giữ trạm tốt nhất mỗi nhóm" làm tham chiếu |

### 7.2 Giám sát

| Giám sát gì | Cách làm | Ngưỡng báo động (đề xuất) |
|---|---|---|
| KPI theo năm | Mỗi khi WorldPop phát hành năm mới: tải (`1a`), chạy lại `chay_toan_bo.sh`, so `kpi_theo_nam_spark.csv` | % trong 3 km giảm > 0,1 điểm % |
| Suy giảm mô hình dự báo | So dự báo cũ với số WorldPop năm mới; theo dõi MAPE backtest (hiện 0,234%) | MAPE vượt biên độ khoảng 80% (0,725% với dân số) |
| Chất lượng dữ liệu đầu vào | `qa_summary.csv` (hiện 7 dòng `CẦN XEM`), số dòng quarantine (hiện 9) | Quarantine tăng; xuất hiện lỗi ở nguồn khác ngoài file nhập tay |
| Pipeline chạy đúng | `doi_soat_spark.csv` (48 phép), `kiem_tra_run_manifest.csv`, `thoi_gian_5_buoc.csv` | Bất kỳ dòng `LỆCH` hay `FAIL` nào |

### 7.3 Đánh giá giá trị và cải tiến lặp

- **Tác động dự kiến** của mỗi phương án đã có số tính trước, ví dụ thêm K trạm (`5_khuyen_nghi/greedy_before_after.csv`). Sau khi Sở hành động, chạy lại
  pipeline với toạ độ trạm mới để đo **tác động thực** trên cùng KPI.
- **Giới hạn cần cải tiến**, xếp theo mức ảnh hưởng (đánh giá của nhóm):
  1. Mạng đường còn gồm cao tốc (A6).
  2. "Xã miền núi" mới phân loại theo địa hình, chưa có danh sách chính thức (A7).
  3. Toạ độ trạm chưa kiểm chứng thực địa (A13).
  4. Danh sách CSKCB thiếu phòng khám tư (A17).
  5. WorldPop lệch số liệu chính thức khoảng 21%.
- **Dữ liệu nên xin thêm:** lượt khám theo trạm (đo nhu cầu thật), danh sách xã miền núi chính thức, toạ độ đã kiểm chứng.

---

## Câu hỏi còn mở

1. Sở Y tế hiện lập kế hoạch mạng lưới trạm theo quy trình nào, dùng chỉ tiêu gì? Phần này cần để hoàn thiện Giai đoạn 1.
2. Có mục tiêu chính thức nào về khoảng cách hoặc thời gian tới trạm không? Nếu có, mục tiêu đó sẽ thay cho giả định 3 km (A1).
3. Sở có cung cấp được dữ liệu lượt khám (đã ẩn danh) không? Nếu có, bài toán chuyển từ "khoảng cách" sang "nhu cầu thật", và lúc đó mới cân nhắc dashboard.
