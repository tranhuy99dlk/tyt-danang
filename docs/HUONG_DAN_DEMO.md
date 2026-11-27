# Hướng dẫn demo và giới thiệu sản phẩm trên VS Code

Kịch bản từng bước để trình bày đồ án MIS5007 như một **sản phẩm thực tế**: mở VS Code, chạy thật, chỉ vào file thật, đọc số thật.
Mỗi bước gồm bốn phần: **Thao tác** (bấm gì, mở file nào), **Nói** (câu mẫu), **Người xem thấy gì**, **Vì sao**.

Mọi con số dưới đây lấy từ lần chạy ngày 27/11/2026 và dẫn về file trong `outputs/tables/`. Nếu chạy lại sau khi đổi dữ liệu,
mở lại đúng file đó để đọc số mới, **không đọc số từ tài liệu này**.

Cách cài và chạy: [HUONG_DAN_CHAY.md](HUONG_DAN_CHAY.md). Nội dung báo cáo đầy đủ: [BAO_CAO_7_GIAI_DOAN.md](BAO_CAO_7_GIAI_DOAN.md).

---

## 0. Sản phẩm là gì (giới thiệu trong 30 giây)

> "Đây là một hệ thống phân tích dữ liệu lớn giúp Sở Y tế trả lời bốn câu hỏi: bao nhiêu phần trăm dân sống trong 3 km tới trạm y tế,
> ai đang ở ngoài, vì sao, và nên đặt thêm hoặc rà soát trạm nào. Đầu vào là dữ liệu mở – lưới dân số WorldPop 100 m của 10 năm,
> bản đồ đường OpenStreetMap, độ cao – cùng toạ độ 56 trạm. Đầu ra là bộ chỉ số, bản đồ và danh sách khuyến nghị, mỗi con số đều truy
> được về file nguồn. Toàn bộ chạy lại bằng một phím trong khoảng một phút."

| | Nội dung |
|---|---|
| Người dùng | Ban lãnh đạo Sở Y tế (ra quyết định), nhóm phân tích (vận hành hằng năm) |
| Đầu vào | 158 file nguồn mở (1.066 MB, `data/manifest_raw.csv`) + 2 file Excel nhóm chuẩn bị + 4 bảng tham chiếu |
| Xử lý | Thu thập đa nguồn → hồ dữ liệu Bronze / Silver / Gold → pipeline Spark 5 bước → báo cáo |
| Đầu ra | 51 bảng CSV, 8 hình, 14 bảng Gold Parquet, sổ truy vết SHA-256 |
| Giá trị | Biết chính xác 2.624 người ở xa trạm hơn 5 km nằm ở đâu và vì sao; biết thêm 5 trạm đúng chỗ thì tỷ lệ trong 3 km tăng từ 98,69% lên 99,53% |

**Ba thông điệp người nghe phải nhớ:**
1. **Mạng lưới đã phủ gần hết dân: vấn đề nằm ở 2 xã miền núi và ở vị trí trạm, không phải thiếu trạm đại trà.**
   - 98,69% dân trong 3 km, 99,71% dân tới trạm trong 15 phút (`3_mo_ta_kpi/kpi_tong_hop_spark.csv`).
   - Xã miền núi chỉ có 47,87% dân trong 3 km.
   - Trạm Hòa Ninh cách tâm dân cư của xã 3.150 m (`2_chat_luong/tram_y_te_56.csv`).
2. **Dữ liệu được xử lý có kiểm soát.**
   - Chỉ 9 dòng lỗi thật bị cách ly.
   - 3.030.871 dòng ngoài phạm vi được tách riêng và **không** gọi là lỗi (`2_chat_luong/spark_quarantine_tong_hop.csv`).
   - 48/48 phép đối soát ĐẠT (`7_kiem_chung/doi_soat_spark.csv`).
3. **Lựa chọn kỹ thuật có số đo chứng minh:** trên cùng một truy vấn, Parquet phân vùng quét ít hơn CSV phẳng 38,79 lần (`7_kiem_chung/so_sanh_parquet_csv.csv`).

---

## 1. Chuẩn bị

### Một ngày trước

- [ ] Bấm Cmd + Shift + B, kiểm tra `Doi soat: 48 phep, 0 LECH.` và `7_kiem_chung/qa_summary.csv` chỉ có 7 `CẦN XEM` đã biết
      (danh sách ở [HUONG_DAN_CHAY.md](HUONG_DAN_CHAY.md#c2-đọc-kết-quả-kiểm-tra-trước-khi-dùng-số)).
- [ ] Ghi số liệu lên slide **từ** `7_kiem_chung/so_lieu_chot.csv`. File này có sẵn file nguồn và SHA-256 cho từng con số.
- [ ] Chụp màn hình dự phòng: Terminal lúc chạy xong, cây thư mục `data/lake/`, các hình h1, h3, h8, h9.
- [ ] (Chỉ khi demo cụm) Chạy Phần B của hướng dẫn chạy tới bước B5, chụp http://localhost:9871 và http://localhost:8088, rồi **giữ cụm bật**
      tới buổi demo hoặc dùng ảnh chụp.

### 30 phút trước

- [ ] Mở VS Code ở thư mục dự án, đóng hết tab thừa. Đóng các ứng dụng nặng khác để lúc chạy trực tiếp máy không bị chậm.
- [ ] Phóng to cho khán giả đọc được: Cmd + `=` hai–ba lần; chữ Terminal đã to theo.
- [ ] Mở sẵn các tab theo thứ tự sẽ show (tab nào mở trước nằm bên trái):
  1. `README.md` (bấm chuột phải → **Open Preview**)
  2. `outputs/figures/h8_ban_do_hanh_chinh_truoc_2025.png`
  3. `outputs/tables/1_du_lieu_nguon/spark_bang_da_doc.csv`
  4. `scripts/2_spark_5_buoc/chay_spark.py`
  5. `outputs/tables/2_chat_luong/spark_quarantine_tong_hop.csv`
  6. `logs/plan_khoang_cach_2024.txt`
  7. `outputs/tables/7_kiem_chung/so_sanh_parquet_csv.csv`
  8. `outputs/tables/3_mo_ta_kpi/kpi_tong_hop_spark.csv`
  9. `outputs/figures/h1_ban_do_khoang_cach.png`
  10. `outputs/figures/h9_truoc_sau_de_xuat.png`
  11. `outputs/tables/7_kiem_chung/doi_soat_spark.csv`
- [ ] Mở sẵn một Terminal trong VS Code (`Ctrl` + `` ` ``) và kéo khung Terminal cao khoảng 1/3 màn hình.

---

## 2. Kịch bản từng bước trên VS Code (khoảng 18 phút)

Kịch bản đi theo 7 giai đoạn của vòng đời phân tích dữ liệu lớn. Phần kỹ thuật đi theo 5 bước Spark.

### Bước 1 · Vấn đề và sản phẩm (phút 0–2) – Giai đoạn 1

| | |
|---|---|
| **Thao tác** | Tab `README.md` (Preview): đọc câu hỏi quản lý. Chuyển sang tab `h8_ban_do_hanh_chinh_truoc_2025.png` |
| **Nói** | Đọc đoạn giới thiệu 30 giây ở mục 0. Sau đó: "Phạm vi là 56 xã/phường của Đà Nẵng **trước** ngày 01/01/2025 (45 phường, 11 xã). Từ 2025, Nghị quyết 1251 gộp 17 xã thành 8 đơn vị, nên câu hỏi mạng lưới trạm trở nên cấp thiết." |
| **Người xem thấy** | Bản đồ 56 đơn vị đánh số |
| **Vì sao** | Người nghe là lãnh đạo: mở đầu bằng quyết định cần ra, không bằng công nghệ |

### Bước 2 · Dữ liệu nhiều nguồn và cách thu thập (phút 2–4) – Giai đoạn 2

| | |
|---|---|
| **Thao tác** | Cột Explorer (Cmd + Shift + E): mở `scripts/1_thu_thap/` (3 file `1a`, `1b`, `1c`). Mở tab `spark_bang_da_doc.csv`. Mở `data/lake/bronze/` → `source_system=worldpop` → `ingestion_date=…` |
| **Nói** | "Dữ liệu đến từ 5 định dạng: GeoTIFF, JSON của OpenStreetMap, GeoJSON, ảnh độ cao, Excel. Mỗi loại có một bộ chuyển đổi đưa về **một hợp đồng chung**: thư mục theo nguồn và ngày nạp, giữ nguyên giá trị (kể cả mã NoData), ghi SHA-256. Spark đọc 11 bảng, 4.252.689 dòng, cộng 56 đa giác ranh giới." |
| **Người xem thấy** | Bảng danh mục: nguồn, ngày nạp, số cột schema, số dòng đọc; thư mục Bronze chia theo nguồn / ngày |
| **Vì sao** | Trả lời câu "đã làm gì với dữ liệu nhiều nguồn, vì sao làm vậy" (báo cáo mục 2.3) |

### Bước 3 · Chạy trực tiếp (phút 4–6) – Giai đoạn 5, Spark 5 bước

| | |
|---|---|
| **Thao tác** | Bấm **Cmd + Shift + B**. Trong khi chạy (khoảng 1 phút), chỉ vào Terminal |
| **Nói** | "Một phím chạy cả ba phần: thu thập → Spark 5 bước → báo cáo. Đây là `BUOC 1 - Doc`, `BUOC 2 - Kiem tra & lam sach` … `BUOC 5 - Ghi`. Dòng `pop_100m_raw tho 3006720 sach 408264` nghĩa là 3 triệu dòng thô chỉ còn khoảng 408 nghìn dòng có dân trong phạm vi. Cuối cùng là bảng thời gian và số job Spark của từng bước." |
| **Người xem thấy** | 5 khối BUOC, dòng `Doi chieu NQ 1251 … KHOP`, `Doi soat: 48 phep, 0 LECH.`, bảng thời gian 5 bước |
| **Vì sao** | Chứng minh pipeline lặp lại được, không chạy tay từng bước |

### Bước 4 · Kiểm tra và làm sạch (phút 6–9) – Giai đoạn 3, Spark Bước 1–2

| | |
|---|---|
| **Thao tác** | Tab `chay_spark.py`: chỉ 5 khối `with nc.buoc(1, …)` … `nc.buoc(5, …)`. Mở `b1_doc.py`, cuộn tới `khai_bao_schema`. Mở `b2_kiem_tra_lam_sach.py`, Cmd + F tìm `def tach`. Chuyển sang tab `spark_quarantine_tong_hop.csv` |
| **Nói** | "Bước 1 đọc bằng schema khai báo, không để Spark đoán kiểu; dòng sai kiểu rơi vào cột `_corrupt_record`. Bước 2 gắn mã lý do cho từng dòng: thiếu giá trị, sai kiểu, ngoài miền giá trị, trùng khoá. Kết quả: **9 dòng lỗi thật**, đều là dòng tiêu đề trong file Excel nhập tay. 3.030.871 dòng còn lại là mã NoData và pixel ngoài 56 xã – đó là **ngoài phạm vi, không phải lỗi**, nên được tách sang vùng riêng." |
| **Người xem thấy** | Mỗi dòng code có chú thích tiếng Việt; bảng 7 nhóm lý do với cột `loai` = `LOI_DU_LIEU` / `NGOAI_PHAM_VI` |
| **Vì sao** | Pipeline có kiểm soát chất lượng và bảng cách ly; trung thực về dữ liệu (không gọi NoData là lỗi) |

### Bước 5 · Biến đổi, lưu trữ và quản trị (phút 9–11) – Giai đoạn 4, Spark Bước 3 và 5

| | |
|---|---|
| **Thao tác** | Explorer: `data/lake/silver/fact_dan_so_o_nam/year=2024/ma_quan=hoa_vang/` (thấy file `.snappy.parquet`). Tab `plan_khoang_cach_2024.txt`: Cmd + F tìm `BroadcastExchange`. Tab `so_sanh_parquet_csv.csv` |
| **Nói** | "Bước 3 nối 6 nguồn: bảng 56 trạm rất nhỏ được **broadcast** tới mọi executor nên bảng 41 nghìn ô không phải shuffle. Đây là bằng chứng trong kế hoạch thực thi. Silver lưu Parquet chia thư mục theo năm và quận: truy vấn năm 2024 chỉ đọc đúng thư mục đó. Đo thật trên cùng một truy vấn: Parquet quét 1.196.358 byte, CSV phẳng quét 46.402.204 byte, gấp 38,79 lần." |
| **Người xem thấy** | Thư mục `year=…/ma_quan=…`; nút `BroadcastExchange`, `BroadcastNestedLoopJoin`; cột `dung_luong_file_quet_byte`, `partition_filters` |
| **Vì sao** | Kiến trúc lưu trữ có thật (không chỉ hình vẽ), quyết định Parquet có số đo |

### Bước 6 · Kết quả cho người quyết định (phút 11–15) – Giai đoạn 5 và 6, Spark Bước 4

| | |
|---|---|
| **Thao tác** | Tab `kpi_tong_hop_spark.csv` → tab `h1_ban_do_khoang_cach.png` → mở `outputs/figures/h3_ty_le_3km_theo_xa.png` → tab `h9_truoc_sau_de_xuat.png` → mở `outputs/tables/5_khuyen_nghi/ung_vien_gop_tram.csv` và `5_khuyen_nghi/scenario_nq1251.csv` |
| **Nói – mô tả** | "98,69% dân trong 3 km chim bay, 96,10% theo đường đi, 99,71% tới trạm trong 15 phút xe máy. Khoảng cách trung vị 611 m, nhưng 5% dân xa hơn 1.984 m." |
| **Nói – chẩn đoán** | "Phường đạt 99,71%, xã miền núi chỉ 47,87%. Thấp nhất là Hòa Ninh 42,19% (theo đường đi 34,90%). 91 trong 98 cụm dân cách trạm hơn 5 km mang cả yếu tố địa hình và dân cư phân tán." |
| **Nói – khuyến nghị** | "Thêm 5 trạm đúng chỗ: tỷ lệ 3 km từ 98,69% lên 99,53%, số dân cách trạm hơn 5 km từ 2.624 xuống 1.693. Theo quy tắc của nhóm, 17 trạm vào danh sách rà soát; 14 trong số đó NQ 1251 đã gộp, còn 3 trạm chưa gộp – đây là danh sách **rà soát**, không phải đề nghị đóng. Gộp 56 → 47 trạm không làm giảm tỷ lệ 3 km, nhưng khoảng cách trung vị tăng từ 611 m lên 647–699 m tuỳ chọn giữ trạm nào." |
| **Người xem thấy** | Bản đồ khoảng cách; biểu đồ tỷ lệ theo xã; bản đồ trước / sau khi thêm trạm; danh sách 17 trạm có cột `trang_thai_nq1251` |
| **Vì sao** | Trả lời trực tiếp câu hỏi quản lý, đủ ba loại phân tích: mô tả, chẩn đoán, khuyến nghị |

### Bước 7 · Dự báo 2030 (phút 15–16) – Giai đoạn 5

| | |
|---|---|
| **Thao tác** | Mở `outputs/figures/h5_du_bao_dan_so.png`, rồi `outputs/tables/6_du_bao/forecast_total_2030.csv` |
| **Nói** | "So 4 mô hình bằng backtest, Holt tắt dần (ETS) thắng. **Dự báo** dân năm 2030 là 1.214.516 người, khoảng 80% từ 1.205.428 đến 1.223.605. Nếu giữ 56 trạm, tỷ lệ trong 3 km vẫn 98,69%. Đây là dự báo, không phải số đo." |
| **Người xem thấy** | Đường dự báo kèm dải 80% |
| **Vì sao** | Luôn nói rõ bản chất dự báo và khoảng tin cậy |

### Bước 8 · Tin cậy được không, và triển khai thế nào (phút 16–18) – Giai đoạn 7

| | |
|---|---|
| **Thao tác** | Tab `doi_soat_spark.csv` (bấm vào cột `ket_qua`). Mở `outputs/tables/7_kiem_chung/so_lieu_chot.csv` |
| **Nói** | "48 phép đối soát, tất cả ĐẠT: bảo toàn số dòng và tổng dân, hai cách tính độc lập (KDTree, Spark SQL so với DataFrame), so với bộ số đã chốt ngày 19/11. Mỗi con số trên slide có file nguồn và mã SHA-256. Triển khai: chạy lại mỗi năm khi WorldPop phát hành số mới, theo dõi KPI theo năm và sai số dự báo. Nhóm **không** làm dashboard, vì quyết định ra theo năm và ít người dùng; các bảng Gold đã sẵn sàng nếu sau này cần. Hạn chế chính: WorldPop là ước tính mô hình, ngưỡng 3 km là giả định của nhóm, toạ độ trạm chưa kiểm chứng thực địa." |
| **Người xem thấy** | Cột `ket_qua` toàn `ĐẠT`; bảng số liệu chốt có cột `file`, `sha256_file` |
| **Vì sao** | Kết thúc bằng mức độ tin cậy, giới hạn và cách đưa vào sử dụng |

### (+3 phút, tuỳ chọn) Cụm HDFS / YARN – chỉ khi đã chuẩn bị trước

| | |
|---|---|
| **Thao tác** | Trình duyệt: http://localhost:9871 → Utilities → Browse the file system → `/mis5007` (bronze, silver, gold). http://localhost:8088 → ứng dụng `mis5007_tram_y_te` `FINISHED` / `SUCCEEDED`. Hoặc mở ảnh chụp |
| **Nói** | "Cùng một code 5 bước chạy được trên HDFS và trên executor YARN của cụm Docker. Chỉ cần đổi hai biến môi trường. Lần chạy YARN gần nhất: 171 giây, 47/47 đối soát ĐẠT (`logs/spark_5_buoc_20261127T005707Z.log`). Cụm chỉ có một máy nên không dùng để đo tăng tốc." |
| **Vì sao** | Đáp ứng yêu cầu học phần về HDFS / YARN |

**Không chạy YARN trực tiếp trên sân khấu:** mất 3–5 phút và cần nhiều RAM. Không bật Docker nếu không demo cụm, vì phần demo chính không cần Docker.

---

## 3. Đi theo một ô dữ liệu từ Bronze tới kết quả

Dùng khi người nghe hỏi "dữ liệu đi qua từng bước thế nào". Ví dụ thật: ô `R0256C0338` ở xã Hòa Ninh.

| Bước | File | Giá trị của ô này |
|---|---|---|
| Thu thập (Bronze) | `data/lake/bronze/source_system=worldpop/…/pop_100m_raw.csv` (`row` 256, `col` 338) | 10 dòng, mỗi năm một dòng: 18,40 người (2015) … 19,97 người (2024) |
| Thu thập (tra cứu không gian) | `…/source_system=tra_cuu_khong_gian/…/o_luoi_100m.csv` | thuộc xã `TYT23` (Hòa Ninh), độ cao 26 m |
| Spark 1 · Đọc → 2 · Kiểm tra | schema khai báo, quy tắc chất lượng | đọc đúng kiểu, không vi phạm quy tắc nào → dòng sạch |
| Spark 3 · Biến đổi | `silver/o_luoi_tiep_can_2024` | trạm gần nhất `TYT23` cách 4.166 m (chim bay), 4.925 m theo đường (10,29 phút xe máy); CSKCB gần nhất cách 9.790 m |
| Spark 4 · Tổng hợp | `4_chan_doan/white_zones.csv` | ô nằm trong vùng xám 3–5 km, góp vào việc Hòa Ninh chỉ có 42,19% dân trong 3 km |
| Spark 5 · Ghi | `luoi_100m/tiep_can_luoi_100m.csv`, `luoi_100m/dan_cu_luoi_100m.csv` | đúng các số trên, mở được bằng Excel |

Cách tìm trong VS Code: mở `outputs/tables/luoi_100m/tiep_can_luoi_100m.csv`, Cmd + F gõ `R0256C0338`.

---

## 4. Khi chạy trực tiếp gặp sự cố

- Pipeline chạy hoàn toàn trên máy, mất mạng không ảnh hưởng.
- Nếu Terminal báo lỗi: nói "kết quả trong `outputs/tables/` là của lần chạy trước", mở log mới nhất `logs/spark_5_buoc_*.log` và ảnh chụp đã chuẩn bị, rồi demo tiếp từ Bước 4.
- Máy chạy chậm hơn thường lệ (macOS đang chạy tác vụ nền): vẫn để chạy, trong lúc chờ thì nói trước nội dung Bước 4.

---

## 5. Câu hỏi thường gặp

| Câu hỏi | Trả lời ngắn | Bằng chứng |
|---|---|---|
| Vì sao chia 7 giai đoạn và 5 bước? | 7 giai đoạn là vòng đời phân tích (từ vấn đề tới triển khai); 5 bước là cách pipeline Spark xử lý dữ liệu: Đọc → Kiểm tra & làm sạch → Biến đổi → Tổng hợp → Ghi | `docs/BAO_CAO_7_GIAI_DOAN.md`, `scripts/2_spark_5_buoc/` |
| Dữ liệu nhiều nguồn được tích hợp thế nào? | Mỗi loại nguồn một bộ chuyển đổi, đổ về cùng hợp đồng Bronze (nguồn / ngày nạp, SHA-256, không sửa giá trị); Spark đọc bằng schema khai báo | Báo cáo mục 2.3, `1_du_lieu_nguon/spark_bang_da_doc.csv` |
| Có đọc Database, dữ liệu luồng (Event) không? | Không, vì bài toán không có nguồn đó và không được tạo dữ liệu giả. Nếu có: JDBC chụp vào Bronze, Structured Streaming ghi nối tiếp vào Bronze | Báo cáo mục 2.3 |
| Vì sao không làm dashboard? | Quyết định ra theo năm, ít người dùng, dữ liệu đổi mỗi năm một lần; báo cáo + bản đồ + bảng là đủ. Bảng Gold sẵn sàng cho công cụ BI khi cần | Báo cáo mục 6.3 |
| Vì sao có cả `quarantine` và `excluded`? | NoData của WorldPop và pixel ngoài 56 xã không phải lỗi; trộn vào quarantine sẽ thành "3 triệu dòng lỗi" – sai sự thật | `2_chat_luong/spark_quarantine_tong_hop.csv` |
| Thiết kế nói lệch ±3% thì cách ly, sao không làm? | Chỉ 2/17 bản ghi mỗi năm đạt; WorldPop là ước tính mô hình nên lệch là cảnh báo, không phải lỗi; báo cáo song song ngưỡng ±20% (A11) | `2_chat_luong/qa_official_population.csv` |
| Broadcast Hash Join ở đâu? | Phép "trạm gần nhất" không có khoá bằng nhau nên Spark dùng `BroadcastNestedLoopJoin` – vẫn broadcast bảng nhỏ, không shuffle bảng lớn. Hash join có ở phép nối theo `ma_tram` | `logs/plan_khoang_cach_2024.txt`, `logs/plan_pruning_2024.txt` |
| Vì sao Dijkstra không chạy trong Spark? | Spark không có sẵn thuật toán đường đi ngắn nhất; đồ thị 236.981 nút đủ nhỏ cho scipy trên driver; kết quả nối lại vào Spark | `b3_bien_doi.py` (`DoThi`) |
| Vì sao không dùng `percentile_approx`? | Nó tính theo **ô**, không theo **người**; câu hỏi là "50% dân sống trong bao xa" nên phải có trọng số dân | `b4_tong_hop.py` (`phan_vi`) |
| Vì sao UTM 49N? | 56 xã nằm từ 107,82°Đ tới 108,34°Đ, phần lớn thuộc múi 49 | `7_kiem_chung/pham_vi_da_nang_cu.csv` |
| Tối ưu được gì? | `excluded_records` chỉ lưu khoá tham chiếu (dữ liệu giảm 72%); số lần quét cửa sổ giảm từ 25 xuống 15; tổng thời gian không đổi đáng kể, đã đo 5 cặp và ghi trung thực | Báo cáo mục 5.3, `7_kiem_chung/thoi_gian_5_buoc.csv` |
| Chạy trên YARN có nhanh hơn không? | Không: cụm chỉ có một máy, NodeManager chạy qua giả lập. Mục đích là chứng minh cùng code chạy phân tán được | `docs/KIEN_TRUC.md` mục 8 |
| Executor bị giết giữa chừng thì sao? | Lần chạy YARN đầu tiên, 2 executor bị giết vì máy ảo Docker hết RAM; Spark tự chạy lại, kết quả vẫn ĐẠT. Sau đó giảm bộ nhớ executor xuống 1 GB | `docs/KIEN_TRUC.md` mục 8 |
| Dự báo tin được không? | Backtest chỉ đo khả năng tái tạo chuỗi WorldPop, không đo sai số với dân số thật; luôn kèm khoảng 80% | `6_du_bao/forecast_backtest.csv`, `6_du_bao/forecast_model_choice.csv` |
| Ngưỡng 3 km lấy từ đâu? | Giả định của nhóm (A1), không phải quy định; luôn kèm 2 km, 5 km, đường đi, 15 phút | `3_mo_ta_kpi/kpi_tong_hop_spark.csv` |
| Vì sao 17 trạm rà soát mà không đề nghị đóng? | Ngưỡng chọn sau khi biết NQ 1251 (lạc quan, A15); CSKCB không thay được chức năng dự phòng theo địa bàn của TYT (A16) | `5_khuyen_nghi/ung_vien_gop_tram.csv` |
| Làm sao biết kết quả đúng? | 48 phép đối soát; mỗi con số có file nguồn và SHA-256 | `7_kiem_chung/doi_soat_spark.csv`, `7_kiem_chung/so_lieu_chot.csv` |

---

## 6. Gắn với chuẩn đầu ra học phần

| Chuẩn đầu ra | Bằng chứng để show |
|---|---|
| CLO2 – kiến trúc lưu trữ có quản trị | Cây `data/lake/` (`source_system` / `ingestion_date`, `year` / `ma_quan`); `1_du_lieu_nguon/bronze_danh_muc.csv`; `data/manifest_raw.csv`; `outputs/run_manifest.csv` |
| CLO3 – pipeline Spark có kiểm soát chất lượng / quarantine | `scripts/2_spark_5_buoc/` (5 bước), `2_chat_luong/spark_quarantine_tong_hop.csv`, `7_kiem_chung/doi_soat_spark.csv`, `7_kiem_chung/thoi_gian_5_buoc.csv`, kế hoạch thực thi trong `logs/`; chạy được trên HDFS và YARN |
| CLO4 – giá trị hỗ trợ quyết định | `3_mo_ta_kpi/kpi_tong_hop_spark.csv`, hình h1, h3, h9, `5_khuyen_nghi/ung_vien_gop_tram.csv`, `5_khuyen_nghi/greedy_before_after.csv`, `7_kiem_chung/so_lieu_chot.csv` |

---

## 7. Những câu không nên nói

| Không nói | Nói đúng |
|---|---|
| "Cách ly 3 triệu dòng lỗi" | "9 dòng lỗi thật; 3.030.871 dòng ngoài phạm vi (NoData, ngoài 56 xã) – không phải lỗi" |
| "Dân số Đà Nẵng năm 2024 là 1.177.774 người" | "Ước tính mô hình WorldPop cho 56 xã/phường cũ là 1.177.774 người" |
| "Năm 2030 có 1.214.516 người" | "Dự báo 2030 là 1.214.516 người, khoảng 80%: 1.205.428–1.223.605" |
| "Spark nhanh hơn 10–15 lần, tiết kiệm 70% I/O" | "Parquet quét ít hơn 38,79 lần so với CSV phẳng trên cùng truy vấn" |
| "Tối ưu làm pipeline nhanh hơn x%" | "Tối ưu giảm 72% dung lượng vùng excluded; tổng thời gian không đổi đáng kể" |
| "Đề nghị đóng 3 trạm" | "3 trạm vào danh sách rà soát; quyết định cần xét chức năng dự phòng theo địa bàn" |
| "Theo quy định, trạm phải cách dân dưới 3 km" | "Ngưỡng 3 km là giả định của nhóm" |
| "Chạy phân tán trên cụm nhiều máy" | "Đã chạy executor trên YARN của cụm Docker một máy, 47/47 đối soát ĐẠT; không dùng để đo tăng tốc" |

Ghi giấy phép trên slide nguồn dữ liệu: WorldPop CC BY 4.0; "© OpenStreetMap contributors" (ODbL 1.0); GADM chỉ dùng học thuật; AWS Terrain Tiles.
