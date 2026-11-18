# Tiếp cận trạm y tế xã/phường – Đà Nẵng cũ

Dự án đo mức độ người dân tiếp cận được trạm y tế (TYT) ở 56 xã/phường Đà Nẵng theo bản đồ hành chính **trước** đợt sắp xếp 01/01/2025,
trên 10 mốc năm 2015–2024 (năm hiện trạng 2024), và đặt TYT cạnh 30 cơ sở khám chữa bệnh (CSKCB) khác trên địa bàn.
Đồ án học phần MIS5007 – Dữ liệu lớn và Ứng dụng; người đọc kết quả là ban lãnh đạo Sở Y tế.

## Kết luận chính

**Không tăng trạm đại trà, cũng không giảm trạm đại trà.** Dữ liệu cho thấy đây là bài toán **đặt sai chỗ và phân bổ sai tải**,
không phải bài toán đếm số trạm. Mỗi con số dưới đây dẫn về một file trong `outputs/tables/` (trừ khi ghi khác).

| Nhận định | Số liệu | File |
|---|---|---|
| Thêm trạm đại trà lợi ít | 98,69% dân đã ở trong 3 km; thêm 5 điểm đề xuất chỉ nâng lên 99,53% | `greedy_before_after.csv` |
| Bớt trạm đại trà không còn chỗ | 17 TYT có từ 6 CSKCB trong 2 km; 14 trong số đó đã bị NQ 1251 gộp từ 01/01/2025, chỉ còn 3 (Khuê Trung, Thạch Thang, Hòa Thuận Tây) để rà soát | `ung_vien_gop_tram.csv` |
| Đặt sai chỗ | TYT xã Hòa Ninh cách tâm dân cư của xã 3.150 m – trạm duy nhất vượt 3 km; Hòa Ninh có tỷ lệ dân trong 3 km thấp nhất 56 xã (42,19%; theo đường đi 34,90%) | `qa_summary.csv`, `data/interim/stations_56.csv`, `kpi_theo_xa_spark.csv` |
| Phân bổ sai tải | Dân thuộc vùng phục vụ của mỗi trạm từ 3.479 (Hòa Bắc) tới 76.954 người (Hòa Khánh Bắc), chênh 22 lần; trung vị 19.488 | `tram_vung_phuc_vu_spark.csv` |
| Nguyên nhân không phải dân số tăng | Dân tăng từ 1.079.220 (2015) lên 1.177.774 (2024) nhưng tỷ lệ trong 3 km giữ 98,69%; dự báo 2030 vẫn 98,69% | `kpi_theo_nam_spark.csv`, `access_2030_total.csv` |
| Không chỉ là đường xá | 91 trong 98 cụm dân cách trạm trên 5 km mang đồng thời hai nguyên nhân: địa hình và dân cư phân tán | `white_zones.csv` |

**CSKCB khác không thay thế được TYT**, xét ở ba lớp:

| Lớp | Bằng chứng | Nguồn |
|---|---|---|
| Pháp lý – chức năng (quyết định) | Thông tư 30/2024/TT-BYT giao TYT 15 nhóm gói dịch vụ. Theo đánh giá của nhóm, 14/15 là dự phòng hoặc quản lý sức khoẻ theo địa bàn (tiêm chủng, quản lý thai, bệnh không lây nhiễm, hồ sơ sức khoẻ điện tử, giám sát dịch, an toàn thực phẩm, xác định tình trạng nghiện); chỉ y học cổ truyền là cơ sở khác cung cấp tương đương được | `data/reference/tt30_2024_goi_dich_vu.csv` |
| Cấp chuyên môn | Luật KCB 15/2023/QH15 (Điều 104) chia 3 cấp: ban đầu – cơ bản – chuyên sâu. Trong 30 CSKCB: 27 cấp cơ bản, 2 chuyên sâu, 1 ban đầu (16 tuyến huyện, 13 tuyến tỉnh, 1 tuyến trung ương); không có TYT nào. Cơ sở cấp ban đầu duy nhất là một trung tâm bác sĩ gia đình tư nhân ở Cẩm Lệ | `cskcb_danang.csv` |
| Địa lý | Ở Hòa Vang, 10/11 TYT không có CSKCB nào trong 2 km, 8/11 không có trong 3 km; CSKCB gần nhất trung bình cách 5.332 m, xa nhất 11.638 m (Hòa Bắc). Ở Hải Châu và Thanh Khê, trung bình 7 CSKCB trong 2 km | `tyt_cskcb_theo_nhom.csv`, `tyt_cskcb.csv` |

Đọc kèm các giới hạn ở cuối trang: dân số là ước tính mô hình, ngưỡng 3 km và cách phân loại gói dịch vụ là giả định của nhóm.

## Bắt đầu nhanh

Cần macOS, Docker Desktop đang chạy, `uv` và khoảng 20 GB ổ đĩa trống. Kho GitHub đã kèm các file nhóm tự chuẩn bị;
cách lấy dự án khi không dùng GitHub: [hướng dẫn cài đặt](docs/HUONG_DAN_CAI_DAT.md#3-lấy-thư-mục-dự-án).

```bash
git clone https://github.com/tranhuy99dlk/tyt-danang.git mis5007_tram_y_te && cd mis5007_tram_y_te
bash scripts/00_check.sh                                              # máy đủ điều kiện chưa
uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python -r requirements.lock.txt
bash scripts/01_download.sh                                           # tải khoảng 1 GB dữ liệu nguồn mở
source .venv/bin/activate && bash scripts/run_all.sh                  # phần pandas + CSV thô cho Spark
bash scripts/run_spark.sh                                             # pipeline Spark (bước 11, 16), rồi bước 15 (TYT và CSKCB)
```

Chạy đúng thì lệnh cuối in ra `Doi soat: 27 phep, 0 LECH.`, rồi dòng `Doi chieu NQ 1251: ... KHOP` và `Xong buoc 15.`
Trên máy thử, hai lệnh cuối mất 2–3 phút và 4–9 phút (chưa tính thời gian tải dữ liệu và image Docker ở lần đầu).
Mở `outputs/tables/kpi_tong_hop_spark.csv` để xem KPI chính.

## Tài liệu

| Bạn là | Đọc |
|---|---|
| Người mới, cần cài và chạy trên một máy chưa có gì | [docs/HUONG_DAN_CAI_DAT.md](docs/HUONG_DAN_CAI_DAT.md) |
| Người dùng, cần chạy lại, đọc kết quả, sửa toạ độ trạm hay danh sách CSKCB, đổi tham số, xử lý lỗi | [docs/HUONG_DAN_SU_DUNG.md](docs/HUONG_DAN_SU_DUNG.md) |
| Người cần hiểu thiết kế: thành phần, luồng dữ liệu, pipeline Spark, cụm Docker, các đánh đổi | [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md) |
| Người sửa code hoặc viết báo cáo: quy tắc làm việc và bảng giả định A1–A17 | [CLAUDE.md](CLAUDE.md) |

## Cấu trúc thư mục

```
scripts/      00_check.sh, 01_download.sh, 02–16 *.py, 11a_landing.py, 11b_hdfs_yarn_cli.sh,
              run_all.sh, run_spark.sh, common.py (mọi tham số)
docs/         hướng dẫn cài đặt, hướng dẫn sử dụng, tài liệu kiến trúc
docker/       docker-compose.yml – cụm HDFS + YARN + JupyterLab
notebooks/    buoc11_pyspark_hdfs.ipynb – PySpark nối HDFS trong JupyterLab
data/raw/         dữ liệu nguồn tải về + location-tyt.xlsx (toạ độ trạm) + danh-sach-cskcb.xlsx (30 CSKCB)
data/reference/   danh sách 56 trạm, dân số NQ 1251, 8 đơn vị gộp theo NQ 1251, 15 gói dịch vụ TT 30/2024
data/interim/     file trung gian của pandas
data/landing/     CSV thô cho Spark
data/spark/       curated / quarantine (lỗi thật) / excluded (ngoài phạm vi) / gold khi không dùng HDFS
outputs/tables/   mọi bảng kết quả (CSV)        outputs/figures/   hình h1–h5, h7–h9
outputs/run_manifest.csv   mỗi file kết quả: script tạo ra, số dòng, SHA-256
logs/             nhật ký từng lần chạy
```

`data/interim/`, `data/landing/`, `data/spark/`, `outputs/` và `logs/` đều tạo lại được bằng cách chạy lại pipeline.
`data/raw/location-tyt.xlsx`, `data/raw/danh-sach-cskcb.xlsx` và `data/reference/` thì không; đừng xoá.

## Nguồn dữ liệu và giấy phép

| Nguồn | Dùng cho | Giấy phép / ghi chú |
|---|---|---|
| WorldPop Global2 R2025A | Dân số lưới 100 m, 1 km, nhóm tuổi | CC BY 4.0 – ghi nguồn WorldPop, University of Southampton |
| OpenStreetMap (Overpass API) | Mạng đường, ranh giới đối chiếu, đối chiếu toạ độ trạm | ODbL 1.0 – ghi "© OpenStreetMap contributors"; dữ liệu phái sinh chia sẻ lại phải cùng ODbL |
| GADM 4.1 | Ranh giới 56 xã | Chỉ dùng học thuật/phi thương mại, không phân phối lại |
| AWS Terrain Tiles (Mapzen) | Độ cao, độ dốc | Ghi nguồn |
| Nhóm thực hiện | Toạ độ 56 trạm (nhập tay, kiểm tra theo Cổng dữ liệu Đà Nẵng congdulieu.vn, dữ liệu cập nhật 12/12/2024) | Ghi nguồn cổng dữ liệu; điều kiện dùng lại chưa được xác nhận |
| Danh sách CSKCB của Sở Y tế (người dùng cung cấp) | 30 cơ sở: tuyến, hạng, cấp chuyên môn, toạ độ | Chưa đối chiếu với công bố gốc (A17) |
| Thông tư 30/2024/TT-BYT | 15 nhóm gói dịch vụ của TYT | Văn bản pháp luật; cột phân loại chức năng là đánh giá của nhóm (A16) |
| NQ 1251/NQ-UBTVQH15 và báo chí dẫn lại | 8 đơn vị gộp, số liệu dân số đối chiếu | Xem cột nguồn trong `data/reference/` |

## Giới hạn cần biết trước khi dùng số liệu

- Dân số là ước tính mô hình của WorldPop, không phải số đếm; lệch khoảng 21% so với số liệu NQ 1251 ở các phường trung tâm.
- Ngưỡng 3 km là giả định do nhóm tự nghiên cứu (A1); luôn đọc kèm kết quả 2 km, 5 km, theo đường đi và theo 15 phút.
- Toạ độ trạm do nhóm nhập tay, chưa kiểm chứng thực địa (A13). 8 trạm nằm ngoài ranh giới xã của chính nó, nhưng xa nhất 104 m – mức lệch này nhiều khả năng do ranh giới GADM thô, **không** dùng làm bằng chứng đặt sai chỗ.
- Kết luận "đặt sai chỗ" ở Hòa Ninh dựa trên khoảng cách trạm – tâm dân cư; tác động nếu dời trạm **chưa được tính** (chưa có số).
- Danh sách 30 CSKCB không có phòng khám tư nhỏ nên mọi số đếm CSKCB là sàn dưới; địa chỉ trong danh sách dùng tên đơn vị hành chính sau 2025, tức là danh sách hiện hành chứ không phải ảnh chụp năm 2024 (A17).
- Ngưỡng ứng viên rà soát gộp trạm (≥ 6 CSKCB trong 2 km) được chọn **sau khi** đã biết NQ 1251 gộp xã nào, nên mức khớp 14/17 và p-value trong `doi_soat_nq1251_quy_tac.csv` là lạc quan (A15). Danh sách ứng viên là danh sách rà soát, không phải đề nghị đóng trạm.
- Cột "bệnh viện tuyến trên thay thế được" trong `tt30_2024_goi_dich_vu.csv` là đánh giá của nhóm; bản PDF trong `data/reference/` là bản thảo, báo cáo phải dẫn bản chính thức (A16).
- Số liệu sau năm 2024 (`forecast_*.csv`, `access_2030_*.csv`, hình h5) là **dự báo** từ chuỗi 2015–2024, không phải số đo; đọc kèm khoảng 80%.

Chi tiết: [docs/KIEN_TRUC.md](docs/KIEN_TRUC.md#10-hạn-chế-đã-biết) và bảng giả định trong [CLAUDE.md](CLAUDE.md).
