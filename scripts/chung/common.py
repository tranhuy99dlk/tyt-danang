# -*- coding: utf-8 -*-
"""Tham số và hàm dùng chung cho toàn bộ pipeline (MIS5007 – trạm y tế Đà Nẵng cũ).

Mọi script đều `from common import ...` để dùng chung một bộ tham số: đổi một con số ở đây
(ví dụ ngưỡng 3 km) thì mọi bước đều đổi theo. Mỗi tham số phân tích là một GIẢ ĐỊNH, có mã A1–A18
ghi trong CLAUDE.md.

Mỗi file kết quả được ghi kèm một dòng vào outputs/run_manifest.csv (tên file, số dòng, SHA-256,
script tạo ra, giờ ghi) để mọi con số trong báo cáo truy ngược được về một file cụ thể.
"""
from __future__ import annotations  # cho phép viết kiểu dữ liệu như "int | None" trên Python cũ hơn

import csv  # đọc/ghi file CSV dạng từng dòng (dùng cho sổ run_manifest.csv)
import datetime as dt  # lấy ngày giờ hiện tại
import hashlib  # tính mã băm SHA-256 để biết file có bị đổi hay không
import logging  # ghi nhật ký (log) ra màn hình và ra file
import os  # đọc biến môi trường, ví dụ JAVA_HOME
import re  # biểu thức chính quy (regex) để chuẩn hoá tên địa danh
import sys  # dùng sys.stdout để in log ra màn hình
import unicodedata  # chuẩn hoá chữ tiếng Việt có dấu về cùng một dạng mã
from pathlib import Path  # làm việc với đường dẫn file theo kiểu hướng đối tượng

# ============================== 1. ĐƯỜNG DẪN ==============================
ROOT = Path(__file__).resolve().parents[2]  # thư mục gốc dự án (file này nằm ở scripts/chung/ -> lùi 2 cấp)
RAW = ROOT / "data" / "raw"  # vùng nguồn: file tải về / người dùng cung cấp, KHÔNG sửa, KHÔNG xoá
REF = ROOT / "data" / "reference"  # danh mục tham chiếu do nhóm chuẩn bị (56 trạm, NQ 1251...), KHÔNG sửa
LAKE = ROOT / "data" / "lake"  # "hồ dữ liệu" kiến trúc Medallion trên máy (khi không dùng HDFS)
BRONZE = LAKE / "bronze"  # tầng Bronze: bản ghi thô, giữ nguyên giá trị, chia thư mục theo nguồn và ngày nạp
SILVER = LAKE / "silver"  # tầng Silver: dữ liệu đã kiểm tra, làm sạch, chuẩn hoá (Parquet)
GOLD = LAKE / "gold"  # tầng Gold: bảng chỉ số phục vụ ra quyết định (Parquet)
OUT = ROOT / "outputs"  # thư mục kết quả cho báo cáo
TAB = OUT / "tables"  # bảng kết quả dạng CSV, chia thư mục con theo bước (xem THU_MUC_BANG)
FIG = OUT / "figures"  # hình PNG
LOGS = ROOT / "logs"  # nhật ký từng lần chạy
RUN_MANIFEST = OUT / "run_manifest.csv"  # sổ ghi mọi file kết quả: số dòng, SHA-256, script tạo ra
for _p in (TAB, FIG, LOGS, BRONZE, SILVER, GOLD):  # duyệt từng thư mục cần có
    _p.mkdir(parents=True, exist_ok=True)  # tạo nếu chưa có (không báo lỗi nếu đã có)

# Mỗi bảng kết quả CSV nằm trong thư mục của bước tạo ra nó: outputs/tables/<thư mục bước>/<tên file>
THU_MUC_BANG = {  # thư mục kết quả (theo nhóm nội dung của báo cáo) -> danh sách bảng
    "1_du_lieu_nguon": ["bronze_danh_muc.csv", "qa_boundaries.csv", "qa_network.csv", "spark_bang_da_doc.csv", "xa_phuong_56_toa_do.csv"],  # dữ liệu nguồn đã thu thập, bảng Bronze đã đọc, ranh giới, mạng đường, toạ độ 56 xã
    "2_chat_luong": ["spark_so_dong_theo_nguon.csv", "spark_quarantine_tong_hop.csv", "spark_quarantine_mau.csv", "qa_official_population.csv", "qa_grid.csv", "qa_stations.csv", "tram_y_te_56.csv", "cskcb_danang.csv"],  # kiểm tra & làm sạch: số dòng, quarantine / excluded, lệch tổng dân, kiểm tra trạm và lưới, danh sách đã làm sạch
    "3_mo_ta_kpi": ["kpi_tong_hop_spark.csv", "kpi_theo_nam_spark.csv", "kpi_theo_xa_nam_spark.csv", "kpi_theo_xa_spark.csv", "kpi_theo_nhom_spark.csv", "kpi_theo_nhom_spark_sql.csv", "kpi_theo_quan_spark.csv", "tram_vung_phuc_vu_spark.csv"],  # phân tích mô tả: KPI tiếp cận theo năm, xã, nhóm, quận
    "4_chan_doan": ["white_zones.csv"],  # phân tích chẩn đoán: vùng lõm và nguyên nhân
    "5_khuyen_nghi": ["scenario_nq1251.csv", "greedy_curve.csv", "greedy_sites.csv", "greedy_before_after.csv", "tyt_cskcb.csv", "tyt_cskcb_theo_nhom.csv", "ung_vien_gop_tram.csv", "doi_soat_nq1251_quy_tac.csv", "doi_soat_nq1251_theo_xa.csv", "ban_do_truoc_sau_de_xuat.csv"],  # phân tích khuyến nghị: điểm đặt mới, kịch bản NQ 1251, TYT – CSKCB
    "6_du_bao": ["commune_series.csv", "validation_downscale_A4.csv", "forecast_worldpop_chieu.csv", "forecast_backtest.csv", "forecast_model_choice.csv", "forecast_commune_2030.csv", "forecast_total_2030.csv", "access_2030_change.csv", "access_2030_total.csv"],  # phân tích dự báo 2030 (file riêng)
    "7_kiem_chung": ["qa_summary.csv", "doi_soat_spark.csv", "so_sanh_parquet_csv.csv", "thoi_gian_5_buoc.csv", "pham_vi_da_nang_cu.csv", "tu_dien_du_lieu.csv", "kiem_tra_run_manifest.csv", "so_lieu_chot.csv"],  # kiểm chứng và truy vết: đối soát, QA, đo Parquet/CSV, thời gian 5 bước, từ điển, sổ, số liệu chốt
    "luoi_100m": ["dan_cu_luoi_100m.csv", "tiep_can_luoi_100m.csv"],  # bảng lưới 100 m phục vụ Excel / GIS
}  # hết bảng
_THU_MUC_CUA = {ten: tm for tm, ds in THU_MUC_BANG.items() for ten in ds}  # tra ngược: tên file -> thư mục bước


def bang(ten: str) -> Path:  # đường dẫn đầy đủ của một bảng kết quả, ví dụ bang("kpi_tong_hop_spark.csv")
    if ten not in _THU_MUC_CUA:  # bảng chưa được xếp vào bước nào
        raise KeyError(f"Bang {ten} chua co trong THU_MUC_BANG (common.py) - them vao thu muc cua buoc tao ra no")  # báo lỗi rõ ràng
    return TAB / _THU_MUC_CUA[ten] / ten  # outputs/tables/<thư mục bước>/<tên file>


# ============================== 2. THAM SỐ PHÂN TÍCH (GIẢ ĐỊNH) ==============================
BBOX = (107.78, 15.88, 108.36, 16.24)  # hộp bao Đà Nẵng cũ (Tây, Nam, Đông, Bắc), có vùng đệm; ranh giới thật 107,82–108,34°Đ, 15,92–16,23°B
CRS_M = "EPSG:32649"  # hệ toạ độ phẳng UTM múi 49 Bắc (đơn vị mét) để đo khoảng cách; Đà Nẵng nằm chủ yếu ở múi 49 (từ 108°Đ)
BASE_YEAR = 2024  # năm hiện trạng = năm cuối trước khi sắp xếp đơn vị hành chính 01/01/2025
YEARS = list(range(2015, 2025))  # 10 mốc năm phân tích 2015, 2016, ..., 2024
PROJ_YEARS = list(range(2025, 2031))  # các năm SỐ CHIẾU của WorldPop (2025–2030), chỉ dùng cho bước dự báo
FORECAST_YEAR = 2030  # năm đích của dự báo (bước 08); KHÔNG thuộc các năm phân tích
THRESH_KM = [2.0, 3.0, 5.0]  # các ngưỡng khoảng cách để xem độ nhạy (A1)
MAIN_KM = 3.0  # ngưỡng KPI chính 3 km (giả định A1 do nhóm chốt)
WHITE_KM = 5.0  # "vùng trắng": cách trạm gần nhất trên 5 km
TIME_MIN = 15.0  # ngưỡng thời gian 15 phút xe máy (A6)
TIME_LOM_MIN = 20.0  # ngưỡng "vùng lõm" 20 phút xe máy theo thiết kế (giả định mới A18)
AGE60_BANDS = [60, 65, 70, 75, 80, 85, 90]  # 7 nhóm tuổi của WorldPop cộng lại thành dân 60+ (A3)
POP_TOL_CANH_BAO_PCT = 3.0  # quy tắc thiết kế: tổng dân lệch số liệu chính thức quá ±3% thì CẢNH BÁO (không cách ly)
POP_TOL_A11_PCT = 20.0  # ngưỡng chấp nhận cũ ±20% (A11), vẫn báo cáo song song
SPEED_KMH = {  # tốc độ xe máy giả định (km/h) theo loại đường OSM (A6)
    "motorway": 50, "trunk": 40, "primary": 35, "secondary": 30, "tertiary": 30,  # đường lớn
    "unclassified": 25, "residential": 20, "living_street": 15, "service": 15,  # đường nhỏ, khu dân cư
    "track": 12, "road": 20,  # đường mòn và đường chưa phân loại
}  # hết bảng tốc độ
WALK_KMH = 5.0  # đoạn đi bộ từ tâm ô lưới (hoặc từ trạm) ra nút đường gần nhất: 5 km/h (A6)
DISTRICTS = ["Quận Hải Châu", "Quận Thanh Khê", "Quận Sơn Trà", "Quận Ngũ Hành Sơn",  # 7 quận/huyện trước 2025
             "Quận Liên Chiểu", "Quận Cẩm Lệ", "Huyện Hòa Vang"]  # (tiếp danh sách)
MA_QUAN = {  # mã quận không dấu, dùng làm tên thư mục phân vùng (district_id) trong Parquet
    "Quận Hải Châu": "hai_chau", "Quận Thanh Khê": "thanh_khe", "Quận Sơn Trà": "son_tra",  # 3 quận lõi và ven biển
    "Quận Ngũ Hành Sơn": "ngu_hanh_son", "Quận Liên Chiểu": "lien_chieu",  # 2 quận
    "Quận Cẩm Lệ": "cam_le", "Huyện Hòa Vang": "hoa_vang",  # quận Cẩm Lệ và huyện Hòa Vang
}  # hết bảng mã quận
NQ1251_GROUPS = [  # 8 nhóm phường gộp thành 1 đơn vị theo NQ 1251/NQ-UBTVQH15 (hiệu lực 01/01/2025)
    ["Thanh Khê Đông", "Hòa Khê"], ["Xuân Hà", "Tam Thuận"], ["Thạc Gián", "Vĩnh Trung"],  # nhóm 1–3
    ["Chính Gián", "Tân Chính"], ["An Hải Đông", "An Hải Tây"],  # nhóm 4–5
    ["Phước Ninh", "Nam Dương", "Bình Hiên"], ["Bình Thuận", "Hòa Thuận Đông"],  # nhóm 6–7
    ["Hải Châu I", "Hải Châu II"],  # nhóm 8
]  # hết danh sách; được đối chiếu với data/reference/nq1251_sap_xep.csv ở Bước 4 (b4_khuyen_nghi.py)


# ============================== 3. HÀM TIỆN ÍCH ==============================
def now_utc() -> str:  # trả về giờ hiện tại theo giờ quốc tế (UTC) dạng chuỗi
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # ví dụ "2026-11-26T14:03:28Z"


def today_utc() -> str:  # trả về ngày hôm nay (UTC) dạng YYYY-MM-DD, dùng làm "ingestion_date" của Bronze
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")  # ví dụ "2026-11-26"


def get_logger(name: str) -> logging.Logger:  # tạo bộ ghi log: in ra màn hình VÀ ghi vào logs/<tên>_<giờ>.log
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")  # dấu thời gian để mỗi lần chạy có file log riêng
    lg = logging.getLogger(name)  # lấy (hoặc tạo) logger theo tên
    lg.setLevel(logging.INFO)  # ghi từ mức INFO trở lên (INFO, WARNING, ERROR)
    lg.handlers.clear()  # xoá handler cũ để không in trùng khi gọi lại
    lg.propagate = False  # không chuyển log lên logger gốc (tránh in hai lần)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")  # mẫu mỗi dòng log
    fh = logging.FileHandler(LOGS / f"{name}_{ts}.log", encoding="utf-8")  # handler ghi ra file
    fh.setFormatter(fmt)  # áp mẫu cho handler file
    sh = logging.StreamHandler(sys.stdout)  # handler in ra màn hình
    sh.setFormatter(fmt)  # áp mẫu cho handler màn hình
    lg.addHandler(fh)  # gắn handler file
    lg.addHandler(sh)  # gắn handler màn hình
    return lg  # trả logger cho script dùng


def sha256(path: Path) -> str:  # tính mã băm SHA-256 của một file (đổi 1 byte thì mã đổi hoàn toàn)
    h = hashlib.sha256()  # tạo bộ băm rỗng
    with open(path, "rb") as f:  # mở file ở chế độ nhị phân
        for chunk in iter(lambda: f.read(1 << 20), b""):  # đọc từng khúc 1 MB cho tới hết file
            h.update(chunk)  # đưa khúc vào bộ băm
    return h.hexdigest()  # trả mã băm dạng chuỗi hex 64 ký tự


def register(path: Path, script: str, rows: int | None = None, note: str = "") -> None:  # ghi/cập nhật một dòng trong run_manifest.csv
    path = Path(path)  # bảo đảm là đối tượng Path
    rel = str(path.relative_to(ROOT))  # đường dẫn tương đối so với gốc dự án (dễ đọc, không lộ tên máy)
    rec = {"file": rel, "rows": "" if rows is None else rows, "bytes": path.stat().st_size,  # thông tin file
           "sha256": sha256(path), "script": script, "created_utc": now_utc(), "note": note}  # (tiếp) băm, nguồn, giờ, ghi chú
    recs = []  # danh sách các dòng cũ của sổ
    if RUN_MANIFEST.exists():  # nếu sổ đã có
        with open(RUN_MANIFEST, newline="", encoding="utf-8") as f:  # mở sổ để đọc
            recs = [r for r in csv.DictReader(f) if r["file"] != rel]  # giữ mọi dòng trừ dòng của chính file này (sẽ thay)
    recs.append(rec)  # thêm dòng mới
    with open(RUN_MANIFEST, "w", newline="", encoding="utf-8") as f:  # mở sổ để ghi đè
        w = csv.DictWriter(f, fieldnames=list(rec.keys()))  # bộ ghi CSV theo tên cột
        w.writeheader()  # ghi dòng tiêu đề
        w.writerows(recs)  # ghi mọi dòng


def save_csv(df, path: Path, script: str, note: str = "") -> None:  # ghi một bảng pandas ra CSV và đăng ký vào sổ
    path = Path(path)  # bảo đảm là Path
    path.parent.mkdir(parents=True, exist_ok=True)  # tạo thư mục cha nếu chưa có
    df.to_csv(path, index=False, encoding="utf-8-sig")  # ghi CSV; "utf-8-sig" để Excel đọc đúng tiếng Việt
    register(path, script, rows=len(df), note=note)  # đăng ký vào run_manifest.csv


# ============================== 4. CHUẨN HOÁ TÊN ĐỊA DANH ==============================
_TONE_FIX = {  # đưa dấu thanh về cùng một kiểu đặt dấu (ví dụ "hoà" và "hòa" là cùng một chữ)
    "oà": "òa", "oá": "óa", "oả": "ỏa", "oã": "õa", "oạ": "ọa",  # nhóm "oa"
    "oè": "òe", "oé": "óe", "oẻ": "ỏe", "oẽ": "õe", "oẹ": "ọe",  # nhóm "oe"
    "uỳ": "ùy", "uý": "úy", "uỷ": "ủy", "uỹ": "ũy", "uỵ": "ụy",  # nhóm "uy"
}  # hết bảng
_PREFIX = re.compile(r"^(phường|phuong|xã|xa|thị trấn|thi tran|p\.|x\.)\s+")  # tiền tố cần bỏ ở đầu tên
_ROMAN = {"1": "i", "2": "ii", "3": "iii"}  # số Ả Rập cuối tên đổi sang số La Mã ("Hải Châu 1" = "Hải Châu I")


def norm_name(s) -> str:  # chuẩn hoá tên xã/phường để so khớp: chữ thường, bỏ tiền tố, thống nhất dấu
    if s is None:  # giá trị rỗng
        return ""  # trả chuỗi rỗng
    s = unicodedata.normalize("NFC", str(s)).lower().strip()  # chuẩn mã Unicode dạng dựng sẵn, chữ thường, bỏ khoảng trắng đầu cuối
    for a, b in _TONE_FIX.items():  # duyệt từng cặp kiểu đặt dấu
        s = s.replace(a, b)  # thay về kiểu thống nhất
    s = re.sub(r"\s+", " ", s)  # gộp nhiều khoảng trắng liên tiếp thành một
    s = _PREFIX.sub("", s)  # bỏ tiền tố "phường", "xã"...
    s = re.sub(r"\b([123])$", lambda m: _ROMAN[m.group(1)], s)  # "hải châu 1" -> "hải châu i"
    return s.strip()  # bỏ khoảng trắng thừa và trả kết quả


def wquantile(values, weights, q: float) -> float:  # phân vị có trọng số: giá trị nhỏ nhất mà luỹ kế trọng số >= q
    import numpy as np  # nhập numpy tại chỗ (để file này nhẹ khi chỉ cần đường dẫn)
    v = np.asarray(values, float)  # mảng giá trị (ví dụ khoảng cách)
    w = np.asarray(weights, float)  # mảng trọng số (ví dụ số dân của ô)
    m = np.isfinite(v) & (w > 0)  # chỉ giữ giá trị hữu hạn có trọng số dương
    v, w = v[m], w[m]  # lọc theo mặt nạ
    if len(v) == 0:  # không còn gì để tính
        return float("nan")  # trả "không phải số"
    o = np.argsort(v, kind="stable")  # thứ tự sắp xếp giá trị tăng dần
    v, w = v[o], w[o]  # sắp xếp cả giá trị và trọng số
    cw = np.cumsum(w)  # trọng số cộng dồn
    return float(v[np.searchsorted(cw, q * cw[-1])])  # vị trí đầu tiên mà cộng dồn >= q * tổng


def bronze_moi_nhat(nguon: str) -> Path:  # thư mục Bronze của lần nạp MỚI NHẤT của một nguồn (Bronze chỉ thêm, không sửa)
    goc = BRONZE / f"source_system={nguon}"  # thư mục của nguồn
    ngay = sorted(p for p in goc.glob("ingestion_date=*") if p.is_dir())  # các lần nạp, sắp theo ngày tăng dần
    if not ngay:  # chưa nạp lần nào
        raise FileNotFoundError(f"Chua co Bronze cho nguon '{nguon}' - chay 1b_chuyen_doi_bronze.py / 1c_tra_cuu_khong_gian.py truoc")  # báo lỗi rõ ràng
    return ngay[-1]  # lần nạp mới nhất


def set_java_home() -> None:  # Spark cần Java; nếu chưa đặt JAVA_HOME thì tìm Java 17 do Homebrew cài
    if os.environ.get("JAVA_HOME"):  # đã có JAVA_HOME (ví dụ trong container Docker)
        return  # không làm gì
    for cand in ("/opt/homebrew/opt/openjdk@17", "/usr/local/opt/openjdk@17"):  # đường dẫn Homebrew trên máy Apple Silicon và Intel
        if Path(cand, "bin", "java").exists():  # nếu có file java ở đó
            os.environ["JAVA_HOME"] = cand  # đặt JAVA_HOME cho tiến trình này
            os.environ["PATH"] = f"{cand}/bin" + os.pathsep + os.environ.get("PATH", "")  # đưa java lên đầu PATH
            return  # xong
