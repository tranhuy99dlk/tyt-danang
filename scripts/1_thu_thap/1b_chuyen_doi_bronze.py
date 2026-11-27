# -*- coding: utf-8 -*-
"""1b – THU THẬP: CHUYỂN ĐỔI DỮ LIỆU NHIỀU NGUỒN VÀO TẦNG BRONZE (giai đoạn 2 của vòng đời).

Mỗi loại nguồn có một bộ chuyển đổi: raster GeoTIFF -> bảng pixel CSV, Excel -> CSV nguyên văn (giữ số dòng Excel),
bảng tham chiếu -> CSV. Tất cả đổ về cùng một "hợp đồng Bronze": source_system=<nguồn>/ingestion_date=<ngày>, SHA-256,
không sửa giá trị (xem docs/BAO_CAO_7_GIAI_DOAN.md, mục 2.3).

Đầu vào : các file gốc trong data/raw/ (do 1a_tai_du_lieu.sh tải về hoặc nhóm cung cấp) và data/reference/.
Đầu ra  : data/lake/bronze/source_system=<nguồn>/ingestion_date=<ngày nạp>/<bảng>.csv  (+ 1 file GeoJSON)
          outputs/tables/1_du_lieu_nguon/bronze_danh_muc.csv  (danh mục bảng Bronze: số dòng, SHA-256, file gốc)

Nguyên tắc của Bronze: KHÔNG làm sạch, KHÔNG sửa giá trị.
  - Giữ nguyên mã NoData (-99999) của WorldPop và mọi pixel trong hộp bao (kể cả pixel ngoài 56 xã).
  - Giữ mọi dòng của file Excel (kể cả dòng tiêu đề, dòng tên quận).
  - Chỉ đổi ĐỊNH DẠNG sang CSV vì Spark không đọc được GeoTIFF và Excel.
Việc đọc theo schema và kiểm tra, tách lỗi (quarantine) là của Spark Bước 1 · Đọc (b1_doc.py) và Bước 2 · Kiểm tra & làm sạch (b2_kiem_tra_lam_sach.py).

Chạy: python scripts/1_thu_thap/1b_chuyen_doi_bronze.py   (hoặc F5 trong VS Code khi đang mở file này)
"""
import json  # đọc/ghi GeoJSON
import shutil  # xoá thư mục cũ khi nạp lại trong cùng ngày
import zipfile  # đọc file GADM nén .zip

import numpy as np  # tính toán mảng
import pandas as pd  # bảng dữ liệu
import rasterio  # đọc ảnh raster GeoTIFF của WorldPop
from rasterio.windows import from_bounds  # cắt một cửa sổ raster theo hộp bao

import sys  # danh sách thư mục Python tìm module
from pathlib import Path  # làm việc với đường dẫn
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "chung"))  # thêm scripts/chung/ để import được common.py, hinh_hoc.py
from common import (bang, RAW, REF, BRONZE, BBOX, YEARS, PROJ_YEARS, AGE60_BANDS,  # đường dẫn và tham số dùng chung
                    get_logger, register, save_csv, sha256, today_utc, norm_name)  # (tiếp) hàm tiện ích

S = "1b_chuyen_doi_bronze"  # tên bước, dùng cho log và sổ run_manifest
log = get_logger(S)  # bộ ghi log của bước này
NGAY = today_utc()  # ngày nạp (ingestion_date) = hôm nay theo giờ UTC
BBOX1K = (BBOX[0] - .02, BBOX[1] - .02, BBOX[2] + .02, BBOX[3] + .02)  # hộp bao nới rộng 0,02° cho lưới 1 km (ô 1 km ở rìa không bị cắt)
danh_muc = []  # danh mục các bảng Bronze đã ghi trong lần chạy này


def thu_muc(nguon: str):  # trả về thư mục Bronze của một nguồn trong ngày nạp hôm nay
    return BRONZE / f"source_system={nguon}" / f"ingestion_date={NGAY}"  # ví dụ bronze/source_system=worldpop/ingestion_date=2026-11-26


def ghi(df: pd.DataFrame, nguon: str, ten: str, goc: list, ghi_chu: str):  # ghi một bảng CSV vào Bronze và ghi danh mục
    p = thu_muc(nguon) / f"{ten}.csv"  # đường dẫn file đích
    p.parent.mkdir(parents=True, exist_ok=True)  # tạo thư mục nếu chưa có
    df.to_csv(p, index=False, encoding="utf-8")  # ghi CSV UTF-8 không BOM (Spark đọc tiêu đề đúng)
    register(p, S, rows=len(df), note=ghi_chu)  # đăng ký vào run_manifest.csv
    danh_muc.append(dict(source_system=nguon, ingestion_date=NGAY, bang=ten, dinh_dang="csv", so_dong=len(df),  # thông tin bảng
                         so_cot=df.shape[1], sha256=sha256(p), file_goc="; ".join(goc), ghi_chu=ghi_chu))  # (tiếp) băm, nguồn gốc
    log.info("Bronze %-22s %-24s %9d dong, %2d cot", nguon, ten, len(df), df.shape[1])  # in tiến độ


def doc_cua_so(path, bbox):  # đọc cửa sổ raster trong hộp bao, GIỮ NGUYÊN giá trị thô (kể cả NoData)
    with rasterio.open(path) as ds:  # mở file GeoTIFF
        win = from_bounds(*bbox, transform=ds.transform).round_offsets().round_lengths()  # cửa sổ pixel ứng với hộp bao, làm tròn về số nguyên
        return ds.read(1, window=win).astype("float64"), ds.window_transform(win), ds.nodata  # mảng giá trị, phép biến đổi toạ độ, mã NoData


def main():  # hàm chính của bước
    for nguon in ("worldpop", "co_so_y_te", "danh_muc", "ranh_gioi"):  # các nguồn sẽ nạp
        if thu_muc(nguon).exists():  # nếu hôm nay đã nạp một lần
            shutil.rmtree(thu_muc(nguon))  # xoá để nạp lại sạch (chỉ xoá trong data/lake, không đụng data/raw)

    # ---------- 1. WorldPop 100 m: mỗi pixel x mỗi năm, giá trị thô ----------
    parts, tr0, shape0 = [], None, None  # danh sách bảng con từng năm; phép biến đổi và kích thước của năm đầu
    for y in YEARS:  # duyệt 10 năm 2015–2024
        p = RAW / "worldpop" / "pop100m" / f"vnm_pop_{y}_CN_100m_R2025A_v1.tif"  # file raster năm y
        a, tr, nd = doc_cua_so(p, BBOX)  # đọc cửa sổ hộp bao
        if tr0 is None:  # năm đầu tiên
            tr0, shape0 = tr, a.shape  # ghi nhớ lưới chuẩn
        assert a.shape == shape0 and tr == tr0, f"Luoi {y} khong trung luoi {YEARS[0]}"  # mọi năm phải cùng lưới pixel
        rows, cols = np.indices(a.shape)  # chỉ số hàng/cột của từng pixel
        parts.append(pd.DataFrame({"year": y, "row": rows.ravel(), "col": cols.ravel(), "pop_raw": a.ravel()}))  # bảng con năm y
        log.info("WorldPop 100 m %d: %d pixel, NoData=%s (%d pixel), pixel > 0: %d", y, a.size, nd, int((a == nd).sum()), int((a > 0).sum()))  # thống kê
    goc = [f"data/raw/worldpop/pop100m/vnm_pop_{y}_CN_100m_R2025A_v1.tif" for y in YEARS]  # danh sách file gốc
    ghi(pd.concat(parts, ignore_index=True), "worldpop", "pop_100m_raw", goc, "pixel 100 m x nam, gia tri tho (NoData -99999 giu nguyen)")  # ghi bảng

    # ---------- 2. WorldPop 1 km: tổng dân + 7 nhóm tuổi 60+, các năm phân tích VÀ các năm chiếu ----------
    parts, t1 = [], None  # bảng con; phép biến đổi lưới 1 km
    for y in YEARS + PROJ_YEARS:  # 2015–2024 (phân tích) và 2025–2030 (số chiếu, chỉ cho dự báo)
        bien_file = [("pop", RAW / "worldpop" / "pop1km" / f"vnm_pop_{y}_CN_1km_R2025A_UA_v1.tif")]  # biến tổng dân
        bien_file += [(f"t_{b}", RAW / "worldpop" / "age1km" / f"vnm_t_{b}_{y}_CN_1km_R2025A_UA_v1.tif") for b in AGE60_BANDS]  # 7 biến nhóm tuổi
        for bien, p in bien_file:  # duyệt từng biến
            a, t, _ = doc_cua_so(p, BBOX1K)  # đọc cửa sổ hộp bao nới rộng
            t1 = t1 or t  # ghi nhớ lưới 1 km của file đầu tiên
            assert t == t1, f"Luoi 1 km {p.name} khong trung"  # mọi file 1 km phải cùng lưới
            r1, c1 = np.indices(a.shape)  # chỉ số hàng/cột ô 1 km
            parts.append(pd.DataFrame({"year": y, "k1_row": r1.ravel(), "k1_col": c1.ravel(), "bien": bien, "gia_tri": a.ravel()}))  # bảng con
    ghi(pd.concat(parts, ignore_index=True), "worldpop", "pop_1km_raw", ["data/raw/worldpop/pop1km/*.tif", "data/raw/worldpop/age1km/*.tif"],  # ghi bảng
        "luoi 1 km: pop + 7 nhom tuoi 60-90+, 2015-2030 (2025-2030 la so CHIEU), gia tri tho")  # (tiếp) ghi chú

    # ---------- 3. Toạ độ 56 trạm y tế do nhóm nhập tay: nguyên văn file Excel ----------
    x = pd.read_excel(RAW / "location-tyt.xlsx", header=None, dtype=str).iloc[:, :3]  # đọc 3 cột đầu, mọi ô là chuỗi, không coi dòng nào là tiêu đề
    x.columns = ["cot1", "cot2", "cot3"]  # đặt tên cột: quận/huyện, tên xã/phường, "vĩ độ, kinh độ"
    x.insert(0, "dong_excel", range(1, len(x) + 1))  # số thứ tự dòng trong Excel để truy vết
    ghi(x, "co_so_y_te", "tyt_raw", ["data/raw/location-tyt.xlsx"], "nguyen van location-tyt.xlsx (ca dong tieu de, dong ten quan)")  # ghi bảng

    # ---------- 4. Danh sách 30 cơ sở khám chữa bệnh (CSKCB) của Sở Y tế: nguyên văn ----------
    c = pd.read_excel(RAW / "danh-sach-cskcb.xlsx", sheet_name=0, dtype=str)  # đọc sheet đầu, mọi ô là chuỗi
    c.columns = [str(k).strip() for k in c.columns]  # bỏ khoảng trắng thừa trong tên cột
    an = [k for k in c.columns if k.startswith("Unnamed")]  # các cột không tên trong file gốc
    if c[an].notna().any().any():  # nếu cột không tên lại có dữ liệu
        log.warning("CAN XEM: cot khong ten trong danh-sach-cskcb.xlsx co du lieu -> van bo qua: %s", an)  # báo người dùng
    c = c.drop(columns=an).rename(columns={"STT": "stt", "Mã": "ma_cs", "Tên": "ten_cs", "Tuyến CMKT": "tuyen",  # bỏ cột trống, đổi tên cột sang không dấu
                                           "Hạng bệnh viện": "hang", "Cấp CMKT": "cap", "Điểm": "diem",  # (tiếp)
                                           "Địa chỉ": "dia_chi", "tọa độ": "toa_do"})  # (tiếp)
    c.insert(0, "dong_excel", range(2, len(c) + 2))  # số dòng Excel (dòng 1 là tiêu đề)
    ghi(c, "co_so_y_te", "cskcb_raw", ["data/raw/danh-sach-cskcb.xlsx"], "nguyen van danh sach CSKCB (cac o la chuoi)")  # ghi bảng

    # ---------- 5. Danh mục tham chiếu do nhóm chuẩn bị ----------
    for ten, f in (("dm_56_tram", "danh_sach_56_tram.csv"), ("dan_so_nq1251", "dan_so_chinh_thuc_nq1251.csv"),  # cặp (tên bảng Bronze, file gốc)
                   ("nq1251_sap_xep", "nq1251_sap_xep.csv"), ("tt30_goi_dich_vu", "tt30_2024_goi_dich_vu.csv")):  # (tiếp)
        d = pd.read_csv(REF / f, encoding="utf-8-sig", dtype=str)  # đọc nguyên văn, mọi ô là chuỗi
        ghi(d, "danh_muc", ten, [f"data/reference/{f}"], "ban sao nguyen van danh muc tham chieu")  # ghi bảng

    # ---------- 6. Ranh giới GADM 4.1 cấp xã của Đà Nẵng: hình học nguyên bản (GeoJSON) ----------
    zp = RAW / "gadm" / "gadm41_VNM_3.json.zip"  # file GADM nén
    with zipfile.ZipFile(zp) as z:  # mở file zip
        ten_json = [n for n in z.namelist() if n.endswith(".json")][0]  # tên file JSON bên trong
        data = json.load(z.open(ten_json))  # đọc toàn bộ GeoJSON Việt Nam cấp xã
    dn = [f for f in data["features"] if norm_name(f["properties"].get("NAME_1", "")).replace(" ", "") == "đànẵng"]  # chỉ giữ các đơn vị thuộc Đà Nẵng
    p = thu_muc("ranh_gioi") / "gadm41_da_nang_cap3.geojson"  # file đích
    p.parent.mkdir(parents=True, exist_ok=True)  # tạo thư mục
    json.dump({"type": "FeatureCollection", "features": dn}, open(p, "w", encoding="utf-8"), ensure_ascii=False)  # ghi GeoJSON, không sửa hình học
    register(p, S, rows=len(dn), note="GADM 4.1 cap xa, chi Da Nang, hinh hoc nguyen ban (hoc thuat, khong phan phoi lai)")  # đăng ký
    danh_muc.append(dict(source_system="ranh_gioi", ingestion_date=NGAY, bang="gadm41_da_nang_cap3", dinh_dang="geojson",  # ghi danh mục
                         so_dong=len(dn), so_cot=None, sha256=sha256(p), file_goc="data/raw/gadm/gadm41_VNM_3.json.zip",  # (tiếp)
                         ghi_chu="GADM 4.1 cap xa cua Da Nang, hinh hoc nguyen ban"))  # (tiếp)
    log.info("Bronze ranh_gioi gadm41_da_nang_cap3: %d don vi", len(dn))  # in tiến độ

    save_csv(pd.DataFrame(danh_muc), bang("bronze_danh_muc.csv"), S, note=f"danh muc bang Bronze nap ngay {NGAY}")  # ghi danh mục Bronze
    log.info("Xong. Bronze: %s (ngay nap %s, %d bang)", BRONZE, NGAY, len(danh_muc))  # thông báo hoàn tất


if __name__ == "__main__":  # chỉ chạy khi gọi trực tiếp file này (không chạy khi bị import)
    main()  # gọi hàm chính
